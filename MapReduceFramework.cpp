#include "MapReduceFramework.h"
#include <pthread.h>
#include <atomic>
#include <iostream>
#include <algorithm>
#include <semaphore.h>
#include "Barrier/Barrier.h"

#define THREAD_NOT_FOUND "Thread not found in the system"
#define BITS_0_31 0x7FFFFFFF
#define BITS_32_61 0x3FFFFFFF
#define BIT_62 62
#define BIT_31 31

typedef std::vector<IntermediateVec *> IntermediateMap;

/** Function declarations */
struct JobContext;
void sortIntermediateVec (JobContext *context);
int getThreadIndex (pthread_t thread, JobContext *context);
void shuffleIntermediateVec(JobContext *context);

struct JobContext
{
    const MapReduceClient &client;
    const InputVec &inputVec;
    OutputVec &outputVec;
    int multiThreadLevel;
    pthread_t *threads;
    bool isJoined = false;
    std::atomic<uint64_t> *counter; // [62,
    // 63] - stage, [31, 61] - total tasks, [0, 30] - completed tasks
    std::atomic<int> *inputIndex = new std::atomic<int>(0);
    std::atomic<int> *reduceIndex = new std::atomic<int>(0);
    std::atomic<int> *shuffledCount = new std::atomic<int>(0);
    IntermediateMap intermediateMap;
    IntermediateMap shuffledVec;
    pthread_mutex_t mutex_3;
    pthread_mutex_t stateMutex;
    pthread_mutex_t waitMutex;
    Barrier sortBarrier;
    sem_t shuffleSemaphore;

    JobContext (const MapReduceClient &client, const InputVec &inputVec, OutputVec &outputVec, int multiThreadLevel)
        : client(client), inputVec(inputVec), outputVec(outputVec),
        multiThreadLevel(multiThreadLevel), intermediateMap(), sortBarrier
          (multiThreadLevel)
    {
      threads = new pthread_t[multiThreadLevel];
      threads = new pthread_t[multiThreadLevel];
      pthread_mutex_init (&mutex_3, nullptr);
      intermediateMap = IntermediateMap (multiThreadLevel, nullptr);
      sortBarrier = Barrier (multiThreadLevel);
      sem_init(&shuffleSemaphore, 0, 0);
    }

    ~JobContext ()
    {
      pthread_mutex_destroy (&mutex_3);
      sem_destroy(&shuffleSemaphore);
      delete[] threads;
    }
};


/**
 * Compares two IntermediatePair objects by comparing the first element of each pair
 * @param pair1 The first pair
 * @param pair2 The second pair
 * @return True if the first pair is less than the second pair
 */
bool comparePairs (const IntermediatePair &pair1, const IntermediatePair &pair2)
{
  return *(pair1.first) < *(pair2.first);
}


// TODO: fix this function
void* mapReduceThread (void *context)
{
  JobContext *jobContext = static_cast<JobContext *>(context);
  const InputVec &inputVec = jobContext->inputVec;

  // Map phase
  *(jobContext->counter) = ((uint64_t)MAP_STAGE << BIT_62);
  while (*(jobContext->inputIndex) < static_cast<int>(jobContext->inputVec.size()))
  {
    int index = *(jobContext->inputIndex);
    (*(jobContext->inputIndex))++;
    jobContext->client.map (inputVec[index].first, inputVec[index].second, jobContext);
    (*jobContext->counter) += ((uint64_t)1 << BIT_31);
  }

  // Sort phase
  sortIntermediateVec (jobContext);

  // Wait for all threads to complete the sort phase
  jobContext->sortBarrier.barrier();

  // If this is the shuffle thread (thread 0), perform the shuffle operation
  if (pthread_equal(pthread_self(), jobContext->threads[0]))
  {
    // Shuffle phase
    *(jobContext->counter) = ((uint64_t)SHUFFLE_STAGE << BIT_62);
    shuffleIntermediateVec(jobContext);

    // Signal other threads that the shuffle phase is complete
    for (int i = 1; i < jobContext->multiThreadLevel; i++)
    {
      sem_post(&jobContext->shuffleSemaphore);
    }
  }
  else // For other threads, wait for the shuffle phase to complete
  {
    sem_wait(&jobContext->shuffleSemaphore);
  }

  // Reduce phase
  *(jobContext->counter) = ((uint64_t)REDUCE_STAGE << BIT_62);
  while (*(jobContext->reduceIndex) < static_cast<int>(jobContext->intermediateMap
  .size()))
  {
    int index = *(jobContext->reduceIndex);
    (*(jobContext->reduceIndex))++;
    jobContext->client.reduce (jobContext->intermediateMap[index],
                          jobContext);
    (*jobContext->counter) += ((uint64_t)1 << BIT_31);  // TODO: check if
    // the increment correct
  }
  return nullptr;
}

void emit2 (K2 *key, V2 *value, void *context)
{
  JobContext *jobContext = static_cast<JobContext *>(context);
  int index = getThreadIndex (pthread_self (), jobContext);
  (*jobContext->intermediateMap[index]).push_back ((std::make_pair (key,
                                                                    value)));
  ++(*jobContext->shuffledCount);
}

void emit3 (K3 *key, V3 *value, void *context)
{
  JobContext *jobContext = static_cast<JobContext *>(context);
  pthread_mutex_lock (&jobContext->mutex_3);
  OutputPair pair (key, value);
  jobContext->outputVec.push_back (pair);
  pthread_mutex_unlock (&jobContext->mutex_3);
}

JobHandle startMapReduceJob (const MapReduceClient &client,
                             const InputVec &inputVec, OutputVec &outputVec,
                             int multiThreadLevel)
{
  JobContext *jobContext = new JobContext (client, inputVec, outputVec, multiThreadLevel);
  *(jobContext->counter) = ((uint64_t)UNDEFINED_STAGE << BIT_62);

  for (int i = 0; i < multiThreadLevel; i++)
  {
    pthread_create (&jobContext->threads[i], nullptr, mapReduceThread, jobContext);
  }

  return static_cast<JobHandle>(jobContext);
}

void waitForJob (JobHandle job)
{
  JobContext *jobContext = static_cast<JobContext *>(job);
  pthread_mutex_lock (&jobContext->waitMutex);
  if (!jobContext->isJoined)
  {
    for (int i = 0; i < jobContext->multiThreadLevel; i++)
    {
      int success = pthread_join (jobContext->threads[i], nullptr);
      if (success != 0)
      {
        std::cout << "system error: Failed to join the thread" << std::endl;
        exit (1);
      }
    }
    jobContext->isJoined = true;
  }
  pthread_mutex_unlock (&jobContext->waitMutex);
}

void getJobState (JobHandle job, JobState *state)
{
  JobContext *jobContext = (JobContext *)job;
  pthread_mutex_lock(&jobContext->stateMutex);
  uint64_t current_state = *(jobContext->counter);
  state->stage = static_cast<stage_t>(current_state >> BIT_62);
  uint64_t completedTasks = ((current_state >> BIT_31) & BITS_0_31);
  int totalTasks;
    if (state->stage == UNDEFINED_STAGE)
    {
        totalTasks = 1;  //TODO: check what to to in this case
    }
    else if (state->stage == MAP_STAGE)
    {
        totalTasks = jobContext->inputVec.size();
    }
    else if (state->stage == SHUFFLE_STAGE)
    {
        totalTasks = *jobContext->shuffledCount;
    }
    else
    {
        totalTasks = *jobContext->shuffledCount;
    }
  state->percentage = ((float)completedTasks / totalTasks) * 100;
  pthread_mutex_unlock(&jobContext->stateMutex);
}

void closeJobHandle (JobHandle job)
{
  JobContext *jobContext = static_cast<JobContext *>(job);

  // Ensure that the job has finished
  waitForJob (job);

  // TODO: Destroy any mutexes or semaphores
  // pthread_mutex_destroy(&jobContext->someMutex);
  // sem_destroy(&jobContext->someSemaphore);

  delete jobContext;
  job = nullptr;
}

int getThreadIndex (pthread_t thread, JobContext *context)
{
  for (int i = 0; i < context->multiThreadLevel; i++)
  {
    if (context->threads[i] == thread)
    {
      return i;
    }
  }
  std::cout << "system error: " << THREAD_NOT_FOUND << std::endl;
  exit (1);
}

void sortIntermediateVec (JobContext *context)
{
  JobContext *jobContext = static_cast<JobContext *>(context);
  int threadIndex =  getThreadIndex (pthread_self (), jobContext);
    std::sort (jobContext->intermediateMap[threadIndex]->begin (),
                 jobContext->intermediateMap[threadIndex]->end (), comparePairs);
}

void shuffleIntermediateVec(JobContext *context)
{
  IntermediateMap groupedVecs;

  while (!context->intermediateMap.empty())
  {
    IntermediatePair maxPair = context->intermediateMap[0]->back();
    for (size_t j = 1; j < context->intermediateMap.size(); j++)
    {
      if (context->intermediateMap[j]->empty())
      {
        continue;
      }
      IntermediatePair currentPair = context->intermediateMap[j]->back();
      if (*maxPair.first < *currentPair.first)
      {
        maxPair = currentPair;
      }
    }

    IntermediateVec sameKeyVec;
    for (auto& vec : context->intermediateMap)
    {
      while (!vec->empty() && !(*maxPair.first < *vec->back().first) && !(*vec->back().first < *maxPair.first))
      {
        sameKeyVec.push_back(vec->back());
        vec->pop_back();
      }
    }
    groupedVecs.push_back(&sameKeyVec);
    (*context->counter) += ((uint64_t)1 << BIT_31);
  }
  context->intermediateMap = groupedVecs;
}