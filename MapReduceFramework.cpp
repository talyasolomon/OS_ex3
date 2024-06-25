#include "MapReduceFramework.h"
#include <pthread.h>
#include <atomic>
#include <iostream>
#include <algorithm>
#include <semaphore.h>
#include "Barrier/Barrier.h"

#define THREAD_NOT_FOUND "Thread not found in the system"

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
    stage_t stage;
    bool *joined;
    std::atomic<int> *totalTasks;
    std::atomic<int> *completedTasks;
    std::atomic<int> *inputIndex = 0;
    std::atomic<int> *shuffledCount = 0;
    IntermediateMap intermediateMap;
    IntermediateMap shuffledVec;
    pthread_mutex_t mutex_2;
    pthread_mutex_t mutex_3;
    Barrier sortBarrier;
    sem_t shuffleSemaphore;

    JobContext (const MapReduceClient &client, const InputVec &inputVec, OutputVec &outputVec, int multiThreadLevel)
        : client (client), inputVec (inputVec), outputVec (outputVec),
          multiThreadLevel (multiThreadLevel), stage (UNDEFINED_STAGE),
          completedTasks (0), totalTasks (0), inputIndex (0), shuffledCount
          (0), sortBarrier
          (multiThreadLevel), intermediateMap (multiThreadLevel, nullptr),
          shuffledVec (multiThreadLevel, nullptr)
    {
      totalTasks = new std::atomic<int> (inputVec.size ());
      completedTasks = new std::atomic<int> (0);
      threads = new pthread_t[multiThreadLevel];
      threads = new pthread_t[multiThreadLevel];
      joined = new bool[multiThreadLevel] (); // Initialize all elements to false
      pthread_mutex_init (&mutex_2, nullptr);
      pthread_mutex_init (&mutex_3, nullptr);
      intermediateMap = IntermediateMap (multiThreadLevel, nullptr);
      sortBarrier = Barrier (multiThreadLevel);
      sem_init(&shuffleSemaphore, 0, 0);
    }

    ~JobContext ()
    {
      pthread_mutex_destroy (&mutex_2);
      pthread_mutex_destroy (&mutex_3);
      sem_destroy(&shuffleSemaphore);
      delete totalTasks;
      delete completedTasks;
      delete[] threads;
      delete[] joined;
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
void *mapReduceThread (void *context)
{
  JobContext *jobContext = static_cast<JobContext *>(context);
  const InputVec &inputVec = jobContext->inputVec;

  // Map phase
  jobContext->stage = MAP_STAGE;
  while (*(jobContext->inputIndex) < jobContext->inputVec.size ())
  {
    int index = *(jobContext->inputIndex);
    (*(jobContext->inputIndex))++;
    jobContext->client.map (inputVec[index].first, inputVec[index].second, jobContext);
  }

  // Sort phase
  sortIntermediateVec (jobContext);

  // Wait for all threads to complete the sort phase
  jobContext->sortBarrier.barrier();

  // If this is the shuffle thread (thread 0), perform the shuffle operation
  if (pthread_self() == jobContext->threads[0])
  {
    // Shuffle phase
    jobContext->stage = SHUFFLE_STAGE;
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
  jobContext->completedTasks++;
  pthread_mutex_unlock (&jobContext->mutex_3);
}

JobHandle startMapReduceJob (const MapReduceClient &client,
                             const InputVec &inputVec, OutputVec &outputVec,
                             int multiThreadLevel)
{
  JobContext *jobContext = new JobContext (client, inputVec, outputVec, multiThreadLevel);

  for (int i = 0; i < multiThreadLevel; i++)
  {
    pthread_create (&jobContext->threads[i], nullptr, mapReduceThread, jobContext);
  }

  return static_cast<JobHandle>(jobContext);
}

void waitForJob (JobHandle job)
{
  JobContext *jobContext = static_cast<JobContext *>(job);

  for (int i = 0; i < jobContext->multiThreadLevel; i++)
  {
    if (!jobContext->joined[i])
    {
      pthread_join (jobContext->threads[i], nullptr);
      jobContext->joined[i] = true;
    }
  }
}

void getJobState (JobHandle job, JobState *state)
{
  JobContext *jobContext = static_cast<JobContext *>(job);

  state->stage = jobContext->stage;
  state->percentage = (*(jobContext->completedTasks) / *
      (jobContext->totalTasks)) * 100;
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
  for (int i = 0 ; i < context->intermediateMap.size(); i++)
  {
    std::sort(context->intermediateMap[i]->begin (),
              context->intermediateMap[i]->end (), comparePairs);
  }
}

void shuffleIntermediateVec(JobContext *context)
{
  IntermediateMap groupedVecs;

  while (!context->intermediateMap.empty())
  {
    IntermediatePair maxPair = context->intermediateMap[0]->back();
    for (int j = 1; j < context->intermediateMap.size(); j++)
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
    (*context->shuffledCount)++;
  }
  context->intermediateMap = groupedVecs;
}