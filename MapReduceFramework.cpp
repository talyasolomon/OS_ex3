#include "MapReduceFramework.h"
#include <pthread.h>
#include <atomic>
#include <iostream>
#include <algorithm>
#include <semaphore.h>
#include "Barrier.h"

/** Error messages */
#define THREAD_NOT_FOUND "Thread not found in the system"
#define FAILED_TO_JOIN "Failed to join the thread"
#define FAILED_TO_LOCK "Failed to lock the mutex"
#define FAILED_TO_UNLOCK "Failed to unlock the mutex"
#define FAILED_TO_CREATE_THREAD "Failed to create the thread"

typedef std::vector<IntermediateVec *> IntermediateMap;

/** Function declarations */
struct JobContext;
void sortIntermediateVec (JobContext *context);
int getThreadIndex (pthread_t thread, JobContext *context);
void shuffleIntermediateVec (JobContext *context);
void mapPhase (JobContext *jobContext);
void reducePhase (JobContext *jobContext);
void *mapReduceThread (void *context);

IntermediatePair findMaxKey (const JobContext *context);

void initCounters (JobContext *jobContext, stage_t stage, int totalTasks);
struct JobContext {
    const MapReduceClient &client;
    const InputVec &inputVec;
    OutputVec &outputVec;
    int multiThreadLevel;
    pthread_t *threads;
    bool isJoined = false;
//    std::atomic<uint64_t> *counter; // [62, 63] - stage, [31, 61] - total tasks, [0, 30] - completed tasks
    std::atomic<int> *stageCounter;
    std::atomic<int> *totalTasksCounter;
    std::atomic<int> *completedCounter;
    std::atomic<int> *inputIndex;
    std::atomic<int> *reduceIndex;
    std::atomic<int> *keysCounter;
    IntermediateMap intermediateMap;
    IntermediateMap shuffledVec;
    pthread_mutex_t countersMutex;
    pthread_mutex_t mutex3;
    pthread_mutex_t stateMutex;
    pthread_mutex_t waitMutex;
    Barrier *mapBarrier;
    Barrier *sortBarrier;
    Barrier *shuffleBarrier;
    sem_t shuffleSemaphore;
    bool countersInit;

    JobContext (const MapReduceClient &client, const InputVec &inputVec, OutputVec &outputVec, int multiThreadLevel, Barrier barrier)
        : client (client), inputVec (inputVec), outputVec (outputVec),
          multiThreadLevel (multiThreadLevel)
    {
      threads = new pthread_t[multiThreadLevel];
//      counter = new std::atomic<uint64_t>(0);
      stageCounter = new std::atomic<int> (0);
      totalTasksCounter = new std::atomic<int> (1);
      completedCounter = new std::atomic<int> (0);
      inputIndex = new std::atomic<int> (0);
      reduceIndex = new std::atomic<int> (0);
      keysCounter = new std::atomic<int> (0);
      intermediateMap = IntermediateMap (multiThreadLevel);
      shuffledVec = IntermediateMap ();
      for (int i = 0; i < multiThreadLevel; ++i)
        {
          intermediateMap[i] = new IntermediateVec ();
        }
      pthread_mutex_init (&countersMutex, nullptr);
      pthread_mutex_init (&mutex3, nullptr);
      pthread_mutex_init (&stateMutex, nullptr);
      pthread_mutex_init (&waitMutex, nullptr);
      mapBarrier = new Barrier (multiThreadLevel);
      sortBarrier = new Barrier (multiThreadLevel);
      shuffleBarrier = new Barrier (multiThreadLevel);
      countersInit = false;

//      sem_init(&shuffleSemaphore, 0, 0);
    }

    ~JobContext ()
    {
      delete[] threads;
//      delete counter;
      delete stageCounter;
      delete totalTasksCounter;
      delete completedCounter;
      delete reduceIndex;
      delete keysCounter;
      for (auto &intermediateVec: intermediateMap)
        {
          delete intermediateVec;
        }
      for (auto &shuffledVecElem: shuffledVec)
        {
          delete shuffledVecElem;
        }
      pthread_mutex_destroy (&countersMutex);
      pthread_mutex_destroy (&mutex3);
      pthread_mutex_destroy (&waitMutex);
      pthread_mutex_destroy (&stateMutex);
      delete mapBarrier;
      delete sortBarrier;
      delete shuffleBarrier;
      sem_destroy (&shuffleSemaphore);
    }
};

/**
 * Compares two IntermediatePair objects by comparing the first element of each pair
 * @param pair1 The first pair
 * @param pair2 The second pair
 * @return True if the first pair is less than the second pair
 */
bool
comparePairs (const IntermediatePair &pair1, const IntermediatePair &pair2)
{
  return *(pair1.first) < *(pair2.first);
}

void lockMutex (pthread_mutex_t *mutex)
{
  if (pthread_mutex_lock (mutex) != 0)
    {
      std::cout << "system error: " << FAILED_TO_LOCK << std::endl;
      exit (1);
    }
}

void unlockMutex (pthread_mutex_t *mutex)
{
  if (pthread_mutex_unlock (mutex) != 0)
    {
      std::cout << "system error: " << FAILED_TO_UNLOCK << std::endl;
      exit (1);
    }
}

JobHandle startMapReduceJob (const MapReduceClient &client,
                             const InputVec &inputVec, OutputVec &outputVec,
                             int multiThreadLevel)
{
  JobContext *jobContext = new JobContext (client, inputVec, outputVec,
                                           multiThreadLevel, Barrier
                                               (multiThreadLevel));

  for (int i = 0; i < multiThreadLevel; ++i)
    {
      if (pthread_create (
          (jobContext->threads) + i, nullptr, mapReduceThread, jobContext)
          != 0)
        {
          std::cout << "system error: " << FAILED_TO_CREATE_THREAD
                    << std::endl;
          exit (1);
        }
    }

  return static_cast<JobHandle>(jobContext);
}

void *mapReduceThread (void *context)
{
  JobContext *jobContext = static_cast<JobContext *>(context);
  mapPhase (jobContext);

  // Sort phase
  sortIntermediateVec (jobContext);

  // Wait for all threads to complete the sort phase
  jobContext->sortBarrier->barrier ();

  if (pthread_equal (pthread_self (), jobContext->threads[0]))
    {
      // Shuffle phase
      jobContext->countersInit = false;

//    *(jobContext->counter) = ((uint64_t) SHUFFLE_STAGE << BIT_62);
      initCounters (jobContext, SHUFFLE_STAGE, jobContext->keysCounter->load ());
      shuffleIntermediateVec (jobContext);
      jobContext->countersInit = false;

//    *(jobContext->counter) = ((uint64_t) REDUCE_STAGE << BIT_62);
    }

  // SEMAPHORE OPTION
//     Signal other threads that the shuffle phase is complete
//    for (int i = 1; i < jobContext->multiThreadLevel; i++)
//    {
//      sem_post(&jobContext->shuffleSemaphore);
//    }
//  }
//  else // For other threads, wait for the shuffle phase to complete
//  {
//    sem_wait(&jobContext->shuffleSemaphore);
//  }

  jobContext->shuffleBarrier->barrier ();
  // Reduce phase
  reducePhase (jobContext);

  pthread_exit (nullptr);
}

void mapPhase (JobContext *jobContext)
{
  const InputVec &inputVec = jobContext->inputVec;
  initCounters (jobContext, MAP_STAGE, (int) jobContext->inputVec.size ());
  const int inputVecSize = *jobContext->totalTasksCounter;
  int index;
  while ((index = (*jobContext->inputIndex)++) < inputVecSize)
    {
      jobContext->client.map (inputVec[index].first, inputVec[index].second, jobContext);
      jobContext->completedCounter->fetch_add (1);
    }
}

void initCounters (JobContext *jobContext, stage_t stage, int totalTasks)
{
  lockMutex (&jobContext->countersMutex);

  if (!jobContext->countersInit)
    {
      *(jobContext->stageCounter) = stage;
      *(jobContext->completedCounter) = 0;
      *(jobContext->totalTasksCounter) = totalTasks;
      jobContext->countersInit = true;
    }
  unlockMutex (&jobContext->countersMutex);

}

void reducePhase (JobContext *jobContext)
{
//  std::cerr << "start reduce phase" << std::endl;
  initCounters (jobContext, REDUCE_STAGE, (int) jobContext->shuffledVec.size ());
  int vecSize = *jobContext->totalTasksCounter;
  int idx;
  while ((idx = (*(jobContext->reduceIndex))++) < vecSize)
    {
      IntermediateVec int_vec = static_cast<IntermediateVec>(*jobContext->shuffledVec[idx]);
      jobContext->client.reduce (jobContext->shuffledVec[idx],
                                 jobContext);
      (*jobContext->completedCounter).fetch_add (1);
    }
}

void emit2 (K2 *key, V2 *value, void *context)
{
  JobContext *jobContext = static_cast<JobContext *>(context);
  int index = getThreadIndex (pthread_self (), jobContext);
  (*jobContext->intermediateMap[index]).push_back((std::make_pair (key, value)));
  ++(*jobContext->keysCounter);
}

void emit3 (K3 *key, V3 *value, void *context)
{
  JobContext *jobContext = static_cast<JobContext *>(context);
  lockMutex (&jobContext->mutex3);
  OutputPair pair (key, value);
  jobContext->outputVec.push_back (pair);
  unlockMutex (&jobContext->mutex3);
}

void waitForJob (JobHandle job)
{
  JobContext *jobContext = static_cast<JobContext *>(job);
  lockMutex (&jobContext->waitMutex);
  if (!jobContext->isJoined)
    {
      for (int i = 0; i < jobContext->multiThreadLevel; i++)
        {
          int success = pthread_join (jobContext->threads[i], nullptr);
          if (success != 0)
            {
              std::cout << "system error:" << FAILED_TO_JOIN << std::endl;
              exit (1);
            }
        }
      jobContext->isJoined = true;
    }
  unlockMutex (&jobContext->waitMutex);
}

void getJobState (JobHandle job, JobState *state)
{
  JobContext *jobContext = (JobContext *) job;
  lockMutex (&jobContext->countersMutex);

  state->stage = static_cast<stage_t>(jobContext->stageCounter->load ());
  int completedTasks = jobContext->completedCounter->load ();
  int totalTasks = jobContext->totalTasksCounter->load ();
  state->percentage = ((float) completedTasks / totalTasks) * 100;
  unlockMutex (&jobContext->countersMutex);
}

void closeJobHandle (JobHandle job)
{
  JobContext *jobContext = (JobContext *) job;

  waitForJob (job);
  delete jobContext;
}

int getThreadIndex (pthread_t thread, JobContext *context)
{
  for (int i = 0; i < context->multiThreadLevel; ++i)
    {
      if (pthread_equal (context->threads[i], thread))
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
  int threadIndex = getThreadIndex (pthread_self (), jobContext);
  std::sort (jobContext->intermediateMap[threadIndex]->begin (),
             jobContext->intermediateMap[threadIndex]->end (), comparePairs);
}

void shuffleIntermediateVec (JobContext *context)
{

  while (*context->completedCounter < *context->totalTasksCounter)
    {
      IntermediatePair maxPair = findMaxKey (context);
      IntermediateVec *sameKeyVec = new IntermediateVec ();
      for (auto vec: context->intermediateMap)
        {
          while (!vec->empty () && !(*maxPair.first < *vec->back ().first)
                 && !(*vec->back ().first < *maxPair.first))
            {
              sameKeyVec->push_back (vec->back ());
              vec->pop_back ();
            }
        }

      context->shuffledVec.push_back (sameKeyVec);
      context->completedCounter->fetch_add ((int) sameKeyVec->size ());
    }
}

IntermediatePair findMaxKey (const JobContext *context)
{
  IntermediatePair maxPair = {nullptr, nullptr};
  for (auto j: context->intermediateMap)
    {
      if (j->empty ())
        {
          continue;
        }
      IntermediatePair currentPair = j->back ();
      if (maxPair.first == nullptr || *maxPair.first < *currentPair.first)
        {
          maxPair = currentPair;
        }
    }
  return maxPair;
}