#include "MapReduceFramework.h"
#include <pthread.h>
#include <atomic>

struct JobContext {
    const MapReduceClient& client;
    const InputVec& inputVec;
    OutputVec& outputVec;
    int multiThreadLevel;
    pthread_t* threads;
    stage_t stage;
    bool* joined;
    std::atomic<int> totalTasks;
    std::atomic<int> completedTasks;
    std::vector<IntermediatePair> intermediateVec;
    std::atomic<int> intermediateCount;
    std::atomic<int> outputCount;
    pthread_mutex_t mutex_2;
    pthread_mutex_t mutex_3;
    // TODO: Add any additional data (mutexes, semaphores)


    JobContext(const MapReduceClient& client, const InputVec& inputVec, OutputVec& outputVec, int multiThreadLevel)
        : client(client), inputVec(inputVec), outputVec(outputVec),
        multiThreadLevel(multiThreadLevel), stage(UNDEFINED_STAGE), totalTasks(inputVec.size()), completedTasks(0) {
      threads = new pthread_t[multiThreadLevel];
      joined = new bool[multiThreadLevel](); // Initialize all elements to false
      pthread_mutex_init(&mutex_2, nullptr);
      pthread_mutex_init(&mutex_3, nullptr);
    }

    ~JobContext() {
      pthread_mutex_destroy(&mutex_2);
      pthread_mutex_destroy(&mutex_3);
      delete[] threads;
      delete[] joined;
    }
};

// TODO: fix this function
void* mapReduceThread(void* context) {
  // Cast the context back to JobContext
  JobContext* jobContext = static_cast<JobContext*>(context);

  // Get the input vector from the job context
  const InputVec& inputVec = jobContext->inputVec;

  // For each input pair, call the client's map function
  for (const auto& inputPair : inputVec) {
    jobContext->client.map(inputPair.first, inputPair.second);
  }

  // After all map operations are done, call the client's reduce function
  // You might need to modify this part depending on how you're storing the intermediate data
  jobContext->client.reduce(/* pass the intermediate data here */);
  jobContext->completedTasks++;

  return nullptr;
}

void emit2 (K2* key, V2* value, void* context)
{
  JobContext* jobContext = static_cast<JobContext*>(context);
  pthread_mutex_lock(&jobContext->mutex_2);
  IntermediatePair pair(key, value);
  jobContext->intermediateVec.push_back(pair);
  jobContext->intermediateCount++;
  pthread_mutex_unlock(&jobContext->mutex_2);
}

void emit3 (K3* key, V3* value, void* context)
{
  JobContext* jobContext = static_cast<JobContext*>(context);
  pthread_mutex_lock(&jobContext->mutex_3);
  OutputPair pair(key, value);
  jobContext->outputVec.push_back(pair);
  jobContext->outputCount++;
  pthread_mutex_unlock(&jobContext->mutex_3);
}

JobHandle startMapReduceJob(const MapReduceClient& client,
                            const InputVec& inputVec, OutputVec& outputVec,
                            int multiThreadLevel)
{
  JobContext* jobContext = new JobContext(client, inputVec, outputVec, multiThreadLevel);

  for (int i = 0; i < multiThreadLevel; i++) {
    pthread_create(&jobContext->threads[i], nullptr, mapReduceThread, jobContext);
  }

  return static_cast<JobHandle>(jobContext);
}

void waitForJob(JobHandle job)
{
  JobContext* jobContext = static_cast<JobContext*>(job);

  for (int i = 0; i < jobContext->multiThreadLevel; i++) {
    if (!jobContext->joined[i]) {
      pthread_join(jobContext->threads[i], nullptr);
      jobContext->joined[i] = true;
    }
  }
}

void getJobState(JobHandle job, JobState* state)
{
  JobContext* jobContext = static_cast<JobContext*>(job);

  state->stage = jobContext->stage;
  state->percentage = (jobContext->completedTasks / jobContext->totalTasks) * 100;
}

void closeJobHandle(JobHandle job)
{
  JobContext* jobContext = static_cast<JobContext*>(job);

  // Ensure that the job has finished
  waitForJob(job);

  // TODO: Destroy any mutexes or semaphores
  // pthread_mutex_destroy(&jobContext->someMutex);
  // sem_destroy(&jobContext->someSemaphore);

  delete jobContext;
  job = nullptr;
}