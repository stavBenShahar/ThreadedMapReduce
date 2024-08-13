#ifndef _JOB_H_
#define _JOB_H_

#include "MapReduceFramework.h"
#include "MapReduceClient.h"
#include <atomic>
#include "Barrier.h"
#include <memory>
#include "ThreadContext.cpp"
//#include "shared_array.cpp"
#include "EmitContext.hpp"

class Job {
private:
    const MapReduceClient &client;
    const InputVec &input_vec;
    const unsigned int multi_thread_level;
    unsigned int denominator;
    stage_t stage;
    bool has_been_deleted = false;
    bool has_waited = false;
    std::atomic<int> num_completed;
    std::atomic<int> atomic_input_vec_index;
    std::vector<IntermediateVec> work_space;
    std::vector<IntermediateVec> shuffled_vectors;
    Barrier barrier;
    OutputVec &output_vec;

    sem_t *emit_sem_ptr;
    pthread_mutex_t wait_for_job_mutex;
    pthread_mutex_t stage_mutex;

    pthread_t *threads;
    ThreadContext *contexts;
    IntermediateVec *intermediate_vectors_ptr;
private:
    void clean();

    void allocate();


public:
    Job(const MapReduceClient &client_,
        const InputVec &inputVec, OutputVec &outputVec,
        int multiThreadLevel);

    ~Job();

    // we want some semaphore or other type of synchronization tool to "hold"
// the calling 'main' thread inside the function until the lock is released.
// we want to chagne the status of this lock iff all of the inner threads
// have finished their reduce stage, this can be achived with a barrier
    void waitForJob();

    JobState getJobState();

//wait and the free 'stuff'
    void closeJobHandle();
};

#endif //_JOB_H_
