#include "MapReduceFramework.h"
#include "Job.h"
#include <iostream>
#include "EmitContext.hpp"
#include "utils.h"


void emit2(K2 *key, V2 *value, void *context) {
    Emit2Context *emit_context = (Emit2Context *) context;
    EXIT_IF(sem_wait(emit_context->sem_p) != 0, "sem_wait failed");
    emit_context->vector_p->push_back(IntermediatePair(key, value));
    EXIT_IF(sem_post(emit_context->sem_p) != 0, "sem_post failed");
}

void emit3(K3 *key, V3 *value, void *context) {
    Emit3Context *emit_context = (Emit3Context *) context;
    EXIT_IF(sem_wait(emit_context->sem_p) != 0, "sem_wait failed");
    emit_context->vector_p->push_back(OutputPair(key, value));
    EXIT_IF(sem_post(emit_context->sem_p) != 0, "sem_post failed");
}

JobHandle
startMapReduceJob(const MapReduceClient &client, const InputVec &inputVec, OutputVec &outputVec, int multiThreadLevel) {
    Job *job = new(std::nothrow) Job(client, inputVec, outputVec, multiThreadLevel);
    EXIT_IF(job == nullptr, "bad alloc");
    return job;
}

void waitForJob(JobHandle job) {
    Job *j = (Job *) job;
    j->waitForJob();
}

void getJobState(JobHandle job, JobState *state) {
    Job *j = (Job *) job;
    JobState s = j->getJobState();
    state->percentage = s.percentage;
    state->stage = s.stage;
}

void closeJobHandle(JobHandle job) {
    Job *j = (Job *) job;
    j->closeJobHandle();
}
