#ifndef __EMIT_CONTEXT_HPP__
#define __EMIT_CONTEXT_HPP__

#include <semaphore.h>

template<class T>
class EmitContext {
public:
    sem_t *sem_p;
    T *vector_p;
public:
    EmitContext(sem_t *sem_p, T *vector_p) : sem_p(sem_p), vector_p(vector_p) {}

    EmitContext() : sem_p(nullptr), vector_p(nullptr) {}
};

typedef EmitContext<IntermediateVec > Emit2Context;
typedef EmitContext<OutputVec > Emit3Context;

#endif //__EMIT_CONTEXT_HPP__