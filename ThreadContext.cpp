#include <atomic>
#include <vector>
#include "Barrier.h"
#include "MapReduceFramework.h"
#include "EmitContext.hpp"

class ThreadContext {
public:
    unsigned int thread_id;
    unsigned int multi_thread_level;
    std::atomic<int> *atomic_index_ptr;
    Barrier *barrier_ptr;
    const MapReduceClient *client_ptr;
    stage_t *stage_ptr;
    std::atomic<int> *num_completed_ptr;
    const InputVec *input_vec_ptr;
    std::vector<IntermediateVec> *shuffled_vectors_ptr;
    unsigned int *denominator_ptr;
    Emit2Context emit2_context;
    Emit3Context emit3_context;
    IntermediateVec *intermediate_vector_ptr;
    pthread_mutex_t *stage_mutex_ptr;
public:
    ThreadContext(
            unsigned int thread_id,
            std::atomic<int> *atomic_input_vec_index_ptr,
            Barrier *barrier_ptr,
            const std::vector<InputPair> *input_vec_ptr,
            const MapReduceClient *client_ptr,
            std::atomic<int> *num_completed_ptr,
            stage_t *stage_ptr,
            unsigned int multi_thread_level,
            std::vector<std::vector<IntermediatePair>> *shuffled_vectors_ptr,
            unsigned int *denominator_ptr,
            Emit2Context emit2_context_ptr,
            Emit3Context emit3_context_ptr,
            IntermediateVec *intermediate_vector_ptr,
            pthread_mutex_t *stage_mutex_ptr
    ) : thread_id(thread_id),
        multi_thread_level(multi_thread_level),
        atomic_index_ptr(atomic_input_vec_index_ptr),
        barrier_ptr(barrier_ptr),
        client_ptr(client_ptr),
        stage_ptr(stage_ptr),
        num_completed_ptr(num_completed_ptr),
        input_vec_ptr(input_vec_ptr),
        shuffled_vectors_ptr(shuffled_vectors_ptr),
        denominator_ptr(denominator_ptr),
        emit2_context(emit2_context_ptr),
        emit3_context(emit3_context_ptr),
        intermediate_vector_ptr(intermediate_vector_ptr),
        stage_mutex_ptr(stage_mutex_ptr) {}

    ThreadContext() {}
};