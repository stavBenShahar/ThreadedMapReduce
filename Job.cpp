#include "Job.h"
#include "pthread.h"
#include <vector>
#include <algorithm>
#include <iostream>
#include <semaphore.h>
#include "utils.h"
#include <map>

//forum said to print to stdout
// https://moodle2.cs.huji.ac.il/nu22/mod/forum/discuss.php?d=79558#p129002
//TODO changed to stderr and made a to smaller case 's' at the start for matan test
#define PTHREAD_OK 0
#define SEM_OK 0

struct IntermediateVecComparer {
    bool operator()(const IntermediatePair &a, const IntermediatePair &b) const {
        return *(a.first) < *(b.first);
    }
};

void map_phase(ThreadContext *tc) {
    unsigned int curr_index;
    while ((curr_index = (*(tc->atomic_index_ptr))++) < tc->input_vec_ptr->size()) {
        ///get current pair
        const InputPair &curr_pair = tc->input_vec_ptr->at(curr_index);
        ///map the pair's values and supply relevant context
        tc->client_ptr->map(curr_pair.first, curr_pair.second, &tc->emit2_context);
        ///increase percent of processed items by relative amount of a single item
        (*(tc->num_completed_ptr))++;
    }
}

void sort_phase(ThreadContext *tc) {
    //TODO what happens if there are threads that didnt map anything?
    std::sort(
            std::begin(tc->intermediate_vector_ptr[tc->thread_id]),
            std::end(tc->intermediate_vector_ptr[tc->thread_id]),
            [](const IntermediatePair &lhs, const IntermediatePair &rhs) {
                return *lhs.first < *rhs.first;
            }
    );
}

void shuffle_phase(ThreadContext *tc) {
    ///reset current state
    EXIT_IF(pthread_mutex_lock(tc->stage_mutex_ptr) != 0, "pthread_mutex_lock failed");
    *(tc->stage_ptr) = SHUFFLE_STAGE;
    *(tc->num_completed_ptr) = 0;
    *(tc->denominator_ptr) = 0;
    for (unsigned int i = 0; i < tc->multi_thread_level; i++) {
        tc->intermediate_vector_ptr[i].shrink_to_fit();
        *(tc->denominator_ptr) += tc->intermediate_vector_ptr[i].size();
    }
    EXIT_IF(pthread_mutex_unlock(tc->stage_mutex_ptr) != 0, "pthread_mutex_lock failed");

    ///shuffling
    auto intermediate_map = std::map<IntermediatePair, IntermediateVec, IntermediateVecComparer>();
    for (unsigned int i = 0; i < tc->multi_thread_level; i++) {
        std::vector<IntermediatePair> &current_vec = tc->intermediate_vector_ptr[i];
        for (IntermediatePair &pair: current_vec) {
            auto it = intermediate_map.find(pair);
            if (it == intermediate_map.end())
                intermediate_map.insert({pair, std::vector<IntermediatePair>()});
            intermediate_map.at(pair).push_back(pair);
            (*(tc->num_completed_ptr))++;
        }
    }
    for (auto &pair: intermediate_map)
        tc->shuffled_vectors_ptr->push_back(pair.second);

    ///prepare for next stage
    *(tc->atomic_index_ptr) = 0;
    EXIT_IF(pthread_mutex_lock(tc->stage_mutex_ptr) != 0, "pthread_mutex_lock failed");
    *(tc->stage_ptr) = REDUCE_STAGE;
    *(tc->denominator_ptr) = tc->shuffled_vectors_ptr->size();
    *(tc->num_completed_ptr) = 0;
    EXIT_IF(pthread_mutex_unlock(tc->stage_mutex_ptr) != 0, "pthread_mutex_lock failed");
}

void reduce_phase(ThreadContext *tc) {
    unsigned int curr_index;
    while ((curr_index = (*(tc->atomic_index_ptr))++) < tc->shuffled_vectors_ptr->size()) {
        tc->client_ptr->reduce(&((*(tc->shuffled_vectors_ptr))[curr_index]), &tc->emit3_context);
        (*(tc->num_completed_ptr))++;
    }
}

void *thread_entry_point(void *arg) {
    ThreadContext *tc = (ThreadContext *) arg;
    if (tc->thread_id == 0) {
        EXIT_IF(pthread_mutex_lock(tc->stage_mutex_ptr) != 0, "pthread_mutex_lock failed");
        *(tc->stage_ptr) = MAP_STAGE;
        *(tc->denominator_ptr) = tc->input_vec_ptr->size();
        EXIT_IF(pthread_mutex_unlock(tc->stage_mutex_ptr) != 0, "pthread_mutex_lock failed");
    }
    map_phase(tc);
    sort_phase(tc);
    tc->barrier_ptr->barrier();
    if (tc->thread_id == 0) {
        shuffle_phase(tc);
    }
    tc->barrier_ptr->barrier();
    reduce_phase(tc);
    return nullptr;
}

//TODO block signals somewhere?
Job::Job(const MapReduceClient &client_, const InputVec &inputVec, OutputVec &outputVec, int multiThreadLevel) :
    client(client_),
    input_vec(inputVec),
    multi_thread_level(multiThreadLevel),
    denominator(0),
    num_completed(0),
    atomic_input_vec_index(0),
    barrier(multiThreadLevel),
    output_vec(outputVec),
    emit_sem_ptr(nullptr),
    threads(nullptr),
    contexts(nullptr),
    intermediate_vectors_ptr(nullptr) {
    ///=============== INIT VARIABLES ==================
    allocate();
    EXIT_IF(pthread_mutex_lock(&stage_mutex) != 0, "pthread_mutex_lock failed");
    this->stage = UNDEFINED_STAGE;
    this->num_completed = 0;
    this->denominator = 1;
    EXIT_IF(pthread_mutex_unlock(&stage_mutex) != 0, "pthread_mutex_lock failed");
    ///=============== INIT CONTEXTS ==================
    for (unsigned int i = 0; i < this->multi_thread_level; ++i) {
        //shallow copy is completely fine here for our needs
        contexts[i] = ThreadContext(
                i,
                &atomic_input_vec_index,
                &barrier,
                &input_vec,
                &client,
                &num_completed,
                &stage,
                multi_thread_level,
                &shuffled_vectors,
                &denominator,
                (Emit2Context) {emit_sem_ptr, intermediate_vectors_ptr + i},
                (Emit3Context) {emit_sem_ptr, &output_vec},
                intermediate_vectors_ptr,
                &stage_mutex
        );
    }

    ///=============== START THREADS ==================
    for (unsigned int i = 0; i < this->multi_thread_level; ++i) {
        CLEAN_AND_EXIT_IF(pthread_create(threads + i, nullptr, thread_entry_point, contexts + i) != PTHREAD_OK,
                          "pthread_create failed");
    }
}

Job::~Job() {
    if (!has_been_deleted) clean();
}

void Job::waitForJob() {

    CLEAN_AND_EXIT_IF(pthread_mutex_lock(&wait_for_job_mutex) != 0, "pthread_mutex_lock failed.")
    if (has_waited) {
        CLEAN_AND_EXIT_IF(pthread_mutex_unlock(&wait_for_job_mutex) != 0, "pthread_mutex_unlock failed.")
        return;
    }
    for (unsigned int i = 0; i < multi_thread_level; ++i)
        CLEAN_AND_EXIT_IF(pthread_join(threads[i], nullptr) != PTHREAD_OK, "pthread_join failed.")
    has_waited = true;
    CLEAN_AND_EXIT_IF(pthread_mutex_unlock(&wait_for_job_mutex) != 0, "pthread_mutex_unlock failed.")
}


JobState Job::getJobState() {
    CLEAN_AND_EXIT_IF(pthread_mutex_lock(&stage_mutex) != 0, "pthread_mutex_lock failed");
    float denom = denominator;
    if (denom == 0) denom = 1;
    int nominator = num_completed.load();
    float percent = 100 * (nominator / denom);
    JobState state = {stage, percent};
    CLEAN_AND_EXIT_IF(pthread_mutex_unlock(&stage_mutex) != 0, "pthread_mutex_lock failed");
    if (state.percentage > 100 || state.percentage < 0) {
        std::cout << "";
    }
    return state;
}

void Job::closeJobHandle() {
    this->waitForJob();
    delete this;
}

void Job::clean() {
    has_been_deleted = true;
    // deleting nullptr has no effect so no need to check this
    delete[] threads;
    threads = nullptr;

    delete[] contexts;
    contexts = nullptr;

    delete[] intermediate_vectors_ptr;
    intermediate_vectors_ptr = nullptr;

    if (emit_sem_ptr != nullptr)
        EXIT_IF(sem_destroy(emit_sem_ptr) != SEM_OK, "sem_destroy failed.")
    delete emit_sem_ptr;
  emit_sem_ptr = nullptr;

    EXIT_IF(pthread_mutex_destroy(&wait_for_job_mutex) != 0, "pthread_mutex_destroy failed.");

    EXIT_IF(pthread_mutex_destroy(&stage_mutex) != 0, "pthread_mutex_destroy failed.");
}

void Job::allocate() {
  emit_sem_ptr = new(std::nothrow) sem_t;
    CLEAN_AND_EXIT_IF(emit_sem_ptr == nullptr, "bad alloc");
    if (sem_init(emit_sem_ptr, 0, 1) != SEM_OK) {
        //we need to do it like this because of the edge case where we
        // successfully allocated the emit2_sem but didn't manage to initialize it.
        //If so, how can we destroy what we did not initialize properly?
        //this edge case happens because clean will sem_destroy iff the pointer
        // is not nullptr which will always be the case if the allocation succeed
        delete emit_sem_ptr;
      emit_sem_ptr = nullptr;
        CLEAN_AND_EXIT_IF(true, "sem_init failed.");
    }

    CLEAN_AND_EXIT_IF(pthread_mutex_init(&wait_for_job_mutex, nullptr) != 0, "failed to init mutex.")

    CLEAN_AND_EXIT_IF(pthread_mutex_init(&stage_mutex, nullptr) != 0, "failed to init mutex.")

    threads = new(std::nothrow)pthread_t[multi_thread_level];
    CLEAN_AND_EXIT_IF(threads == nullptr, "bad alloc");

    contexts = new(std::nothrow)ThreadContext[multi_thread_level];
    CLEAN_AND_EXIT_IF(contexts == nullptr, "bad alloc");

    intermediate_vectors_ptr = new(std::nothrow)std::vector<IntermediatePair>[multi_thread_level];
    CLEAN_AND_EXIT_IF(intermediate_vectors_ptr == nullptr, "bad alloc");
}

