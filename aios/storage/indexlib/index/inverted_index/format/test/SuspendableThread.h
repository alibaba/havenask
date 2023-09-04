#pragma once
#include <functional>
#include <memory>
#include <pthread.h>
#include <signal.h>

#include "autil/Log.h"

#define SIGRESUME SIGUSR2
#define SIGSUSPEND SIGUSR1

namespace indexlib::index {

class SuspendableThread
{
public:
    SuspendableThread();
    ~SuspendableThread() { join(); }

    pthread_t getId() const { return _id; }
    void join();

    // when create thread fail, return null ThreadPtr
    // need hold return value else will hang to the created thread terminate
    static std::shared_ptr<SuspendableThread> createThread(const std::function<void()>& threadFunction);

    void Suspend() { pthread_kill(_id, SIGSUSPEND); }
    void Resume() { pthread_kill(_id, SIGRESUME); }

private:
    static void* threadWrapperFunction(void* arg);

    pthread_t _id;

    AUTIL_LOG_DECLARE();
};

} // namespace indexlib::index
