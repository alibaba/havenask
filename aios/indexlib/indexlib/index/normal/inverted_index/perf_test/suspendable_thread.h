#ifndef __INDEXLIB_SUSPENDABLE_THREAD_H
#define __INDEXLIB_SUSPENDABLE_THREAD_H

#include <tr1/memory>
#include <signal.h>
#include <pthread.h>
#include <tr1/memory>
#include <tr1/functional>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"

#define SIGRESUME SIGUSR2
#define SIGSUSPEND SIGUSR1

IE_NAMESPACE_BEGIN(index);

class SuspendableThread;
DEFINE_SHARED_PTR(SuspendableThread);

class SuspendableThread
{
public:
    SuspendableThread();
    ~SuspendableThread() { join(); }

public:
    pthread_t getId() const { return mId; }

    void join();

public:
    // when create thread fail, return null ThreadPtr
    // need hold return value else will hang to the created thread terminate
    static SuspendableThreadPtr createThread(const std::tr1::function<void ()>& threadFunction);

private:
    static void* threadWrapperFunction(void* arg);


public:
    void Suspend()
    { pthread_kill(mId, SIGSUSPEND); }

    void Resume()
    { pthread_kill(mId, SIGRESUME); }

private:
    pthread_t mId;

private:
    IE_LOG_DECLARE();
};

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_SUSPENDABLE_THREAD_H
