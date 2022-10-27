#include "indexlib/index/normal/inverted_index/perf_test/suspendable_thread.h"

using namespace std;

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, SuspendableThread);

/* Thread specied var, the status*/
static __thread int threadSuspendStatus;

void SuspendHandler(int signum)
{
    threadSuspendStatus = 1;
    
    sigset_t nset;
    pthread_sigmask(0, NULL, &nset);

    // make sure that the resume is not blocked
    sigdelset(&nset, SIGRESUME);
    while(threadSuspendStatus) sigsuspend(&nset);
}

void ResumeHandler(int signum)
{
    threadSuspendStatus = 0;
}

SuspendableThread::SuspendableThread()
{
    struct sigaction suspendsa;
    suspendsa.sa_handler =  SuspendHandler;
    sigaddset(&suspendsa.sa_mask, SIGRESUME);
    sigaction(SIGSUSPEND, &suspendsa, NULL);
    
    struct sigaction resumesa;
    resumesa.sa_handler = ResumeHandler;
    sigaddset(&resumesa.sa_mask, SIGSUSPEND);
    sigaction(SIGRESUME, &resumesa, NULL);
}

void SuspendableThread::join()
{
    if (mId)
    {
        int ret = pthread_join(mId, NULL);
        (void) ret; assert(ret == 0);
    }
    mId = 0;
}

struct ThreadFunctionWrapperArg
{
    std::tr1::function<void ()> threadFunction;
};

SuspendableThreadPtr SuspendableThread::createThread(
        const std::tr1::function<void ()>& threadFunction)
{
    SuspendableThreadPtr threadPtr(new SuspendableThread);
    ThreadFunctionWrapperArg* arg = new ThreadFunctionWrapperArg;
    arg->threadFunction = threadFunction;
    
    int rtn = pthread_create(&threadPtr->mId, NULL,
                             &SuspendableThread::threadWrapperFunction, arg);
    
    if (rtn != 0) {
        delete arg;
        threadPtr->mId = 0;
        threadPtr.reset();
        IE_LOG(ERROR, "Create thread error [%d]", rtn);
    }
    
    return threadPtr;
}

void* SuspendableThread::threadWrapperFunction(void* arg)
{
    unique_ptr<ThreadFunctionWrapperArg> p(static_cast<ThreadFunctionWrapperArg*>(arg));
    p->threadFunction();
    return NULL;
}

IE_NAMESPACE_END(index);

