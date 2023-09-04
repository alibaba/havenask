#include "indexlib/index/inverted_index/format/test/SuspendableThread.h"

namespace indexlib::index {
AUTIL_LOG_SETUP(indexlib.index, SuspendableThread);

/* Thread specied var, the status*/
static __thread int threadSuspendStatus;

void SuspendHandler(int signum)
{
    threadSuspendStatus = 1;

    sigset_t nset;
    pthread_sigmask(0, nullptr, &nset);

    // make sure that the resume is not blocked
    sigdelset(&nset, SIGRESUME);
    while (threadSuspendStatus)
        sigsuspend(&nset);
}

void ResumeHandler(int signum) { threadSuspendStatus = 0; }

SuspendableThread::SuspendableThread()
{
    struct sigaction suspendsa;
    suspendsa.sa_handler = SuspendHandler;
    sigaddset(&suspendsa.sa_mask, SIGRESUME);
    sigaction(SIGSUSPEND, &suspendsa, nullptr);

    struct sigaction resumesa;
    resumesa.sa_handler = ResumeHandler;
    sigaddset(&resumesa.sa_mask, SIGSUSPEND);
    sigaction(SIGRESUME, &resumesa, nullptr);
}

void SuspendableThread::join()
{
    if (_id) {
        int ret = pthread_join(_id, nullptr);
        (void)ret;
        assert(ret == 0);
    }
    _id = 0;
}

struct ThreadFunctionWrapperArg {
    std::function<void()> threadFunction;
};

std::shared_ptr<SuspendableThread> SuspendableThread::createThread(const std::function<void()>& threadFunction)
{
    std::shared_ptr<SuspendableThread> threadPtr(new SuspendableThread);
    ThreadFunctionWrapperArg* arg = new ThreadFunctionWrapperArg;
    arg->threadFunction = threadFunction;

    int rtn = pthread_create(&threadPtr->_id, nullptr, &SuspendableThread::threadWrapperFunction, arg);

    if (rtn != 0) {
        delete arg;
        threadPtr->_id = 0;
        threadPtr.reset();
        AUTIL_LOG(ERROR, "Create thread error [%d]", rtn);
    }

    return threadPtr;
}

void* SuspendableThread::threadWrapperFunction(void* arg)
{
    std::unique_ptr<ThreadFunctionWrapperArg> p(static_cast<ThreadFunctionWrapperArg*>(arg));
    p->threadFunction();
    return nullptr;
}

} // namespace indexlib::index
