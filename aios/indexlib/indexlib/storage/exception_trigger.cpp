#include "indexlib/storage/exception_trigger.h"
#include "indexlib/misc/singleton.h"

using namespace std;
using namespace autil;

IE_NAMESPACE_USE(misc);
IE_NAMESPACE_BEGIN(storage);
IE_LOG_SETUP(storage, ExceptionTrigger);

const size_t ExceptionTrigger::NO_EXCEPTION_COUNT = 1000000000;
ExceptionTrigger::ExceptionTrigger() 
    : mIOCount(0)
    , mNormalIOCount(NO_EXCEPTION_COUNT)
    , mIsPause(false)
{
}

ExceptionTrigger::~ExceptionTrigger() 
{
}

bool ExceptionTrigger::TriggerException() 
{
    ScopedLock lock(mLock);
    if (mIsPause)
    {
        return false;
    }
    if (mIOCount > mNormalIOCount)
    {
        return true;
    }
    mIOCount++;
    return false;
}

void ExceptionTrigger::Init(size_t normalIOCount)
{
    ScopedLock lock(mLock);
    mNormalIOCount = normalIOCount;
    mIOCount = 0;
    mIsPause = false;
}

size_t  ExceptionTrigger::GetIOCount()
{
    ScopedLock lock(mLock);
    return mIOCount;
}

void  ExceptionTrigger::Pause()
{
    ScopedLock lock(mLock);
    mIsPause = true;
}

void  ExceptionTrigger::Resume()
{
    ScopedLock lock(mLock);
    mIsPause = false;
}

void ExceptionTrigger::InitTrigger(size_t normalIOCount)
{
    ExceptionTrigger* trigger = Singleton<ExceptionTrigger>::GetInstance();
    trigger->Init(normalIOCount);
}

bool ExceptionTrigger::CanTriggerException()
{
    ExceptionTrigger* trigger = Singleton<ExceptionTrigger>::GetInstance();
    return trigger->TriggerException();
}

size_t ExceptionTrigger::GetTriggerIOCount()
{
    return Singleton<ExceptionTrigger>::GetInstance()->GetIOCount();
}

void ExceptionTrigger::PauseTrigger()
{
    Singleton<ExceptionTrigger>::GetInstance()->Pause();
}

void ExceptionTrigger::ResumeTrigger()
{
    Singleton<ExceptionTrigger>::GetInstance()->Resume();
}

IE_NAMESPACE_END(storage);

