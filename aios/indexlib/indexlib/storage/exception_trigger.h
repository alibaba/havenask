#ifndef __INDEXLIB_EXCEPTION_TRIGGER_H
#define __INDEXLIB_EXCEPTION_TRIGGER_H

#include <tr1/memory>
#include <autil/Lock.h>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"

IE_NAMESPACE_BEGIN(storage);

class ExceptionTrigger
{
public:
    ExceptionTrigger();
    ~ExceptionTrigger();

public:
    static void InitTrigger(size_t normalIOCount);
    static bool CanTriggerException();
    static size_t GetTriggerIOCount();
    static void PauseTrigger();
    static void ResumeTrigger();
    
    bool TriggerException();
    void Init(size_t normalIOCount);
    size_t GetIOCount();
    void Pause();
    void Resume();

public:
    static const size_t NO_EXCEPTION_COUNT;
private:
    mutable autil::ThreadMutex mLock;
    size_t mIOCount;
    size_t mNormalIOCount;
    bool mIsPause;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(ExceptionTrigger);

IE_NAMESPACE_END(storage);

#endif //__INDEXLIB_EXCEPTION_TRIGGER_H
