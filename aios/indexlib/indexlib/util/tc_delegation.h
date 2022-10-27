#ifndef INDEXLIB_UTIL_TCDELEGETION_H
#define INDEXLIB_UTIL_TCDELEGETION_H
#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"
#include <autil/Log.h>

IE_NAMESPACE_BEGIN(util);

class TcDelegation {
private:
    using MallocExtension_ReleaseFreeMemoryFP = void (*)();
    using MallocExtension_GetStatsFP = void (*)(char *, int);
    using HeapProfilerStartFP = void (*)(const char*);
    using HeapProfilerStopFP = void (*)();
    using HeapProfilerDumpFP = void (*)(const char*);

private:
    TcDelegation();

public:
    ~TcDelegation();

    static TcDelegation* Inst();

public:
    void ReleaseFreeMemory();
    void DumpMallocDetailStatus();
    void HeapProfilerStart(const char* prefix);
    void HeapProfilerStop();
    void HeapProfilerDump(const char* reason);

private:
    void* mHandle;
    MallocExtension_ReleaseFreeMemoryFP mMallocExtension_ReleaseFreeMemory;
    MallocExtension_GetStatsFP mMallocExtension_GetStats;
    HeapProfilerStartFP mHeapProfilerStart;
    HeapProfilerStopFP mHeapProfilerStop;
    HeapProfilerDumpFP mHeapProfilerDump;

private:
    IE_LOG_DECLARE();
};

IE_NAMESPACE_END(util);

#endif
