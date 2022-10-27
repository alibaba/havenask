#include "indexlib/util/tc_delegation.h"
#include "indexlib/util/find_dl.h"
#include "indexlib/util/env_util.h"

IE_NAMESPACE_BEGIN(util);
IE_LOG_SETUP(util, TcDelegation);

TcDelegation::TcDelegation()
    : mHandle(nullptr)
    , mMallocExtension_ReleaseFreeMemory(nullptr)
    , mMallocExtension_GetStats(nullptr)
    , mHeapProfilerStart(nullptr)
    , mHeapProfilerStop(nullptr)
    , mHeapProfilerDump(nullptr)
{
    auto dlHandle = util::FindDl("libtcmalloc");
    if (!dlHandle)
    {
        dlHandle = util::FindDl("libtcmalloc_minimal");
    }

#define LOAD_FUNC(fn)                                                                             \
    m##fn = reinterpret_cast<fn##FP>(::dlsym(dlHandle, #fn));                                     \
    if (m##fn)                                                                                    \
    {                                                                                             \
        std::cerr << "indexlib::util::TcDelegation: " #fn " loaded" << std::endl;                 \
    }

    if (dlHandle)
    {
        LOAD_FUNC(MallocExtension_ReleaseFreeMemory);
        LOAD_FUNC(MallocExtension_GetStats);
        LOAD_FUNC(HeapProfilerStart);
        LOAD_FUNC(HeapProfilerStop);
        LOAD_FUNC(HeapProfilerDump);

        mHandle = dlHandle;
        std::cerr << "indexlib::util::TcDelegation: tcmalloc loaded" << std::endl;
    }
    else
    {
        std::cerr << "indexlib::util::TcDelegation: tcmalloc not loaded" << std::endl;
    }

#undef LOAD_FUNC
}

TcDelegation::~TcDelegation()
{
    if (mHandle)
    {
        ::dlclose(mHandle);
        std::cerr << "indexlib::util::TcDelegation: close tcmalloc" << std::endl;
    }
}

void TcDelegation::ReleaseFreeMemory()
{
    if (mMallocExtension_ReleaseFreeMemory)
    {
        mMallocExtension_ReleaseFreeMemory();
        IE_LOG(INFO, "ReleaseFreeMemory");
    }
}
void TcDelegation::DumpMallocDetailStatus()
{
    if (mMallocExtension_GetStats)
    {
        constexpr size_t kBufferLength = 1 << 14;
        char buffer[kBufferLength];
        mMallocExtension_GetStats(buffer, kBufferLength);
        IE_LOG(INFO, "DumpMallocDetailStatus result [%s]", buffer);
    }
}
void TcDelegation::HeapProfilerStart(const char* prefix)
{
    if (mHeapProfilerStart)
    {
        mHeapProfilerStart(prefix);
        IE_LOG(INFO, "HeapProfilerStart to [%s]", prefix);
    }
}
void TcDelegation::HeapProfilerStop()
{
    if (mHeapProfilerStop)
    {
        mHeapProfilerStop();
        IE_LOG(INFO, "HeapProfilerStop");
    }
}
void TcDelegation::HeapProfilerDump(const char* reason)
{
    if (mHeapProfilerDump)
    {
        mHeapProfilerDump(reason);
        IE_LOG(INFO, "HeapProfilerDump for [%s]", reason);
    }
}


TcDelegation* TcDelegation::Inst()
{
    static TcDelegation tc;
    return &tc;
}

IE_NAMESPACE_END(util);
