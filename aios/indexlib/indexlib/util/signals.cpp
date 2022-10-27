#include "indexlib/util/tc_delegation.h"
#include "indexlib/util/env_util.h"
#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"
#include <signal.h>
#include <iostream>

IE_NAMESPACE_BEGIN(util);


static void handleSignal(int sig) {
    if (sig == SIGRTMIN+12)
    {
        auto inst = TcDelegation::Inst();
        if (inst)
        {
            inst->HeapProfilerStart("indexlib_heap_profile");
            std::cerr << "StartHeapProfile" << std::endl;
        }
    }
    else if (sig == SIGRTMIN+13)
    {
        auto inst = TcDelegation::Inst();
        std::cerr << "Stop" << std::endl;        
        if (inst)
        {
            inst->HeapProfilerDump("EndProfiler");
            inst->HeapProfilerStop();
            std::cerr << "StopHeapProfile" << std::endl;
        }
    }
    else if (sig == SIGRTMIN+14)
    {
        auto inst = TcDelegation::Inst();
        std::cerr << "[start] Manually call ReleaseFreeMemory by signal" << std::endl;
        if (inst)
        {
            inst->ReleaseFreeMemory();
            std::cerr << "[finish] Manually call ReleaseFreeMemory by signal" << std::endl;
        }
    }
    else if (sig == SIGRTMIN+15)
    {
        auto inst = TcDelegation::Inst();
        std::cerr << "[start] DumpMallocDetailStatus by signal" << std::endl;
        if (inst)
        {
            inst->DumpMallocDetailStatus();
            std::cerr << "[finish] DumpMallocDetailStatus by signal" << std::endl;
        }
    }
}

static bool enableSignal = []() -> bool {
    bool useSignals = IE_NAMESPACE(util)::EnvUtil::GetEnv("indexlib_enable_signal", false);
    if (useSignals)
    {
        signal(SIGRTMIN+12, handleSignal);
        signal(SIGRTMIN+13, handleSignal);
        signal(SIGRTMIN+14, handleSignal);
        signal(SIGRTMIN+15, handleSignal);
        return true;
    }
    return false;
}();

IE_NAMESPACE_END(util);
