#include "indexlib/util/counter/state_counter.h"

using namespace std;

IE_NAMESPACE_USE(util);
IE_NAMESPACE_BEGIN(util);
IE_LOG_SETUP(util, StateCounter);

StateCounter::StateCounter(const string& path)
    : Counter(path, CT_STATE)
{
}

StateCounter::~StateCounter() 
{
}

IE_NAMESPACE_END(util);

