#include "indexlib/util/counter/counter_creator.h"
#include "indexlib/util/counter/counter.h"
#include "indexlib/util/counter/multi_counter.h"
#include "indexlib/util/counter/accumulative_counter.h"
#include "indexlib/util/counter/state_counter.h"

using namespace std;

IE_NAMESPACE_USE(util);
IE_NAMESPACE_BEGIN(util);
IE_LOG_SETUP(util, CounterCreator);

CounterCreator::CounterCreator() 
{
}

CounterCreator::~CounterCreator() 
{
}

CounterBasePtr CounterCreator::CreateCounter(const string& path,
        CounterBase::CounterType type)
{
    CounterBasePtr counter;
    switch (type)
    {
    case CounterBase::CT_DIRECTORY:
        counter.reset(new MultiCounter(path));
        break;
    case CounterBase::CT_ACCUMULATIVE:
        counter.reset(new AccumulativeCounter(path));
        break;
    case CounterBase::CT_STATE:
        counter.reset(new StateCounter(path));
        break;
    default:
        assert(false);
    }
    return counter;
}



IE_NAMESPACE_END(util);

