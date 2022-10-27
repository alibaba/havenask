#ifndef __INDEXLIB_COUNTER_CREATOR_H
#define __INDEXLIB_COUNTER_CREATOR_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/util/counter/counter_base.h"

IE_NAMESPACE_BEGIN(util);

class CounterCreator
{
public:
    CounterCreator();
    ~CounterCreator();
public:
    static CounterBasePtr CreateCounter(const std::string& path,
            CounterBase::CounterType type);

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(CounterCreator);

IE_NAMESPACE_END(util);

#endif //__INDEXLIB_COUNTER_CREATOR_H
