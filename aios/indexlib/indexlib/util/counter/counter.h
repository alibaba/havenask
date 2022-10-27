#ifndef __INDEXLIB_COUNTER_H
#define __INDEXLIB_COUNTER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/util/counter/counter_base.h"
#include "indexlib/util/thread_local.h"

IE_NAMESPACE_BEGIN(util);

class Counter : public CounterBase
{
public:
    Counter(const std::string& path, CounterType type);
    virtual ~Counter();

public:
    virtual int64_t Get() const = 0;
public:
    autil::legacy::Any ToJson() const override;
    void FromJson(const autil::legacy::Any& any, FromJsonType fromJsonType) override;
    
protected:
    // AccumulativeCounter: Sum of thread-specific values  will be  
    // added to the mMergedSum when thread terminates or in ThreadLocalPtr's destructor.
    // StatusCounter: mMergedSum is the final value since there are no thread-specific values
    std::atomic_int_fast64_t mMergedSum;
private:
    IE_LOG_DECLARE(); 
};

DEFINE_SHARED_PTR(Counter);

IE_NAMESPACE_END(util);
#endif //__INDEXLIB_COUNTER_H
