#ifndef __INDEXLIB_STATE_COUNTER_H
#define __INDEXLIB_STATE_COUNTER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/util/counter/counter.h"

IE_NAMESPACE_BEGIN(util);

class StateCounter : public Counter
{
public:
    StateCounter(const std::string& path);
    ~StateCounter();
private:
    StateCounter(const StateCounter&);
    StateCounter& operator = (const StateCounter&);
public:
    void Set(int64_t value)
    { mMergedSum.store(value, std::memory_order_relaxed); }        

    int64_t Get() const override final
    { return mMergedSum.load(std::memory_order_relaxed); }
    
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(StateCounter);

IE_NAMESPACE_END(util);

#endif //__INDEXLIB_STATE_COUNTER_H
