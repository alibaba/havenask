#ifndef __INDEXLIB_ACCUMULATIVE_COUNTER_H
#define __INDEXLIB_ACCUMULATIVE_COUNTER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/util/counter/counter.h"
#include "indexlib/util/thread_local.h"

IE_NAMESPACE_BEGIN(util);

class AccumulativeCounter : public Counter
{
public:
    AccumulativeCounter(const std::string& path = "");
    ~AccumulativeCounter();
private:
    AccumulativeCounter(const AccumulativeCounter&);
    AccumulativeCounter& operator = (const AccumulativeCounter&);
public:
    void Reset();
    void Increase(int64_t value);
    void FromJson(const autil::legacy::Any& any, FromJsonType fromJsonType) override;
    int64_t Get() const override final;
    int64_t GetLocal() const;

private:
    struct ThreadCounter
    {
        int64_t value;
        // During teardown, value will be summed into *merged_sum.
        std::atomic_int_fast64_t* mergedSum;

        ThreadCounter(int64_t _value,
                      std::atomic_int_fast64_t* _mergedSum)
            : value(_value)
            , mergedSum(_mergedSum)
        {}

        // use align allocation to avoid false sharing problem.
        // use 128 bytes(x2 L1 cache line size) to avoid false sharing trigged by spacial prefecher
        // see section 2.3.5.4 of 'Intel 64 and IA-32 Architectures Optimization Reference Manual'
        // (Order number : 248966-033)
        void* operator new (size_t size)
        {
            void* p;
            int ret = posix_memalign(&p, size_t(128), size);
            
            if (ret != 0)
            {
                throw std::bad_alloc();
            }
            return p;
        }

        void operator delete (void *p)
        {
            free(p);
        }
    };
    
private:
    static void MergeThreadValue(void* ptr)
    {
        auto threadCounter = static_cast<ThreadCounter*>(ptr);
        *(threadCounter->mergedSum) += threadCounter->value;
        delete threadCounter;
    }

    ThreadCounter* GetThreadCounter()
    {
        auto threadCounterPtr = static_cast<ThreadCounter*>(mThreadValue->Get());
        if (threadCounterPtr == nullptr)
        {
            threadCounterPtr = new ThreadCounter(0, &mMergedSum);
            mThreadValue->Reset(threadCounterPtr);
        }
        return threadCounterPtr;
    }    
private:
    // Holds thread-specific pointer to ThreadCounter
    std::unique_ptr<util::ThreadLocalPtr> mThreadValue;
private:
    IE_LOG_DECLARE(); 
};

DEFINE_SHARED_PTR(AccumulativeCounter);

/////////////////////////////////////////////////////////
inline void AccumulativeCounter::Reset()
{
    auto threadCounterPtr = GetThreadCounter();
    assert(threadCounterPtr);
    threadCounterPtr->value = 0;
}

inline void AccumulativeCounter::Increase(int64_t value)
{
    auto threadCounterPtr = GetThreadCounter();
    assert(threadCounterPtr);
    threadCounterPtr->value += value;
}

inline int64_t AccumulativeCounter::GetLocal() const
{
    auto threadCounterPtr = static_cast<ThreadCounter*>(mThreadValue->Get());
    if (threadCounterPtr == nullptr)
    {
        return 0;
    }
    return threadCounterPtr->value;
}

inline int64_t AccumulativeCounter::Get() const
{
    int64_t threadLocalSum = 0;
    mThreadValue->Fold(
            [](void* entryPtr, void* res) {
                auto* sumPtr = static_cast<int64_t*>(res);
                *sumPtr += *static_cast<int64_t*>(entryPtr);
            },
            &threadLocalSum);
    return threadLocalSum + mMergedSum.load(std::memory_order_relaxed);
}

IE_NAMESPACE_END(util);

#endif //__INDEXLIB_ACCUMULATIVE_COUNTER_H
