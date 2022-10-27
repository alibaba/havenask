#ifndef __INDEXLIB_STATISTIC_H
#define __INDEXLIB_STATISTIC_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/util/thread_local.h"
#include <atomic>

IE_NAMESPACE_BEGIN(util);

class Statistic
{
public:
    struct StatData
    {
        uint64_t sum = 0;
        uint64_t num = 0;
        double avrage = 0.0;
        StatData() = default;
    };

private:
    struct ThreadStat
    {
        std::uint_fast64_t num;
        std::uint_fast64_t sum;

        // During teardown, value will be summed into *merged_sum.
        std::atomic_uint_fast64_t* mergedNum;
        std::atomic_uint_fast64_t* mergedSum;

        ThreadStat(uint64_t _num, uint64_t _sum, std::atomic_uint_fast64_t* _mergedNum,
            std::atomic_uint_fast64_t* _mergedSum)
            : num(_num)
            , sum(_sum)
            , mergedNum(_mergedNum)
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

public:
    Statistic()
        : mThreadStat(new ThreadLocalPtr(&MergeThreadStat))
    {}

    Statistic(const Statistic&) = delete;
    Statistic& operator=(const Statistic&) = delete;

public:
    void Record(uint64_t value)
    {
        auto threadStatPtr = GetThreadStat();
        threadStatPtr->sum += value;
        threadStatPtr->num += 1;
    }
    StatData GetStatData()
    {
        StatData data;
        mThreadStat->Fold(
            [](void* entryPtr, void* res) {
                auto globalData = static_cast<StatData*>(res);
                auto threadData = static_cast<ThreadStat*>(entryPtr);
                globalData->num += threadData->num;
                globalData->sum += threadData->sum;
            },
            &data);
        data.num += mMergedNum.load(std::memory_order_relaxed);
        data.sum += mMergedSum.load(std::memory_order_relaxed);
        if (data.num == 0)
        {
            data.avrage = 0;
        }
        else{
            data.avrage = static_cast<double>(data.sum) / data.num;
        }
        return data;
    }

public:
    static void MergeThreadStat(void* ptr)
    {
        auto threadStat = static_cast<ThreadStat*>(ptr);
        *(threadStat->mergedNum) += threadStat->num;
        *(threadStat->mergedSum) += threadStat->sum;
        delete threadStat;
    }

    ThreadStat* GetThreadStat()
    {
        auto threadStatPtr = static_cast<ThreadStat*>(mThreadStat->Get());
        if (threadStatPtr == nullptr)
        {
            threadStatPtr = new ThreadStat(0, 0, &mMergedNum, &mMergedSum);
            mThreadStat->Reset(threadStatPtr);
        }
        return threadStatPtr;
    }    
private:
    // Holds thread-specific pointer to ThreadStat
    std::unique_ptr<util::ThreadLocalPtr> mThreadStat;

    std::atomic_uint_fast64_t mMergedNum { 0 };
    std::atomic_uint_fast64_t mMergedSum { 0 };
    // TODO: min, max, histogram
};

IE_NAMESPACE_END(util);

#endif //__INDEXLIB_STATISTIC_H
