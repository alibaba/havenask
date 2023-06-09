/*
 * Copyright 2014-present Alibaba Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#pragma once

#include <memory>

#include "autil/Log.h"
#include "autil/ThreadLocal.h"
#include "indexlib/util/counter/Counter.h"

namespace indexlib { namespace util {

class AccumulativeCounter : public Counter
{
public:
    AccumulativeCounter(const std::string& path = "");
    ~AccumulativeCounter();

private:
    AccumulativeCounter(const AccumulativeCounter&);
    AccumulativeCounter& operator=(const AccumulativeCounter&);

public:
    void Reset();
    void Increase(int64_t value);
    void FromJson(const autil::legacy::Any& any, FromJsonType fromJsonType) override;
    int64_t Get() const override final;
    int64_t GetLocal() const;

private:
    struct ThreadCounter {
        int64_t value;
        // During teardown, value will be summed into *merged_sum.
        std::atomic_int_fast64_t* mergedSum;

        ThreadCounter(int64_t _value, std::atomic_int_fast64_t* _mergedSum) : value(_value), mergedSum(_mergedSum) {}

        // use align allocation to avoid false sharing problem.
        // use 128 bytes(x2 L1 cache line size) to avoid false sharing trigged by spacial prefecher
        // see section 2.3.5.4 of 'Intel 64 and IA-32 Architectures Optimization Reference Manual'
        // (Order number : 248966-033)
        void* operator new(size_t size)
        {
            void* p;
            int ret = posix_memalign(&p, size_t(128), size);

            if (ret != 0) {
                throw std::bad_alloc();
            }
            return p;
        }

        void operator delete(void* p) { free(p); }
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
        auto threadCounterPtr = static_cast<ThreadCounter*>(_threadValue->Get());
        if (threadCounterPtr == nullptr) {
            threadCounterPtr = new ThreadCounter(0, &_mergedSum);
            _threadValue->Reset(threadCounterPtr);
        }
        return threadCounterPtr;
    }

private:
    // Holds thread-specific pointer to ThreadCounter
    std::unique_ptr<autil::ThreadLocalPtr> _threadValue;

private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<AccumulativeCounter> AccumulativeCounterPtr;
typedef std::unordered_map<std::string, AccumulativeCounterPtr> AccessCounterMap;

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
    auto threadCounterPtr = static_cast<ThreadCounter*>(_threadValue->Get());
    if (threadCounterPtr == nullptr) {
        return 0;
    }
    return threadCounterPtr->value;
}

inline int64_t AccumulativeCounter::Get() const
{
    int64_t threadLocalSum = 0;
    _threadValue->Fold(
        [](void* entryPtr, void* res) {
            auto* sumPtr = static_cast<int64_t*>(res);
            *sumPtr += *static_cast<int64_t*>(entryPtr);
        },
        &threadLocalSum);
    return threadLocalSum + _mergedSum.load(std::memory_order_relaxed);
}
}} // namespace indexlib::util
