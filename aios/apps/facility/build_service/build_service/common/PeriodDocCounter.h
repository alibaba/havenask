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

#include <cstdint>
#include <functional>
#include <map>
#include <stddef.h>
#include <string>
#include <utility>

#include "alog/Logger.h"
#include "autil/EnvUtil.h"
#include "autil/Lock.h"
#include "autil/LoopThread.h"
#include "build_service/common_define.h"
#include "build_service/util/Log.h"
#include "indexlib/base/FieldType.h"
#include "indexlib/util/Singleton.h"

namespace build_service { namespace common {

class PeriodDocCounterBase
{
public:
    virtual void flush() = 0;
    virtual ~PeriodDocCounterBase() {}
};

enum PeriodDocCounterType { Reader = 0, Processor = 1, Builder = 2 };

class PeriodDocCounterHelper
{
public:
    static PeriodDocCounterBase* create(FieldType ft);
    static void count(PeriodDocCounterType type, FieldType ft, const void* data, size_t n,
                      PeriodDocCounterBase* counter);
    static void count(PeriodDocCounterType type, const std::string& data, PeriodDocCounterBase* counter);
    static void shutdown();
};

template <typename T>
class PeriodDocCounter : public indexlib::util::Singleton<PeriodDocCounter<T>>, public PeriodDocCounterBase
{
public:
    PeriodDocCounter() { reset(); }
    virtual ~PeriodDocCounter() {}

public:
    void shutdown()
    {
        _writeThread.reset();
        flush();
        BS_LOG_FLUSH();
    }

private:
    void reset();

public:
    inline void count(PeriodDocCounterType type, const T& key)
    {
        autil::ScopedLock lock(_lock);
        int32_t typeId = (int32_t)type;
        auto iter = _counter[typeId].find(key);
        if (iter == _counter[typeId].end()) {
            _counter[typeId].insert(std::make_pair(key, 1));
        } else {
            iter->second++;
        }
    }
    void flush() override;

private:
    inline void writeLog(int32_t type, const T& key, uint64_t value);

private:
    static const int32_t TYPE_COUNT = 4;

private:
    autil::ThreadMutex _lock;
    autil::LoopThreadPtr _writeThread;
    int32_t _period;
    std::map<T, uint64_t> _counter[TYPE_COUNT];

private:
    BS_LOG_DECLARE();
};

BS_LOG_SETUP_TEMPLATE(common, PeriodDocCounter, T);

// impl
template <typename T>
inline void PeriodDocCounter<T>::writeLog(int32_t type, const T& key, uint64_t value)
{
    BS_LOG(INFO, "%d,%d,%lu", type, key, value);
}

template <>
inline void PeriodDocCounter<std::string>::writeLog(int32_t type, const std::string& key, uint64_t value)
{
    BS_LOG(INFO, "%d,%s,%lu", type, key.c_str(), value);
}
template <>
inline void PeriodDocCounter<int64_t>::writeLog(int32_t type, const int64_t& key, uint64_t value)
{
    BS_LOG(INFO, "%d,%ld,%lu", type, key, value);
}
template <>
inline void PeriodDocCounter<uint64_t>::writeLog(int32_t type, const uint64_t& key, uint64_t value)
{
    BS_LOG(INFO, "%d,%lu,%lu", type, key, value);
}
template <>
inline void PeriodDocCounter<int32_t>::writeLog(int32_t type, const int32_t& key, uint64_t value)
{
    BS_LOG(INFO, "%d,%d,%lu", type, key, value);
}
template <>
inline void PeriodDocCounter<uint32_t>::writeLog(int32_t type, const uint32_t& key, uint64_t value)
{
    BS_LOG(INFO, "%d,%u,%lu", type, key, value);
}
template <>
inline void PeriodDocCounter<float>::writeLog(int32_t type, const float& key, uint64_t value)
{
    BS_LOG(INFO, "%d,%f,%lu", type, key, value);
}
template <>
inline void PeriodDocCounter<double>::writeLog(int32_t type, const double& key, uint64_t value)
{
    BS_LOG(INFO, "%d,%f,%lu", type, key, value);
}

template <typename T>
void PeriodDocCounter<T>::reset()
{
    flush();
    BS_LOG_FLUSH();
    int32_t interval = autil::EnvUtil::getEnv(BS_ENV_DOC_TRACE_INTERVAL, int32_t(5));
    _writeThread =
        autil::LoopThread::createLoopThread(std::bind(&PeriodDocCounter<T>::flush, this), interval * 1000 * 1000);
}

template <typename T>
void PeriodDocCounter<T>::flush()
{
    autil::ScopedLock lock(_lock);
    for (int32_t i = 0; i < TYPE_COUNT; i++) {
        auto iter = _counter[i].begin();
        for (; iter != _counter[i].end(); iter++) {
            writeLog(i, iter->first, iter->second);
        }
        _counter[i].clear();
    }
}

typedef PeriodDocCounter<float> FloatPeriodDocCounter;
typedef PeriodDocCounter<double> DoublePeriodDocCounter;
typedef PeriodDocCounter<std::string> StringPeriodDocCounter;
typedef PeriodDocCounter<int8_t> Int8PeriodDocCounter;
typedef PeriodDocCounter<uint8_t> UInt8PeriodDocCounter;
typedef PeriodDocCounter<int16_t> Int16PeriodDocCounter;
typedef PeriodDocCounter<uint16_t> UInt16PeriodDocCounter;
typedef PeriodDocCounter<int32_t> Int32PeriodDocCounter;
typedef PeriodDocCounter<uint32_t> UInt32PeriodDocCounter;
typedef PeriodDocCounter<int64_t> Int64PeriodDocCounter;
typedef PeriodDocCounter<uint64_t> UInt64PeriodDocCounter;

}} // namespace build_service::common
