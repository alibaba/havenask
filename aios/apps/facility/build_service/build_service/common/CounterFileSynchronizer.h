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

#include <functional>
#include <string>
#include <unistd.h>

#include "alog/Logger.h"
#include "build_service/common/CounterSynchronizer.h"
#include "build_service/common_define.h"
#include "build_service/util/Log.h"
#include "indexlib/util/counter/CounterMap.h"

namespace build_service { namespace common {

class CounterFileSynchronizer : public CounterSynchronizer
{
public:
    CounterFileSynchronizer();
    ~CounterFileSynchronizer();

private:
    CounterFileSynchronizer(const CounterFileSynchronizer&);
    CounterFileSynchronizer& operator=(const CounterFileSynchronizer&);

public:
    static indexlib::util::CounterMapPtr loadCounterMap(const std::string& counterFilePath, bool& fileExist);

public:
    bool init(const indexlib::util::CounterMapPtr& counterMap, const std::string& counterFilePath);

public:
    bool sync() const override;

private:
    static bool doWithRetry(std::function<bool()> predicate, int retryLimit, int intervalInSecond)
    {
        for (int i = 0; i < retryLimit; ++i) {
            if (predicate()) {
                return true;
            } else {
                BS_LOG(WARN, "predicate return false, retry for %d times", i + 1);
            }
            sleep(intervalInSecond);
        }
        return false;
    }

private:
    std::string _counterFilePath;

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(CounterFileSynchronizer);

}} // namespace build_service::common
