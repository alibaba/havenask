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
#include "build_service/common/CounterFileSynchronizer.h"

#include <iosfwd>
#include <memory>

#include "build_service/util/ErrorLogCollector.h"
#include "fslib/util/FileUtil.h"
#include "indexlib/util/ErrorLogCollector.h"
#include "indexlib/util/Exception.h"

using namespace std;
using namespace build_service::util;
using namespace indexlib::util;

namespace build_service { namespace common {
BS_LOG_SETUP(common, CounterFileSynchronizer);

CounterFileSynchronizer::CounterFileSynchronizer() {}

CounterFileSynchronizer::~CounterFileSynchronizer() { stopSync(); }

CounterMapPtr CounterFileSynchronizer::loadCounterMap(const std::string& counterFilePath, bool& fileExist)
{
    fileExist = false;
    if (counterFilePath.empty()) {
        BS_LOG(ERROR, "invalid counter file path[%s]", counterFilePath.c_str());
        return CounterMapPtr();
    }

    auto fileExistPredicate = std::bind((bool (*)(const string&, bool&)) & fslib::util::FileUtil::isExist,
                                        counterFilePath, std::ref(fileExist));

    if (!doWithRetry(fileExistPredicate, 10, 5)) {
        BS_LOG(ERROR, "check file path[%s] existence failed", counterFilePath.c_str());
        fileExist = false;
        return CounterMapPtr();
    }

    CounterMapPtr counterMap(new CounterMap());
    if (!fileExist) {
        return counterMap;
    }

    string counterJsonStr;
    auto readFilePredicate = std::bind((bool (*)(const string&, string&)) & fslib::util::FileUtil::readFile,
                                       counterFilePath, std::ref(counterJsonStr));

    if (!doWithRetry(readFilePredicate, 10, 5)) {
        BS_LOG(ERROR, "read counterMap from [%s] failed.", counterFilePath.c_str());
        return CounterMapPtr();
    }

    try {
        counterMap->FromJsonString(counterJsonStr);
    } catch (const ExceptionBase& e) {
        BS_LOG(WARN, "deserialize counterMap json[%s] for [%s] failed", counterJsonStr.c_str(),
               counterFilePath.c_str());
        fileExist = false;
        return CounterMapPtr(new CounterMap());
    }

    return counterMap;
}

bool CounterFileSynchronizer::init(const CounterMapPtr& counterMap, const string& counterFilePath)
{
    if (!CounterSynchronizer::init(counterMap)) {
        return false;
    }
    _counterFilePath = counterFilePath;
    return true;
}

bool CounterFileSynchronizer::sync() const
{
    if (!_counterMap) {
        BS_LOG(ERROR, "counterMap is NULL");
        return false;
    }
    string counterJsonStr = _counterMap->ToJsonString();
    if (!fslib::util::FileUtil::writeFile(_counterFilePath, counterJsonStr)) {
        BS_LOG(ERROR, "serialize counterMap[%s] to file [%s] failed", counterJsonStr.c_str(), _counterFilePath.c_str());
        return false;
    }
    return true;
}

}} // namespace build_service::common
