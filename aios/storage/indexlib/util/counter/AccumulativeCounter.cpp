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
#include "indexlib/util/counter/AccumulativeCounter.h"

#include "autil/legacy/json.h"
#include "autil/legacy/jsonizable.h"
#include "indexlib/util/Exception.h"

using namespace std;
using namespace autil::legacy;

namespace indexlib { namespace util {
AUTIL_LOG_SETUP(indexlib.util, AccumulativeCounter);

AccumulativeCounter::AccumulativeCounter(const string& path)
    : Counter(path, CT_ACCUMULATIVE)
    , _threadValue(new autil::ThreadLocalPtr(&MergeThreadValue))
{
}

AccumulativeCounter::~AccumulativeCounter() {}

void AccumulativeCounter::FromJson(const Any& any, FromJsonType fromJsonType)
{
    json::JsonMap jsonMap = AnyCast<json::JsonMap>(any);
    auto iter = jsonMap.find("value");
    if (iter == jsonMap.end()) {
        INDEXLIB_FATAL_ERROR(InconsistentState, "no value found in counter[%s]", _path.c_str());
    }

    int64_t value = json::JsonNumberCast<int64_t>(iter->second);
    if (fromJsonType == FJT_OVERWRITE) {
        _mergedSum = value;
    } else if (fromJsonType == FJT_MERGE) {
        Increase(value);
    } else {
        INDEXLIB_FATAL_ERROR(InconsistentState, "invalid fromJsonType[%u] for counter[%s]", fromJsonType,
                             _path.c_str());
    }
}
}} // namespace indexlib::util
