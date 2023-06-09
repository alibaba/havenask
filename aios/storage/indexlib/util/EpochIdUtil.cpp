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
#include "indexlib/util/EpochIdUtil.h"

#include "autil/Lock.h"
#include "autil/StringUtil.h"
using namespace std;

namespace indexlib::util {
AUTIL_LOG_SETUP(indexlib.util, EpochIdUtil);

bool EpochIdUtil::CompareGE(const std::string& lValue, const std::string& rValue)
{
    int64_t leftValue = autil::StringUtil::strToInt64WithDefault(lValue.c_str(), 0);
    int64_t rightValue = autil::StringUtil::strToInt64WithDefault(rValue.c_str(), 0);
    return leftValue >= rightValue;
}

std::string EpochIdUtil::GenerateEpochId(int64_t epochId) { return std::to_string(epochId); }

std::string EpochIdUtil::TEST_GenerateEpochId()
{
    static size_t TEST_CurrentEpoch;
    static autil::ThreadMutex TEST_CurrentEpochLock;
    size_t value = autil::TimeUtility::currentTimeInMicroSeconds();

    autil::ScopedLock lock(TEST_CurrentEpochLock);
    value = std::max(value, TEST_CurrentEpoch + 1);
    TEST_CurrentEpoch = value;
    return std::to_string(value);
}

} // namespace indexlib::util
