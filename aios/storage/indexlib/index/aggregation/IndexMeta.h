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

#include "autil/Log.h"
#include "autil/legacy/jsonizable.h"
#include "indexlib/index/aggregation/ValueStat.h"

namespace indexlibv2::index {

struct IndexMeta final : public autil::legacy::Jsonizable {
    size_t keyCount = 0;
    size_t totalValueCount = 0;
    size_t totalValueCountAfterUnique = 0;
    size_t maxValueCountOfAllKeys = 0;
    size_t maxValueCountOfAllKeysAfterUnique = 0;
    size_t maxValueBytesOfAllKeys = 0;
    size_t maxValueBytesOfAllKeysAfterUnique = 0;
    size_t accmulatedValueBytes = 0;
    size_t accmulatedValueBytesAfterUnique = 0;

    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;

    void Update(const ValueStat& stat)
    {
        keyCount += 1;
        totalValueCount += stat.valueCount;
        totalValueCountAfterUnique += stat.valueCountAfterUnique;
        maxValueCountOfAllKeys = std::max(maxValueCountOfAllKeys, stat.valueCount);
        maxValueBytesOfAllKeys = std::max(maxValueBytesOfAllKeys, stat.valueBytes);
        maxValueCountOfAllKeysAfterUnique = std::max(maxValueCountOfAllKeysAfterUnique, stat.valueCountAfterUnique);
        maxValueBytesOfAllKeysAfterUnique = std::max(maxValueBytesOfAllKeysAfterUnique, stat.valueBytesAfterUnique);
        accmulatedValueBytes += stat.valueBytes;
        accmulatedValueBytesAfterUnique += stat.valueBytesAfterUnique;
    }

    std::string ToString() const;
    bool FromString(const std::string& jsonStr);

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::index
