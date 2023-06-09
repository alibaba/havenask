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
#include "build_service/reader/EmptyFilePattern.h"

#include "build_service/util/RangeUtil.h"

using namespace std;

namespace build_service { namespace reader {

BS_LOG_SETUP(reader, EmptyFilePattern);

EmptyFilePattern::EmptyFilePattern() : FilePatternBase("") {}

EmptyFilePattern::~EmptyFilePattern() {}

CollectResults EmptyFilePattern::calculateHashIds(const CollectResults& results) const
{
    CollectResults ret;
    ret.reserve(results.size());
    uint32_t fileCount = static_cast<uint32_t>(results.size());

    for (size_t i = 0; i < results.size(); ++i) {
        uint32_t fileId = static_cast<uint32_t>(i);
        uint16_t rangeId = util::RangeUtil::getRangeId(fileId, fileCount);

        CollectResult now = results[i];
        now._rangeId = rangeId;
        ret.push_back(now);
    }

    return ret;
}

}} // namespace build_service::reader
