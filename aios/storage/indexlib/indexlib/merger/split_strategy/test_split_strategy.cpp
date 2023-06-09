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
#include "indexlib/merger/split_strategy/test_split_strategy.h"

using namespace std;

namespace indexlib { namespace merger {
IE_LOG_SETUP(merger, TestSplitStrategy);

using namespace indexlib::config;
using namespace indexlib::index;
using namespace indexlib::index_base;
using namespace indexlib::util;

const std::string TestSplitStrategy::STRATEGY_NAME = "test";

bool TestSplitStrategy::Init(const util::KeyValueMap& parameters)
{
    std::string segmentCountStr = GetValueFromKeyValueMap(parameters, "segment_count", "1");
    if (!(autil::StringUtil::fromString(segmentCountStr, mSegmentCount))) {
        IE_LOG(ERROR, "convert segment_count faile: [%s]", segmentCountStr.c_str());
        return false;
    }
    std::string useInvalidDocStr = GetValueFromKeyValueMap(parameters, "use_invalid_doc", "false");
    if (useInvalidDocStr == "true") {
        mUseInvaildDoc = true;
    }

    return true;
}
}} // namespace indexlib::merger
