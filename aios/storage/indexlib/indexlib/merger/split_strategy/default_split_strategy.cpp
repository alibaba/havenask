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
#include "indexlib/merger/split_strategy/default_split_strategy.h"

#include <cstddef>
#include <vector>

#include "alog/Logger.h"
#include "autil/StringUtil.h"
#include "autil/legacy/any.h"
#include "autil/legacy/exception.h"
#include "indexlib/index/attribute/Constant.h"
#include "indexlib/index/normal/attribute/accessor/offline_attribute_segment_reader_container.h"
#include "indexlib/index_base/index_meta/segment_info.h"
#include "indexlib/index_base/index_meta/segment_temperature_meta.h"
#include "indexlib/util/ErrorLogCollector.h"

using namespace std;
using namespace autil;
using namespace autil::legacy::json;

using namespace indexlib::config;
using namespace indexlib::index;
using namespace indexlib::index_base;
using namespace indexlib::util;

namespace indexlib { namespace merger {
IE_LOG_SETUP(merger, DefaultSplitStrategy);
const std::string DefaultSplitStrategy::STRATEGY_NAME = "default";
DefaultSplitStrategy::DefaultSplitStrategy(SegmentDirectoryBasePtr segDir, IndexPartitionSchemaPtr schema,
                                           OfflineAttributeSegmentReaderContainerPtr attrReaders,
                                           const SegmentMergeInfos& segMergeInfos, const MergePlan& plan,
                                           std::map<segmentid_t, index::SegmentMetricsUpdaterPtr> hintDocInfos,
                                           const util::MetricProviderPtr& metrics)
    : SplitSegmentStrategy(segDir, schema, attrReaders, segMergeInfos, plan, hintDocInfos, metrics)
    , mTotalValidDocCount(0)
    , mSplitDocCount(0)
    , mCurrentDocCount(0)
    , mCurrentSegIndex(0)
{
    for (const auto& segMergeInfo : segMergeInfos) {
        auto segId = segMergeInfo.segmentId;
        if (plan.HasSegment(segId)) {
            mTotalValidDocCount += segMergeInfo.segmentInfo.docCount - segMergeInfo.deletedDocCount;
        }
    }
}

DefaultSplitStrategy::~DefaultSplitStrategy() {}

bool DefaultSplitStrategy::Init(const KeyValueMap& parameters)
{
    string splitNumStr = GetValueFromKeyValueMap(parameters, "split_num", "1");
    size_t splitNum = 0;
    if (!(StringUtil::fromString(splitNumStr, splitNum))) {
        IE_LOG(ERROR, "convert split_num faile: [%s]", splitNumStr.c_str());
        return false;
    }
    mSplitDocCount = mTotalValidDocCount / splitNum;
    return true;
}

segmentindex_t DefaultSplitStrategy::DoProcess(segmentid_t oldSegId, docid_t oldLocalId, int64_t& hintValue)
{
    ++mCurrentDocCount;
    if (mCurrentDocCount > mSplitDocCount) {
        mCurrentDocCount = 0;
        return ++mCurrentSegIndex;
    }
    return mCurrentSegIndex;
}
}} // namespace indexlib::merger
