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
#include "indexlib/index/inverted_index/InvertedIndexMerger.h"
#include "indexlib/index/inverted_index/builtin_index/pack/OnDiskPackIndexIterator.h"

namespace indexlib::index {
class RangeLevelIndexMerger;

class RangeIndexMerger : public InvertedIndexMerger
{
public:
    using OnDiskRangeIndexIterator = OnDiskPackIndexIterator;

    RangeIndexMerger();
    ~RangeIndexMerger();

    Status Init(const std::shared_ptr<indexlibv2::config::IIndexConfig>& indexConfig,
                const std::map<std::string, std::any>& params) override;
    std::string GetIdentifier() const override;

protected:
    Status
    DoMerge(const SegmentMergeInfos& segMergeInfos,
            const std::shared_ptr<indexlibv2::framework::IndexTaskResourceManager>& taskResourceManager) override;
    void PrepareIndexOutputSegmentResource(
        const std::vector<SourceSegment>& srcSegments,
        const std::vector<std::shared_ptr<indexlibv2::framework::SegmentMeta>>& targetSegments) override;
    std::shared_ptr<OnDiskIndexIteratorCreator> CreateOnDiskIndexIteratorCreator() override;

private:
    Status MergeRangeInfo(const SegmentMergeInfos& segMergeInfos);

private:
    std::shared_ptr<RangeLevelIndexMerger> _bottomLevelIndexMerger;
    std::shared_ptr<RangeLevelIndexMerger> _highLevelIndexMerger;
    AUTIL_LOG_DECLARE();
};

} // namespace indexlib::index
