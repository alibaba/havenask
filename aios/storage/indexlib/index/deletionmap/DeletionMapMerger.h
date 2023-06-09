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
#include "indexlib/index/IIndexMerger.h"
namespace indexlibv2::index {
class DeletionMapDiskIndexer;
class DeletionMapMerger : public index::IIndexMerger
{
public:
    DeletionMapMerger();
    ~DeletionMapMerger();

    Status Init(const std::shared_ptr<config::IIndexConfig>& indexConfig,
                const std::map<std::string, std::any>& params) override;
    Status Merge(const SegmentMergeInfos& segMergeInfos,
                 const std::shared_ptr<framework::IndexTaskResourceManager>& taskResourceManager) override;
    void CollectAllDeletionMap(
        const std::vector<std::pair<segmentid_t, std::shared_ptr<DeletionMapDiskIndexer>>>& patchedAllIndexers);

private:
    std::vector<std::pair<segmentid_t, std::shared_ptr<DeletionMapDiskIndexer>>> _allIndexers;
    bool _hasCollected;
    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::index
