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
#include "indexlib/framework/Segment.h"
#include "indexlib/framework/TabletDataInfo.h"
#include "indexlib/index/IIndexReader.h"
#include "indexlib/index/deletionmap/DeletionMapDiskIndexer.h"
#include "indexlib/index/deletionmap/DeletionMapMemIndexer.h"
#include "indexlib/index/deletionmap/DeletionMapResource.h"
namespace indexlibv2 { namespace framework {
class TabletDataInfo;
}} // namespace indexlibv2::framework

namespace indexlibv2::index {
class IIndexer;
class DeletionMapMetrics;

class DeletionMapIndexReader : public IIndexReader
{
public:
    static std::pair<Status, std::unique_ptr<DeletionMapIndexReader>> Create(const framework::TabletData* tabletData);

public:
    DeletionMapIndexReader();
    ~DeletionMapIndexReader();

    Status Open(const std::shared_ptr<config::IIndexConfig>& indexConfig,
                const framework::TabletData* tabletData) override;
    bool IsDeleted(docid_t docid) const;
    uint32_t GetDeletedDocCount() const;
    uint32_t GetSegmentDeletedDocCount(segmentid_t segmentId) const;
    void RegisterMetrics(const std::shared_ptr<DeletionMapMetrics>& metrics);

private:
    Status DoOpen(
        const std::vector<std::tuple<std::shared_ptr<IIndexer>, framework::Segment::SegmentStatus, size_t>>& indexers);

private:
    std::shared_ptr<DeletionMapMetrics> _metrics;
    std::shared_ptr<const framework::TabletDataInfo> _tabletDataInfo;
    std::vector<std::shared_ptr<DeletionMapDiskIndexer>> _builtIndexers;
    std::vector<std::shared_ptr<DeletionMapMemIndexer>> _dumpingIndexers;
    std::shared_ptr<DeletionMapMemIndexer> _buildingIndexer;
    std::vector<docid_t> _segmentBaseDocids;
    std::vector<std::shared_ptr<DeletionMapResource>> _deletionMapResources;

private:
    friend class DeletionMapIndexReaderTest;
    friend class DeletionMapPatcherTest;
    AUTIL_LOG_DECLARE();
};

inline bool DeletionMapIndexReader::IsDeleted(docid_t docid) const
{
    if (docid >= _tabletDataInfo->GetDocCount()) {
        return true;
    }
    size_t idx =
        std::upper_bound(_segmentBaseDocids.begin(), _segmentBaseDocids.end(), docid) - _segmentBaseDocids.begin() - 1;
    docid_t inSegmentDocid = docid - _segmentBaseDocids[idx];
    bool isDeleted = false;
    if (idx < _builtIndexers.size()) {
        isDeleted = _builtIndexers[idx]->IsDeleted(inSegmentDocid);
    } else if (idx < _builtIndexers.size() + _dumpingIndexers.size()) {
        isDeleted = _dumpingIndexers[idx - _builtIndexers.size()]->IsDeleted(inSegmentDocid);
    } else if (_buildingIndexer) {
        isDeleted = _buildingIndexer->IsDeleted(inSegmentDocid);
    } else {
        isDeleted = true;
    }
    if (!isDeleted) {
        assert(idx < _deletionMapResources.size());
        isDeleted |= _deletionMapResources[idx]->IsDeleted(inSegmentDocid);
    }
    return isDeleted;
}

} // namespace indexlibv2::index
