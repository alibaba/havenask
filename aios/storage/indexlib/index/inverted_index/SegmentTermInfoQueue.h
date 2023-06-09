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

#include <memory>

#include "autil/Log.h"
#include "indexlib/index/IIndexMerger.h"
#include "indexlib/index/common/patch/PatchFileInfos.h"
#include "indexlib/index/inverted_index/IndexIterator.h"
#include "indexlib/index/inverted_index/OnDiskIndexIteratorCreator.h"
#include "indexlib/index/inverted_index/SegmentTermInfo.h"
#include "indexlib/index/inverted_index/config/InvertedIndexConfig.h"

namespace indexlibv2::config {
class TabletSchema;
}
namespace indexlib::index {
class SingleFieldIndexPatchIterator;
class SegmentTermInfoQueue
{
public:
    SegmentTermInfoQueue(const std::shared_ptr<indexlibv2::config::InvertedIndexConfig>& indexConfig,
                         const std::shared_ptr<OnDiskIndexIteratorCreator>& onDiskIndexIterCreator);
    virtual ~SegmentTermInfoQueue();

    Status Init(const std::vector<indexlibv2::index::IIndexMerger::SourceSegment>& srcSegments,
                const indexlibv2::index::PatchInfos& patchInfos);
    Status Init(const std::shared_ptr<file_system::Directory>& indexDir,
                const std::shared_ptr<indexlibv2::index::PatchFileInfo>& patchFileInfo);
    inline bool Empty() const { return _segmentTermInfos.empty(); }

    const std::vector<SegmentTermInfo*>& CurrentTermInfos(index::DictKeyInfo& key,
                                                          SegmentTermInfo::TermIndexMode& termMode);

    void MoveToNextTerm();

    // for test
    SegmentTermInfo* Top() const { return _segmentTermInfos.top(); }
    size_t Size() const { return _segmentTermInfos.size(); }

private:
    Status AddOnDiskTermInfo(docid_t baseDocId, uint64_t docCount, segmentid_t segmentId,
                             const std::shared_ptr<indexlibv2::config::TabletSchema>& schema,
                             const std::shared_ptr<file_system::Directory>& segmentDir);
    void AddQueueItem(size_t baseDocid, segmentid_t& segmentId, const std::shared_ptr<IndexIterator>& indexIt,
                      const std::shared_ptr<SingleFieldIndexSegmentPatchIterator>& patchIter,
                      SegmentTermInfo::TermIndexMode mode);
    // virtual for UT
    virtual std::shared_ptr<IndexIterator>
    CreateOnDiskNormalIterator(const std::shared_ptr<file_system::Directory>& segmentDir,
                               const std::shared_ptr<file_system::Directory>& indexDir) const;

    std::shared_ptr<IndexIterator>
    CreateForDefaultValueIter(const std::shared_ptr<indexlibv2::config::TabletSchema>& schema, uint64_t docCount,
                              segmentid_t segmentId) const;

    std::shared_ptr<file_system::Directory>
    GetIndexDir(const std::shared_ptr<file_system::Directory>& segmentDir) const;
    virtual std::shared_ptr<IndexIterator>
    CreateOnDiskBitmapIterator(const std::shared_ptr<file_system::Directory>& inputIndexDir) const;

    virtual std::pair<Status, std::shared_ptr<SingleFieldIndexSegmentPatchIterator>>
    CreatePatchIterator(segmentid_t targetSegmentId) const;

private:
    using PriorityQueue =
        std::priority_queue<SegmentTermInfo*, std::vector<SegmentTermInfo*>, SegmentTermInfoComparator>;
    PriorityQueue _segmentTermInfos;
    std::vector<SegmentTermInfo*> _mergingSegmentTermInfos;
    std::map<segmentid_t, indexlibv2::index::PatchFileInfos> _patchInfos;
    std::shared_ptr<indexlibv2::config::InvertedIndexConfig> _indexConfig;
    std::shared_ptr<OnDiskIndexIteratorCreator> _onDiskIndexIterCreator;

    AUTIL_LOG_DECLARE();
};

} // namespace indexlib::index
