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

#include "indexlib/common_define.h"
#include "indexlib/config/index_config.h"
#include "indexlib/index/inverted_index/IndexIterator.h"
#include "indexlib/index/inverted_index/SegmentTermInfo.h"
#include "indexlib/index/normal/inverted_index/accessor/on_disk_index_iterator_creator.h"
#include "indexlib/index/util/segment_directory_base.h"
#include "indexlib/index_base/index_meta/segment_merge_info.h"
#include "indexlib/indexlib.h"

DECLARE_REFERENCE_CLASS(index, SingleFieldIndexSegmentPatchIterator);

namespace indexlib { namespace index { namespace legacy {

class SegmentTermInfoQueue
{
public:
    SegmentTermInfoQueue(const config::IndexConfigPtr& indexConf,
                         const OnDiskIndexIteratorCreatorPtr& onDiskIndexIterCreator);
    virtual ~SegmentTermInfoQueue();

public:
    void Init(const SegmentDirectoryBasePtr& segDir, const index_base::SegmentMergeInfos& segMergeInfos);

    inline bool Empty() const { return mSegmentTermInfos.empty(); }
    const SegmentTermInfos& CurrentTermInfos(index::DictKeyInfo& key, SegmentTermInfo::TermIndexMode& termMode);
    void MoveToNextTerm();

public:
    // for test
    SegmentTermInfo* Top() const { return mSegmentTermInfos.top(); }
    size_t Size() const { return mSegmentTermInfos.size(); }

private:
    void AddOnDiskTermInfo(const index_base::SegmentMergeInfo& segMergeInfo);
    void AddQueueItem(const index_base::SegmentMergeInfo& segMergeInfo, const std::shared_ptr<IndexIterator>& indexIter,
                      const std::shared_ptr<SingleFieldIndexSegmentPatchIterator>& patchIter,
                      SegmentTermInfo::TermIndexMode mode);
    // virtual for UT
    virtual std::shared_ptr<IndexIterator>
    CreateOnDiskNormalIterator(const index_base::SegmentMergeInfo& segMergeInfo) const;

    virtual std::shared_ptr<IndexIterator>
    CreateOnDiskBitmapIterator(const index_base::SegmentMergeInfo& segMergeInfo) const;

    virtual std::shared_ptr<SingleFieldIndexSegmentPatchIterator>
    CreatePatchIterator(const index_base::SegmentMergeInfo& segMergeInfo) const;

private:
    typedef std::priority_queue<SegmentTermInfo*, std::vector<SegmentTermInfo*>, SegmentTermInfoComparator>
        PriorityQueue;
    PriorityQueue mSegmentTermInfos;
    std::vector<SegmentTermInfo*> mMergingSegmentTermInfos;
    config::IndexConfigPtr mIndexConfig;
    OnDiskIndexIteratorCreatorPtr mOnDiskIndexIterCreator;
    SegmentDirectoryBasePtr mSegmentDirectory;

private:
    friend class SegmentTermInfoQueueTest;
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(SegmentTermInfoQueue);
}}} // namespace indexlib::index::legacy
