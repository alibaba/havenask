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
#include <queue>

#include "indexlib/common_define.h"
#include "indexlib/config/index_config.h"
#include "indexlib/file_system/IDirectory.h"
#include "indexlib/index/normal/inverted_index/accessor/index_patch_iterator.h"
#include "indexlib/index_base/patch/patch_file_finder.h"
#include "indexlib/indexlib.h"
#include "indexlib/util/Exception.h"

DECLARE_REFERENCE_CLASS(index_base, PartitionData);
DECLARE_REFERENCE_CLASS(index, SingleFieldIndexSegmentPatchIterator);

namespace indexlib { namespace index {

class SingleFieldIndexPatchIterator : public IndexPatchIterator
{
public:
    SingleFieldIndexPatchIterator(const config::IndexConfigPtr& indexConfig, bool isSub)
        : _indexConfig(indexConfig)
        , _isSub(isSub)
        , _cursor(-1)
        , _patchLoadExpandSize(0)
        , _patchItemCount(0)
    {
    }
    ~SingleFieldIndexPatchIterator() {}

public:
    void Init(const index_base::PartitionDataPtr& partitionData, bool isIncConsistentWithRealtime,
              segmentid_t startLoadSegment);
    size_t GetPatchLoadExpandSize() const override { return _patchLoadExpandSize; }
    size_t GetPatchItemCount() const { return _patchItemCount; }
    const std::string& GetIndexName() const { return _indexConfig->GetIndexName(); }

public:
    std::unique_ptr<IndexUpdateTermIterator> Next() override;
    void CreateIndependentPatchWorkItems(std::vector<PatchWorkItem*>& workItems) override;

private:
    std::shared_ptr<indexlib::index::SingleFieldIndexSegmentPatchIterator>
    CreateSegmentPatchIterator(const index_base::PatchFileInfoVec& patchInfoVec, segmentid_t targetSegment);
    void Log() const;

private:
    config::IndexConfigPtr _indexConfig;
    bool _isSub;
    std::vector<std::shared_ptr<indexlib::index::SingleFieldIndexSegmentPatchIterator>> _segmentIterators;
    int64_t _cursor;
    size_t _patchLoadExpandSize;
    size_t _patchItemCount;

private:
    IE_LOG_DECLARE();
};
}} // namespace indexlib::index
