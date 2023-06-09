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

#include "indexlib/index/normal/inverted_index/accessor/index_patch_iterator.h"
#include "indexlib/index/normal/inverted_index/accessor/multi_field_index_patch_iterator.h"

namespace indexlib { namespace index {

class SubDocIndexPatchIterator : public IndexPatchIterator
{
public:
    SubDocIndexPatchIterator(const config::IndexPartitionSchemaPtr& schema);
    ~SubDocIndexPatchIterator() {}

public:
    void Init(const index_base::PartitionDataPtr& partitionData, bool ignorePatchToOldIncSegment,
              const index_base::Version& lastLoadVersion, segmentid_t startLoadSegment);

public:
    std::unique_ptr<IndexUpdateTermIterator> Next() override;
    void CreateIndependentPatchWorkItems(std::vector<PatchWorkItem*>& workItems) override
    {
        _mainIterator.CreateIndependentPatchWorkItems(workItems);
        _subIterator.CreateIndependentPatchWorkItems(workItems);
    }
    size_t GetPatchLoadExpandSize() const override
    {
        return _mainIterator.GetPatchLoadExpandSize() + _subIterator.GetPatchLoadExpandSize();
    }

private:
    config::IndexPartitionSchemaPtr _schema;
    MultiFieldIndexPatchIterator _mainIterator;
    MultiFieldIndexPatchIterator _subIterator;
};
}} // namespace indexlib::index
