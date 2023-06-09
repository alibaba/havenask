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
#include "indexlib/index/normal/inverted_index/accessor/index_patch_iterator.h"
#include "indexlib/index/normal/inverted_index/accessor/single_field_index_patch_iterator.h"
#include "indexlib/indexlib.h"
#include "indexlib/util/Exception.h"

DECLARE_REFERENCE_CLASS(config, IndexPartitionSchema);
DECLARE_REFERENCE_CLASS(index_base, PartitionData);
DECLARE_REFERENCE_CLASS(index_base, Version);

namespace indexlib { namespace index {

class MultiFieldIndexPatchIterator : public IndexPatchIterator
{
public:
    MultiFieldIndexPatchIterator(const config::IndexPartitionSchemaPtr& schema)
        : _schema(schema)
        , _cursor(-1)
        , _isSub(false)
        , _patchLoadExpandSize(0)
    {
    }
    ~MultiFieldIndexPatchIterator() {}

public:
    void Init(const index_base::PartitionDataPtr& partitionData, bool ignorePatchToOldIncSegment,
              const index_base::Version& lastLoadVersion, segmentid_t startLoadSegment, bool isSub);

    std::unique_ptr<IndexUpdateTermIterator> Next() override;
    void CreateIndependentPatchWorkItems(std::vector<PatchWorkItem*>& workItems) override;
    size_t GetPatchLoadExpandSize() const override { return _patchLoadExpandSize; }

private:
    void AddSingleFieldIterator(const config::IndexConfigPtr& indexConfig,
                                const index_base::PartitionDataPtr& partitionData, bool ignorePatchToOldIncSegment,
                                segmentid_t startLoadSegment, bool isSub);
    void Log() const;

private:
    config::IndexPartitionSchemaPtr _schema;
    int64_t _cursor;
    bool _isSub;
    size_t _patchLoadExpandSize;
    std::vector<std::unique_ptr<SingleFieldIndexPatchIterator>> _singleFieldPatchIters;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(MultiFieldIndexPatchIterator);
}} // namespace indexlib::index
