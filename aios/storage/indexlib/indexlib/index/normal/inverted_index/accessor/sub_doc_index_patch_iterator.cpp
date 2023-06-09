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
#include "indexlib/index/normal/inverted_index/accessor/sub_doc_index_patch_iterator.h"

#include "indexlib/config/index_partition_schema.h"
#include "indexlib/index/inverted_index/patch/IndexUpdateTermIterator.h"
#include "indexlib/index_base/partition_data.h"

namespace indexlib { namespace index {

SubDocIndexPatchIterator::SubDocIndexPatchIterator(const config::IndexPartitionSchemaPtr& schema)
    : _schema(schema)
    , _mainIterator(schema)
    , _subIterator(schema->GetSubIndexPartitionSchema())
{
}

void SubDocIndexPatchIterator::Init(const index_base::PartitionDataPtr& partitionData, bool ignorePatchToOldIncSegment,
                                    const index_base::Version& lastLoadVersion, segmentid_t startLoadSegment)
{
    _mainIterator.Init(partitionData, ignorePatchToOldIncSegment, lastLoadVersion, startLoadSegment, false);
    auto subPartitionData = partitionData->GetSubPartitionData();
    assert(subPartitionData);
    _subIterator.Init(subPartitionData, ignorePatchToOldIncSegment, lastLoadVersion, startLoadSegment, true);
}

std::unique_ptr<IndexUpdateTermIterator> SubDocIndexPatchIterator::Next()
{
    auto termIter = _mainIterator.Next();
    if (termIter) {
        return termIter;
    }
    return _subIterator.Next();
}
}} // namespace indexlib::index
