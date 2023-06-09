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
#include "indexlib/index/normal/inverted_index/accessor/index_patch_iterator_creator.h"

#include "indexlib/config/index_partition_schema.h"
#include "indexlib/index/normal/inverted_index/accessor/index_patch_iterator.h"
#include "indexlib/index/normal/inverted_index/accessor/multi_field_index_patch_iterator.h"
#include "indexlib/index/normal/inverted_index/accessor/sub_doc_index_patch_iterator.h"

using namespace indexlib::config;
using namespace indexlib::index_base;

namespace indexlib { namespace index {
IE_LOG_SETUP(index, IndexPatchIteratorCreator);

IndexPatchIteratorCreator::IndexPatchIteratorCreator() {}

IndexPatchIteratorCreator::~IndexPatchIteratorCreator() {}

IndexPatchIteratorPtr IndexPatchIteratorCreator::Create(const IndexPartitionSchemaPtr& schema,
                                                        const PartitionDataPtr& partitionData,
                                                        bool ignorePatchToOldIncSegment, const Version& lastLoadVersion,
                                                        segmentid_t startLoadSegment)
{
    if (schema->GetSubIndexPartitionSchema()) {
        auto iterator = std::make_shared<SubDocIndexPatchIterator>(schema);
        iterator->Init(partitionData, ignorePatchToOldIncSegment, lastLoadVersion, startLoadSegment);
        return iterator;
    }
    MultiFieldIndexPatchIteratorPtr iterator(new MultiFieldIndexPatchIterator(schema));
    iterator->Init(partitionData, ignorePatchToOldIncSegment, lastLoadVersion, startLoadSegment, false);
    return iterator;
}
}} // namespace indexlib::index
