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
#include "indexlib/index/normal/attribute/accessor/patch_iterator_creator.h"

#include "indexlib/config/index_partition_schema.h"
#include "indexlib/index/normal/attribute/accessor/multi_field_patch_iterator.h"
#include "indexlib/index/normal/attribute/accessor/patch_iterator.h"
#include "indexlib/index/normal/attribute/accessor/sub_doc_patch_iterator.h"

using namespace std;
using namespace indexlib::config;
using namespace indexlib::index_base;

namespace indexlib { namespace index {
IE_LOG_SETUP(index, PatchIteratorCreator);

PatchIteratorCreator::PatchIteratorCreator() {}

PatchIteratorCreator::~PatchIteratorCreator() {}

PatchIteratorPtr PatchIteratorCreator::Create(const IndexPartitionSchemaPtr& schema,
                                              const PartitionDataPtr& partitionData, bool ignorePatchToOldIncSegment,
                                              const Version& lastLoadVersion, segmentid_t startLoadSegment)
{
    if (schema->GetSubIndexPartitionSchema()) {
        SubDocPatchIteratorPtr iterator(new SubDocPatchIterator(schema));
        iterator->Init(partitionData, ignorePatchToOldIncSegment, lastLoadVersion, startLoadSegment);
        return iterator;
    }
    MultiFieldPatchIteratorPtr iterator(new MultiFieldPatchIterator(schema));
    iterator->Init(partitionData, ignorePatchToOldIncSegment, lastLoadVersion, startLoadSegment);
    return iterator;
}
}} // namespace indexlib::index
