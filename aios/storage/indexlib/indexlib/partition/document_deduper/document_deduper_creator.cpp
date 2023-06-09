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
#include "indexlib/partition/document_deduper/document_deduper_creator.h"

#include "indexlib/config/document_deduper_helper.h"
#include "indexlib/config/index_partition_options.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/index_base/partition_data.h"
#include "indexlib/partition/document_deduper/building_segment_document_deduper.h"
#include "indexlib/partition/document_deduper/built_segments_document_deduper.h"
#include "indexlib/partition/document_deduper/sub_doc_building_segment_document_deduper.h"
#include "indexlib/partition/document_deduper/sub_doc_built_segments_document_deduper.h"

using namespace std;
using namespace indexlib::config;
using namespace indexlib::index;
using namespace indexlib::index_base;

namespace indexlib { namespace partition {
IE_LOG_SETUP(partition, DocumentDeduperCreator);

DocumentDeduperCreator::DocumentDeduperCreator() {}

DocumentDeduperCreator::~DocumentDeduperCreator() {}

bool DocumentDeduperCreator::CanCreateDeduper(const IndexPartitionSchemaPtr& schema,
                                              const IndexPartitionOptions& options,
                                              const PartitionModifierPtr& modifier)
{
    if (!modifier) {
        return false;
    }

    const IndexSchemaPtr& indexSchema = schema->GetIndexSchema();
    assert(indexSchema);
    return DocumentDeduperHelper::DelayDedupDocument(options, indexSchema->GetPrimaryKeyIndexConfig());
}

DocumentDeduperPtr DocumentDeduperCreator::CreateBuildingSegmentDeduper(const IndexPartitionSchemaPtr& schema,
                                                                        const IndexPartitionOptions& options,
                                                                        const InMemorySegmentPtr& inMemSegment,
                                                                        const PartitionModifierPtr& modifier)
{
    if (!CanCreateDeduper(schema, options, modifier)) {
        return DocumentDeduperPtr();
    }

    assert(inMemSegment);
    if (schema->GetSubIndexPartitionSchema()) {
        SubDocBuildingSegmentDocumentDeduperPtr deduper(new SubDocBuildingSegmentDocumentDeduper(schema));
        deduper->Init(inMemSegment, modifier);
        return deduper;
    }

    BuildingSegmentDocumentDeduperPtr deduper(new BuildingSegmentDocumentDeduper(schema));
    deduper->Init(inMemSegment, modifier);
    return deduper;
}

DocumentDeduperPtr DocumentDeduperCreator::CreateBuiltSegmentsDeduper(const IndexPartitionSchemaPtr& schema,
                                                                      const IndexPartitionOptions& options,
                                                                      const PartitionModifierPtr& modifier)
{
    if (!CanCreateDeduper(schema, options, modifier)) {
        return DocumentDeduperPtr();
    }

    if (schema->GetSubIndexPartitionSchema()) {
        SubDocBuiltSegmentsDocumentDeduperPtr deduper(new SubDocBuiltSegmentsDocumentDeduper(schema));
        deduper->Init(modifier);
        return deduper;
    }

    BuiltSegmentsDocumentDeduperPtr deduper(new BuiltSegmentsDocumentDeduper(schema));
    deduper->Init(modifier);
    return deduper;
}
}} // namespace indexlib::partition
