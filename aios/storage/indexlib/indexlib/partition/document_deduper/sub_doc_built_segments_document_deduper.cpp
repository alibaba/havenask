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
#include "indexlib/partition/document_deduper/sub_doc_built_segments_document_deduper.h"

#include "indexlib/config/index_partition_schema.h"

using namespace std;
using namespace indexlib::config;

namespace indexlib { namespace partition {
IE_LOG_SETUP(partition, SubDocBuiltSegmentsDocumentDeduper);

SubDocBuiltSegmentsDocumentDeduper::SubDocBuiltSegmentsDocumentDeduper(const config::IndexPartitionSchemaPtr& schema)
    : DocumentDeduper(schema)
{
}

void SubDocBuiltSegmentsDocumentDeduper::Init(const partition::PartitionModifierPtr& modifier)
{
    partition::SubDocModifierPtr subDocModifier = DYNAMIC_POINTER_CAST(partition::SubDocModifier, modifier);
    assert(subDocModifier);

    mMainSegmentsDeduper.reset(new BuiltSegmentsDocumentDeduper(mSchema));
    // subDocModifier get pkReader is mainModifier pkReader
    // subDocModifier remove mainDupPk, can also remove its subDupPk
    mMainSegmentsDeduper->Init(subDocModifier);

    mSubSegmentsDeduper.reset(new BuiltSegmentsDocumentDeduper(mSchema->GetSubIndexPartitionSchema()));
    mSubSegmentsDeduper->Init(subDocModifier->GetSubModifier());
}

void SubDocBuiltSegmentsDocumentDeduper::Dedup()
{
    mMainSegmentsDeduper->Dedup();
    mSubSegmentsDeduper->Dedup();
}
}} // namespace indexlib::partition
