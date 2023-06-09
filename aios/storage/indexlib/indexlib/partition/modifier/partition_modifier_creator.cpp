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
#include "indexlib/partition/modifier/partition_modifier_creator.h"

#include "indexlib/index_base/partition_data.h"
#include "indexlib/partition/modifier/inplace_modifier.h"
#include "indexlib/partition/modifier/offline_modifier.h"
#include "indexlib/partition/modifier/patch_modifier.h"
#include "indexlib/partition/modifier/sub_doc_modifier.h"

using namespace std;
using namespace indexlib::index;
using namespace indexlib::config;
using namespace indexlib::index_base;

namespace indexlib { namespace partition {
IE_LOG_SETUP(partition, PartitionModifierCreator);

PartitionModifierCreator::PartitionModifierCreator() {}

PartitionModifierCreator::~PartitionModifierCreator() {}

PartitionModifierPtr PartitionModifierCreator::CreatePatchModifier(const IndexPartitionSchemaPtr& schema,
                                                                   const PartitionDataPtr& partitionData, bool needPk,
                                                                   bool enablePackFile)
{
    if (schema->GetTableType() == tt_kkv || schema->GetTableType() == tt_kv) {
        return PartitionModifierPtr();
    }
    // TODO: config need check: main no pk, but sub has pk
    if (needPk && !schema->GetIndexSchema()->HasPrimaryKeyIndex()) {
        return PartitionModifierPtr();
    }

    const IndexPartitionSchemaPtr& subSchema = schema->GetSubIndexPartitionSchema();
    // TODO: now sub no pk, we don't delete sub docs
    if (!subSchema) {
        PatchModifierPtr modifier(new PatchModifier(schema, enablePackFile));
        modifier->Init(partitionData);
        return modifier;
    }

    assert(subSchema->GetIndexSchema()->HasPrimaryKeyIndex());

    SubDocModifierPtr modifier(new SubDocModifier(schema));
    modifier->Init(partitionData, enablePackFile, false);
    return modifier;
}

PartitionModifierPtr PartitionModifierCreator::CreateInplaceModifier(const IndexPartitionSchemaPtr& schema,
                                                                     const IndexPartitionReaderPtr& reader)
{
    if (schema->GetTableType() == tt_kkv || schema->GetTableType() == tt_kv) {
        return PartitionModifierPtr();
    }
    if (!schema->GetIndexSchema()->HasPrimaryKeyIndex()) {
        return PartitionModifierPtr();
    }

    const IndexPartitionSchemaPtr& subSchema = schema->GetSubIndexPartitionSchema();
    // TODO: now sub no pk, we don't delete sub docs
    if (!subSchema) {
        InplaceModifierPtr modifier(new InplaceModifier(schema));
        modifier->Init(reader, reader->GetPartitionData()->GetInMemorySegment());
        return modifier;
    }

    assert(subSchema->GetIndexSchema()->HasPrimaryKeyIndex());
    SubDocModifierPtr modifier(new SubDocModifier(schema));
    modifier->Init(reader);
    return modifier;
}

// TODO: refactor
PartitionModifierPtr PartitionModifierCreator::CreateOfflineModifier(const config::IndexPartitionSchemaPtr& schema,
                                                                     const PartitionDataPtr& partitionData, bool needPk,
                                                                     bool enablePackFile)
{
    if (schema->GetTableType() == tt_kkv || schema->GetTableType() == tt_kv) {
        return PartitionModifierPtr();
    }
    // TODO: config need check: main no pk, but sub has pk
    if (needPk && !schema->GetIndexSchema()->HasPrimaryKeyIndex()) {
        return PartitionModifierPtr();
    }

    const IndexPartitionSchemaPtr& subSchema = schema->GetSubIndexPartitionSchema();
    // TODO: now sub no pk, we don't delete sub docs
    if (!subSchema) {
        OfflineModifierPtr modifier(new OfflineModifier(schema, enablePackFile));
        modifier->Init(partitionData);
        return modifier;
    }

    assert(subSchema->GetIndexSchema()->HasPrimaryKeyIndex());

    SubDocModifierPtr modifier(new SubDocModifier(schema));
    modifier->Init(partitionData, enablePackFile, true);
    return modifier;
}
}} // namespace indexlib::partition
