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
#include "indexlib/index/normal/inverted_index/accessor/inplace_index_modifier.h"

#include "indexlib/config/index_partition_schema.h"
#include "indexlib/document/index_document/normal_document/index_document.h"
#include "indexlib/index/inverted_index/patch/IndexUpdateTermIterator.h"
#include "indexlib/index/normal/inverted_index/accessor/multi_field_index_reader.h"
#include "indexlib/index/normal/inverted_index/accessor/multi_sharding_index_reader.h"

namespace indexlib { namespace index {
IE_LOG_SETUP(index, InplaceIndexModifier);

InplaceIndexModifier::InplaceIndexModifier(config::IndexPartitionSchemaPtr schema,
                                           util::BuildResourceMetrics* buildResourceMetrics)
    : IndexModifier(std::move(schema), buildResourceMetrics)
{
}

void InplaceIndexModifier::Update(docid_t docId, const document::IndexDocumentPtr& indexDoc)
{
    assert(indexDoc);
    const auto& multiFieldModifiedTokens = indexDoc->GetModifiedTokens();
    if (multiFieldModifiedTokens.empty()) {
        return;
    }
    for (const auto& modifiedTokens : multiFieldModifiedTokens) {
        UpdateField(docId, modifiedTokens);
    }
}

void InplaceIndexModifier::UpdateByIndex(docid_t docId, const document::IndexDocumentPtr& indexDoc, indexid_t indexId,
                                         size_t shardId)
{
    const auto& indexConfig = _schema->GetIndexSchema()->GetIndexConfig(indexId);
    if (!indexConfig->IsIndexUpdatable()) {
        return;
    }
    if (!indexConfig->GetNonTruncateIndexName().empty()) {
        return;
    }
    const auto& singleIndexReader = _indexReader->GetIndexReaderWithIndexId(indexId);
    assert(singleIndexReader);

    assert(indexDoc);
    const auto& multiFieldModifiedTokens = indexDoc->GetModifiedTokens();
    if (multiFieldModifiedTokens.empty()) {
        return;
    }

    for (const auto& modifiedTokens : multiFieldModifiedTokens) {
        if (!modifiedTokens.Valid()) {
            continue;
        }
        fieldid_t fieldId = modifiedTokens.FieldId();
        if (indexConfig->IsInIndex(fieldId)) {
            UpdateWithShardId(indexConfig, singleIndexReader, shardId, docId, modifiedTokens);
        }
    }
}

void InplaceIndexModifier::UpdateWithShardId(const config::IndexConfigPtr& indexConfig,
                                             const std::shared_ptr<index::InvertedIndexReader>& indexReader,
                                             size_t shardId, docid_t docId,
                                             const document::ModifiedTokens& modifiedTokens)
{
    config::IndexConfig::IndexShardingType shardingType = indexConfig->GetShardingType();
    assert(shardingType != config::IndexConfig::IST_IS_SHARDING);
    if (shardingType == config::IndexConfig::IST_NO_SHARDING) {
        const auto& legacyIndexReader = std::dynamic_pointer_cast<index::LegacyIndexReaderInterface>(indexReader);
        assert(legacyIndexReader);
        legacyIndexReader->Update(docId, modifiedTokens);
        return;
    }
    index::MultiShardingIndexReaderPtr multiShardingIndexReader =
        std::dynamic_pointer_cast<index::MultiShardingIndexReader>(indexReader);
    assert(multiShardingIndexReader);
    multiShardingIndexReader->UpdateByShardId(shardId, docId, modifiedTokens);
}

void InplaceIndexModifier::UpdateOneField(docid_t docId, indexid_t indexId, const document::ModifiedTokens& tokens)
{
    const auto& indexConfig = _schema->GetIndexSchema()->GetIndexConfig(indexId);
    if (!indexConfig || indexConfig->IsDisabled() || indexConfig->IsDeleted()) {
        return;
    }
    if (!indexConfig->IsIndexUpdatable()) {
        return;
    }
    if (!indexConfig->GetNonTruncateIndexName().empty()) {
        return;
    }

    const auto& singleIndexReader = _indexReader->GetIndexReaderWithIndexId(indexId);
    assert(singleIndexReader);
    const auto& legacyIndexReader = std::dynamic_pointer_cast<index::LegacyIndexReaderInterface>(singleIndexReader);
    assert(singleIndexReader);
    legacyIndexReader->Update(docId, tokens);
}

bool InplaceIndexModifier::UpdateField(docid_t docId, const document::ModifiedTokens& modifiedTokens)
{
    if (!modifiedTokens.Valid()) {
        return false;
    }
    fieldid_t fieldId = modifiedTokens.FieldId();
    const auto& indexIdList = _schema->GetIndexSchema()->GetIndexIdList(fieldId);
    for (auto indexId : indexIdList) {
        UpdateOneField(docId, indexId, modifiedTokens);
    }
    return true;
}

bool InplaceIndexModifier::UpdateIndex(IndexUpdateTermIterator* iterator)
{
    const auto& indexConfig = _schema->GetIndexSchema()->GetIndexConfig(iterator->GetIndexId());
    if (!indexConfig) {
        IE_LOG(WARN, "update index on null index config");
        return true;
    }
    if (indexConfig->IsDisabled()) {
        IE_LOG(WARN, "update index on disabled index config");
        return true;
    }
    if (indexConfig->IsDeleted()) {
        IE_LOG(WARN, "update index on deleted index config");
        return true;
    }
    const auto& singleIndexReader = _indexReader->GetIndexReaderWithIndexId(iterator->GetIndexId());
    assert(singleIndexReader);
    const auto& legacyIndexReader = std::dynamic_pointer_cast<index::LegacyIndexReaderInterface>(singleIndexReader);
    assert(legacyIndexReader);
    legacyIndexReader->UpdateIndex(iterator);
    return true;
}

void InplaceIndexModifier::Init(const std::shared_ptr<InvertedIndexReader>& indexReader,
                                const index_base::PartitionDataPtr& partitionData)
{
    _indexReader = DYNAMIC_POINTER_CAST(legacy::MultiFieldIndexReader, indexReader);
    assert(_indexReader);
    const auto& indexSchema = _schema->GetIndexSchema();
    auto indexConfigs = indexSchema->CreateIterator();
    for (auto iter = indexConfigs->Begin(); iter != indexConfigs->End(); ++iter) {
        const auto& indexConfig = *iter;
        if (indexConfig->IsIndexUpdatable()) {
            const auto& singleIndexReader = _indexReader->GetIndexReaderWithIndexId(indexConfig->GetIndexId());
            if (singleIndexReader) {
                const auto& legacyIndexReader =
                    std::dynamic_pointer_cast<index::LegacyIndexReaderInterface>(singleIndexReader);
                assert(legacyIndexReader);
                legacyIndexReader->InitBuildResourceMetricsNode(_buildResourceMetrics);
            }
        }
    }
}
}} // namespace indexlib::index
