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

#include "indexlib/common_define.h"
#include "indexlib/index/normal/inverted_index/accessor/index_modifier.h"
#include "indexlib/indexlib.h"

DECLARE_REFERENCE_CLASS(index, InvertedIndexReader);
DECLARE_REFERENCE_CLASS(index, LegacyIndexReader);
DECLARE_REFERENCE_CLASS(index_base, PartitionData);
DECLARE_REFERENCE_CLASS(document, ModifiedTokens);
DECLARE_REFERENCE_CLASS(file_system, Directory);
DECLARE_REFERENCE_CLASS(config, IndexConfig);

namespace indexlib { namespace index { namespace legacy {
class MultiFieldIndexReader;
}}} // namespace indexlib::index::legacy

namespace indexlib { namespace index {

class InplaceIndexModifier final : public IndexModifier
{
public:
    InplaceIndexModifier(config::IndexPartitionSchemaPtr schema, util::BuildResourceMetrics* buildResourceMetrics);
    ~InplaceIndexModifier() {}

public:
    void Update(docid_t docId, const document::IndexDocumentPtr& indexDoc) override;
    // must be thread-safe
    void UpdateByIndex(docid_t docId, const document::IndexDocumentPtr& indexDoc, indexid_t indexId, size_t shardId);
    bool UpdateField(docid_t docId, const document::ModifiedTokens& modifiedTokens) override;
    bool UpdateIndex(IndexUpdateTermIterator* iterator) override;
    void Dump(const file_system::DirectoryPtr& dir) {}

public:
    void Init(const std::shared_ptr<InvertedIndexReader>& indexReader,
              const index_base::PartitionDataPtr& partitionData);

private:
    void UpdateOneField(docid_t docId, indexid_t indexId, const document::ModifiedTokens& tokens);
    void UpdateWithShardId(const config::IndexConfigPtr& indexConfig,
                           const std::shared_ptr<index::InvertedIndexReader>& indexReader, size_t shardId,
                           docid_t docId, const document::ModifiedTokens& modifiedTokens);

private:
    std::shared_ptr<legacy::MultiFieldIndexReader> _indexReader;

private:
    IE_LOG_DECLARE();
};
}} // namespace indexlib::index
