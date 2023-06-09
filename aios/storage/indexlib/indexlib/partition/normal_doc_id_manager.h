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

#include "indexlib/common_define.h"
#include "indexlib/partition/doc_id_manager.h"

DECLARE_REFERENCE_CLASS(config, IndexPartitionSchema);
DECLARE_REFERENCE_CLASS(document, Document);
DECLARE_REFERENCE_CLASS(document, NormalDocument);
DECLARE_REFERENCE_CLASS(document, AttributeDocument);
DECLARE_REFERENCE_CLASS(document, AttributeDocumentFieldExtractor);
DECLARE_REFERENCE_CLASS(index_base, PartitionData);
DECLARE_REFERENCE_CLASS(index_base, SegmentWriter);
DECLARE_REFERENCE_CLASS(index, CompositePrimaryKeyReader);
DECLARE_REFERENCE_CLASS(index, DefaultAttributeFieldAppender);
DECLARE_REFERENCE_CLASS(index, PartitionInfo);
DECLARE_REFERENCE_CLASS(index, PrimaryKeyIndexReader);
DECLARE_REFERENCE_CLASS(partition, PartitionModifier);
DECLARE_REFERENCE_CLASS(partition, SingleSegmentWriter);

namespace indexlib { namespace partition {

// DocIdManager validates documents and assigns doc ids to documents via Process().
// The assigned doc id might be INVALID_DOCID if the document cannot be found in the index via PK lookup.
// User of DocIdManager might want to make sure the behavior is handled correctly from outside when the doc id is
// invalid.

// 除了基类DocIdManager描述的职责，NormalDocIdManager包含了doc处理数据流中Add2Update的改写逻辑(当add
// doc的pk已经存在时，把add doc中的attribute转换成只对部分attribute的update，doc中倒排、summary等其他部分不变)
// 另外add doc的DefaultAttributeFieldAppender的逻辑也加到了NormalDocIdManager中。DefaultAttributeFieldAppender负责当add
// doc对应的原始doc中某些正排字段为空，但是正排对应schema要求正排有默认值占位的情况，此时DefaultAttributeFieldAppender会额外对doc做前置处理，加上正排的默认值。

class NormalDocIdManager : public DocIdManager
{
public:
    NormalDocIdManager(const config::IndexPartitionSchemaPtr& schema, bool enableAutoAdd2Update);
    ~NormalDocIdManager();

    NormalDocIdManager(const NormalDocIdManager&) = delete;
    NormalDocIdManager& operator=(const NormalDocIdManager&) = delete;
    NormalDocIdManager(NormalDocIdManager&&) = delete;
    NormalDocIdManager& operator=(NormalDocIdManager&&) = delete;

public: // public for upper modules
    void Reinit(const index_base::PartitionDataPtr& partitionData, const PartitionModifierPtr& partitionModifier,
                const index_base::SegmentWriterPtr& segmentWriter, PartitionWriter::BuildMode buildMode,
                bool delayDedupDocument) override;
    bool Process(const document::DocumentPtr& document) override;

public: // public for MainSubDocIdManager
    docid_t GetNextGlobalDocId() const;
    docid_t GetNextLocalDocId() const;
    index::CompositePrimaryKeyReader* GetPrimaryKeyReader();

    bool DoDeleteDocument(docid_t docId);
    bool DedupDocument(docid_t docId);

    AUTIL_NODISCARD bool ValidateAddDocument(const document::NormalDocumentPtr& doc);
    AUTIL_NODISCARD bool ValidateUpdateDocument(const document::NormalDocumentPtr& doc);
    AUTIL_NODISCARD bool ValidateDeleteDocument(const document::NormalDocumentPtr& doc);

    // Attention: @param deletedOldDocId is an (output + input) param
    AUTIL_NODISCARD bool ProcessAddDocument(const document::NormalDocumentPtr& doc, docid_t* deletedOldDocId = nullptr);
    AUTIL_NODISCARD bool ProcessUpdateDocument(const document::NormalDocumentPtr& doc);
    // DELETE doc in the batch can be handled in preprocess step.
    AUTIL_NODISCARD bool ProcessDeleteDocument(const document::NormalDocumentPtr& doc,
                                               docid_t* deletedOldDocId = nullptr);

private:
    void RewriteDocumentForAutoAdd2Update(const document::NormalDocumentPtr& doc);
    docid_t LookupPrimaryKey(const std::string& pk);

private:
    static bool IsPrimaryKeyStrValid(const std::string& str, const config::IndexPartitionSchemaPtr& schema);
    static bool CheckAttributeDocument(const document::AttributeDocumentPtr& attrDoc,
                                       const config::IndexPartitionSchemaPtr& schema);

public:
    void TEST_Init(const std::shared_ptr<index::PrimaryKeyIndexReader>& primaryKeyIndexReader,
                   const index::PartitionInfoPtr& partitionInfo, docid_t baseDocId,
                   const PartitionModifierPtr& modifier, const SingleSegmentWriterPtr& segmentWriter,
                   PartitionWriter::BuildMode buildMode, bool delayDedupDocument);
    PartitionModifierPtr TEST_GetPartitionModifier() const { return _modifier; }

private:
    config::IndexPartitionSchemaPtr _schema;
    bool _enableAutoAdd2Update;
    bool _enableInsertOrIgnore;
    std::unique_ptr<index::DefaultAttributeFieldAppender> _defaultValueAppender;
    std::unique_ptr<document::AttributeDocumentFieldExtractor> _attrFieldExtractor;

    PartitionWriter::BuildMode _buildMode;
    bool _delayDedupDocument;
    PartitionModifierPtr _modifier;
    SingleSegmentWriterPtr _segmentWriter;
    std::unique_ptr<index::CompositePrimaryKeyReader> _compositePrimaryKeyReader;
    index::PartitionInfoPtr _partitionInfo;
    docid_t _baseDocId;

private:
    IE_LOG_DECLARE();
};

}} // namespace indexlib::partition
