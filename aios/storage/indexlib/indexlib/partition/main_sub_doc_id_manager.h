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
DECLARE_REFERENCE_CLASS(common, AttributeConvertor);
DECLARE_REFERENCE_CLASS(document, Document);
DECLARE_REFERENCE_CLASS(document, NormalDocument);
DECLARE_REFERENCE_CLASS(index_base, PartitionData);
DECLARE_REFERENCE_CLASS(index_base, SegmentWriter);
DECLARE_REFERENCE_CLASS(index, CompositeJoinDocIdReader);
DECLARE_REFERENCE_CLASS(index, CompositePrimaryKeyReader);
DECLARE_REFERENCE_CLASS(index, JoinDocidAttributeReader);
DECLARE_REFERENCE_CLASS(partition, PartitionModifier);
DECLARE_REFERENCE_CLASS(partition, SubDocModifier);
DECLARE_REFERENCE_CLASS(partition, NormalDocIdManager);

namespace indexlib { namespace partition {

// MainSubDocIdManager是针对主子表场景的特殊化了的DocIdManager，包含了主表和子表各自对应的NormalDocIdManager。此外，MainSubDocIdManager包含了校验主子doc的一些特殊逻辑，比如子doc去重，主子doc
// id对应关系校验，前置处理在主子doc中分别加入主子doc id互相映射所需的特殊字段(main_docid_to_sub_docid_attr_name,
// sub_docid_to_main_docid_attr_name)

class MainSubDocIdManager : public DocIdManager
{
public:
    MainSubDocIdManager(const config::IndexPartitionSchemaPtr& schema);
    ~MainSubDocIdManager();

    MainSubDocIdManager(const MainSubDocIdManager&) = delete;
    MainSubDocIdManager& operator=(const MainSubDocIdManager&) = delete;
    MainSubDocIdManager(MainSubDocIdManager&&) = delete;
    MainSubDocIdManager& operator=(MainSubDocIdManager&&) = delete;

public:
    void Reinit(const index_base::PartitionDataPtr& partitionData, const PartitionModifierPtr& partitionModifier,
                const index_base::SegmentWriterPtr& segmentWriter, PartitionWriter::BuildMode buildMode,
                bool delayDedupDocument) override;
    bool Process(const document::DocumentPtr& document) override;

public:
    NormalDocIdManager* GetMainDocIdManager() const { return _mainDocIdManager.get(); }
    NormalDocIdManager* GetSubDocIdManager() const { return _subDocIdManager.get(); }
    void TEST_Init(std::unique_ptr<NormalDocIdManager> mainDocIdManager,
                   std::unique_ptr<NormalDocIdManager> subDocIdManager,
                   const index::JoinDocidAttributeReaderPtr mainToSubJoinAttributeReader);

private:
    AUTIL_NODISCARD bool ValidateAndCorrectAddDocument(const document::NormalDocumentPtr& doc);
    AUTIL_NODISCARD bool ValidateDeleteDocument(const document::NormalDocumentPtr& doc);

    AUTIL_NODISCARD bool ProcessAddDocument(const document::NormalDocumentPtr& doc);
    AUTIL_NODISCARD bool ProcessUpdateDocument(const document::NormalDocumentPtr& doc);
    AUTIL_NODISCARD bool ProcessDeleteDocument(const document::NormalDocumentPtr& doc);
    AUTIL_NODISCARD bool ProcessDeleteSubDocument(const document::NormalDocumentPtr& doc);

    void DedupSubDocs(const document::NormalDocumentPtr& doc);
    bool IsSubDocDuplicated(const document::NormalDocumentPtr& subDoc, const std::set<std::string>& pkSet);

    void AddJoinFieldToMainDocument(const document::NormalDocumentPtr& doc);
    void UpdateMainDocumentJoinValue(const document::NormalDocumentPtr& doc, docid_t subDocIdEnd);
    void AddJoinFieldToSubDocuments(const document::NormalDocumentPtr& doc);

private:
    static bool NeedUpdate(const document::NormalDocumentPtr& doc, fieldid_t pkIndexField);
    static bool ValidateAndCorrectMainSubRelation(const document::NormalDocumentPtr& doc,
                                                  const index::CompositePrimaryKeyReader* mainPrimaryKeyReader,
                                                  const index::CompositePrimaryKeyReader* subPrimaryKeyxReader,
                                                  const index::JoinDocidAttributeReader* joinDocIdAttributeReader);

private:
    // 这里管理的都是 global docid
    config::IndexPartitionSchemaPtr _schema;
    std::unique_ptr<NormalDocIdManager> _mainDocIdManager;
    std::unique_ptr<NormalDocIdManager> _subDocIdManager;
    std::unique_ptr<index::CompositeJoinDocIdReader> _mainToSubJoinAttributeReader;

    fieldid_t _mainJoinFieldId;
    fieldid_t _subJoinFieldId;
    std::unique_ptr<common::AttributeConvertor> _mainJoinFieldConverter;
    std::unique_ptr<common::AttributeConvertor> _subJoinFieldConverter;

private:
    IE_LOG_DECLARE();
};

}} // namespace indexlib::partition
