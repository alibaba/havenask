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
#include "indexlib/partition/main_sub_doc_id_manager.h"

#include "autil/ConstString.h"
#include "indexlib/common/field_format/attribute/attribute_convertor_factory.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/document/index_document/normal_document/normal_document.h"
#include "indexlib/index/normal/attribute/accessor/attribute_reader_factory.h"
#include "indexlib/index/normal/attribute/accessor/composite_join_docid_attribute_reader.h"
#include "indexlib/index/normal/attribute/accessor/join_docid_attribute_reader.h"
#include "indexlib/index/normal/primarykey/composite_primary_key_reader.h"
#include "indexlib/index/partition_info.h"
#include "indexlib/index/primary_key/PrimaryKeyIndexReader.h"
#include "indexlib/index_base/partition_data.h"
#include "indexlib/partition/modifier/main_sub_modifier_util.h"
#include "indexlib/partition/modifier/sub_doc_modifier.h"
#include "indexlib/partition/normal_doc_id_manager.h"
#include "indexlib/partition/segment/single_segment_writer.h"
#include "indexlib/partition/segment/sub_doc_segment_writer.h"

namespace indexlib { namespace partition {
IE_LOG_SETUP(partition, MainSubDocIdManager);

MainSubDocIdManager::MainSubDocIdManager(const config::IndexPartitionSchemaPtr& schema) : _schema(schema)
{
    assert(schema && schema->GetSubIndexPartitionSchema());
    _mainDocIdManager = std::make_unique<NormalDocIdManager>(schema, /*enableAutoAdd2Update=*/false);
    _subDocIdManager =
        std::make_unique<NormalDocIdManager>(schema->GetSubIndexPartitionSchema(), /*enableAutoAdd2Update=*/false);
    _mainToSubJoinAttributeReader = std::make_unique<index::CompositeJoinDocIdReader>();

    // init join field ids
    const auto& subSchema = _schema->GetSubIndexPartitionSchema();

    _mainJoinFieldId = _schema->GetFieldId(MAIN_DOCID_TO_SUB_DOCID_ATTR_NAME);
    _mainJoinFieldConverter.reset(common::AttributeConvertorFactory::GetInstance()->CreateAttrConvertor(
        _schema->GetAttributeSchema()->GetAttributeConfig(MAIN_DOCID_TO_SUB_DOCID_ATTR_NAME)));
    assert(_mainJoinFieldConverter);

    _subJoinFieldId = subSchema->GetFieldId(SUB_DOCID_TO_MAIN_DOCID_ATTR_NAME);
    _subJoinFieldConverter.reset(common::AttributeConvertorFactory::GetInstance()->CreateAttrConvertor(
        subSchema->GetAttributeSchema()->GetAttributeConfig(SUB_DOCID_TO_MAIN_DOCID_ATTR_NAME)));
    assert(_subJoinFieldConverter);
}

MainSubDocIdManager::~MainSubDocIdManager() {}

void MainSubDocIdManager::Reinit(const index_base::PartitionDataPtr& partitionData,
                                 const PartitionModifierPtr& partitionModifier,
                                 const index_base::SegmentWriterPtr& segmentWriter,
                                 PartitionWriter::BuildMode buildMode, bool delayDedupDocument)
{
    SubDocModifierPtr mainSubModifier = std::dynamic_pointer_cast<SubDocModifier>(partitionModifier);
    assert(mainSubModifier);

    SubDocSegmentWriterPtr mainSubSegmentWriter = std::dynamic_pointer_cast<SubDocSegmentWriter>(segmentWriter);
    assert(mainSubSegmentWriter);

    _mainDocIdManager->Reinit(partitionData, mainSubModifier->GetMainModifier(), mainSubSegmentWriter->GetMainWriter(),
                              buildMode, delayDedupDocument);
    _subDocIdManager->Reinit(partitionData->GetSubPartitionData(), mainSubModifier->GetSubModifier(),
                             mainSubSegmentWriter->GetSubWriter(), buildMode, delayDedupDocument);

    _mainToSubJoinAttributeReader->Reinit(mainSubModifier->GetMainJoinAttributeReader());
}

void MainSubDocIdManager::TEST_Init(std::unique_ptr<NormalDocIdManager> mainDocIdManager,
                                    std::unique_ptr<NormalDocIdManager> subDocIdManager,
                                    const index::JoinDocidAttributeReaderPtr mainToSubJoinAttributeReader)
{
    _mainDocIdManager = std::move(mainDocIdManager);
    _subDocIdManager = std::move(subDocIdManager);
    _mainToSubJoinAttributeReader->Init(mainToSubJoinAttributeReader);
}

bool MainSubDocIdManager::Process(const document::DocumentPtr& document)
{
    document::NormalDocumentPtr doc = DYNAMIC_POINTER_CAST(document::NormalDocument, document);
    doc->SetDocId(INVALID_DOCID);

    DocOperateType opType = document->GetDocOperateType();
    switch (opType) {
    case ADD_DOC: {
        return ValidateAndCorrectAddDocument(doc) && ProcessAddDocument(doc);
    }
    case DELETE_DOC: {
        return ValidateDeleteDocument(doc) && ProcessDeleteDocument(doc);
    }
    case DELETE_SUB_DOC: {
        return ProcessDeleteSubDocument(doc);
    }
    case UPDATE_FIELD: {
        return ProcessUpdateDocument(doc);
    }
    default:
        return true;
    }
}

bool MainSubDocIdManager::NeedUpdate(const document::NormalDocumentPtr& doc, fieldid_t pkIndexField)
{
    bool needUpdate = false;
    const document::AttributeDocumentPtr& attrDoc = doc->GetAttributeDocument();
    if (attrDoc) {
        document::AttributeDocument::Iterator it = attrDoc->CreateIterator();
        fieldid_t fieldId = INVALID_FIELDID;
        while (it.HasNext()) {
            const autil::StringView& fieldValue = it.Next(fieldId);
            if (fieldValue.empty()) {
                continue;
            }
            if (fieldId != pkIndexField) {
                needUpdate = true;
                break;
            }
        }
    }
    const document::IndexDocumentPtr& indexDoc = doc->GetIndexDocument();
    if (indexDoc && !indexDoc->GetModifiedTokens().empty()) {
        needUpdate = true;
    }
    return needUpdate;
}

bool MainSubDocIdManager::ValidateAndCorrectMainSubRelation(
    const document::NormalDocumentPtr& doc, const index::CompositePrimaryKeyReader* mainPrimaryKeyReader,
    const index::CompositePrimaryKeyReader* subPrimaryKeyxReader,
    const index::JoinDocidAttributeReader* joinDocIdAttributeReader)
{
    docid_t mainDocId = mainPrimaryKeyReader->Lookup(doc->GetIndexDocument()->GetPrimaryKey());
    if (mainDocId == INVALID_DOCID) {
        return false;
    }
    doc->SetDocId(mainDocId);

    docid_t subDocIdBegin = INVALID_DOCID;
    docid_t subDocIdEnd = INVALID_DOCID;
    MainSubModifierUtil::GetSubDocIdRange(joinDocIdAttributeReader, mainDocId, &subDocIdBegin, &subDocIdEnd);

    const document::NormalDocument::DocumentVector& subDocuments = doc->GetSubDocuments();
    for (int32_t i = (int32_t)subDocuments.size() - 1; i >= 0; --i) {
        docid_t subDocId = subPrimaryKeyxReader->Lookup(subDocuments[i]->GetIndexDocument()->GetPrimaryKey());
        // Check: @qingran 这是一个变更，原来 INVALID_DOCID 不会被丢掉而是直接 continue，确认该变更的正确性
        if (subDocId == INVALID_DOCID || subDocId < subDocIdBegin || subDocId >= subDocIdEnd) {
            IE_LOG(WARN, "sub doc not in same doc, mainPK[%s], subPK[%s]", doc->GetPrimaryKey().c_str(),
                   subDocuments[i]->GetPrimaryKey().c_str());
            ERROR_COLLECTOR_LOG(WARN, "sub doc not in same doc, mainPK[%s], subPK[%s]", doc->GetPrimaryKey().c_str(),
                                subDocuments[i]->GetPrimaryKey().c_str());
            doc->RemoveSubDocument(i);
        } else {
            subDocuments[i]->SetDocId(subDocId);
        }
    }
    return true;
}

bool MainSubDocIdManager::ProcessUpdateDocument(const document::NormalDocumentPtr& doc)
{
    assert(doc->GetDocId() == INVALID_DOCID);
    assert(doc->GetDocOperateType() == UPDATE_FIELD);

    if (!MainSubDocIdManager::ValidateAndCorrectMainSubRelation(doc, _mainDocIdManager->GetPrimaryKeyReader(),
                                                                _subDocIdManager->GetPrimaryKeyReader(),
                                                                _mainToSubJoinAttributeReader.get())) {
        return false;
    }

    bool needUpdateMain = MainSubDocIdManager::NeedUpdate(doc, _schema->GetIndexSchema()->GetPrimaryKeyIndexFieldId());
    const document::NormalDocument::DocumentVector& subDocuments = doc->GetSubDocuments();
    if (subDocuments.size() == 0 && !needUpdateMain) {
        IE_LOG(WARN, "no field to update, OriginalOperateType [%d]", doc->GetOriginalOperateType());
        ERROR_COLLECTOR_LOG(WARN, "no field to update, OriginalOperateType [%d]", doc->GetOriginalOperateType());
        return false;
    }

    bool ret = true;
    if (!_mainDocIdManager->ProcessUpdateDocument(doc)) {
        ret = false;
    }
    for (const document::NormalDocumentPtr& subDoc : doc->GetSubDocuments()) {
        if (!_subDocIdManager->ProcessUpdateDocument(subDoc)) {
            ret = false;
        }
    }
    if (needUpdateMain && !_mainDocIdManager->ProcessUpdateDocument(doc)) {
        ret = false;
    }
    return ret;
}

bool MainSubDocIdManager::ValidateDeleteDocument(const document::NormalDocumentPtr& doc)
{
    return _mainDocIdManager->ValidateDeleteDocument(doc);
}

bool MainSubDocIdManager::ProcessDeleteDocument(const document::NormalDocumentPtr& doc)
{
    docid_t oldDeletedMainDocId = INVALID_DOCID;
    if (!_mainDocIdManager->ProcessDeleteDocument(doc, &oldDeletedMainDocId)) {
        return false;
    }
    assert(doc->GetDocId() == oldDeletedMainDocId);

    docid_t subDocIdBegin = INVALID_DOCID;
    docid_t subDocIdEnd = INVALID_DOCID;
    MainSubModifierUtil::GetSubDocIdRange(_mainToSubJoinAttributeReader.get(), oldDeletedMainDocId, &subDocIdBegin,
                                          &subDocIdEnd);

    for (docid_t subDocId = subDocIdBegin; subDocId < subDocIdEnd; ++subDocId) {
        _subDocIdManager->DoDeleteDocument(subDocId);
    }
    _mainDocIdManager->DoDeleteDocument(oldDeletedMainDocId);
    return true;
}

bool MainSubDocIdManager::ProcessDeleteSubDocument(const document::NormalDocumentPtr& doc)
{
    assert(doc->GetDocId() == INVALID_DOCID);
    assert(doc->GetDocOperateType() == DELETE_SUB_DOC);

    if (!MainSubDocIdManager::ValidateAndCorrectMainSubRelation(doc, _mainDocIdManager->GetPrimaryKeyReader(),
                                                                _subDocIdManager->GetPrimaryKeyReader(),
                                                                _mainToSubJoinAttributeReader.get())) {
        return false;
    }
    if (doc->GetSubDocuments().size() == 0) {
        return false;
    }

    bool ret = true;
    for (const document::NormalDocumentPtr& subDoc : doc->GetSubDocuments()) {
        if (!_subDocIdManager->ProcessDeleteDocument(subDoc)) {
            ret = false;
        }
    }
    return ret;
}

bool MainSubDocIdManager::ValidateAndCorrectAddDocument(const document::NormalDocumentPtr& doc)
{
    DedupSubDocs(doc);
    AddJoinFieldToMainDocument(doc);
    AddJoinFieldToSubDocuments(doc);

    if (!_mainDocIdManager->ValidateAddDocument(doc)) {
        return false;
    }

    const document::NormalDocument::DocumentVector& subDocs = doc->GetSubDocuments();
    for (int32_t i = (int32_t)subDocs.size() - 1; i >= 0; --i) {
        const document::NormalDocumentPtr& subDoc = subDocs[i];
        if (!_subDocIdManager->ValidateAddDocument(subDoc)) {
            IE_LOG(WARN, "add sub document fail! main doc pk[%s], sub doc pk[%s]",
                   doc->GetIndexDocument()->GetPrimaryKey().c_str(),
                   subDoc->HasPrimaryKey() ? subDoc->GetIndexDocument()->GetPrimaryKey().c_str()
                                           : std::string("pk empty").c_str());
            ERROR_COLLECTOR_LOG(WARN, "add sub document fail! main doc pk[%s], sub doc pk[%s]",
                                doc->GetIndexDocument()->GetPrimaryKey().c_str(),
                                subDoc->HasPrimaryKey() ? subDoc->GetIndexDocument()->GetPrimaryKey().c_str()
                                                        : std::string("pk empty").c_str());
            doc->RemoveSubDocument(i);
        }
    }
    return true;
}

void MainSubDocIdManager::DedupSubDocs(const document::NormalDocumentPtr& doc)
{
    // TODO: move to processor
    std::set<std::string> pkSet;
    const document::NormalDocument::DocumentVector& subDocs = doc->GetSubDocuments();
    for (int32_t i = (int32_t)subDocs.size() - 1; i >= 0; --i) {
        const document::NormalDocumentPtr& subDocument = subDocs[i];
        assert(subDocument);
        if (IsSubDocDuplicated(subDocument, pkSet)) {
            IE_LOG(WARN, "duplicate sub doc, mainPK[%s], subPK[%s]",
                   doc->HasPrimaryKey() ? doc->GetIndexDocument()->GetPrimaryKey().c_str()
                                        : std::string("pk empty").c_str(),
                   subDocument->HasPrimaryKey() ? subDocument->GetIndexDocument()->GetPrimaryKey().c_str()
                                                : std::string("sub pk empty").c_str());
            doc->RemoveSubDocument(i);
        } else {
            const std::string& subPrimaryKey = subDocument->GetIndexDocument()->GetPrimaryKey();
            pkSet.insert(subPrimaryKey);
        }
    }
}

bool MainSubDocIdManager::IsSubDocDuplicated(const document::NormalDocumentPtr& subDoc,
                                             const std::set<std::string>& pkSet)
{
    if (!subDoc->HasPrimaryKey()) {
        return true;
    }

    const std::string& subPrimaryKey = subDoc->GetIndexDocument()->GetPrimaryKey();
    if (pkSet.count(subPrimaryKey) > 0) {
        return true;
    }
    return false;
}

bool MainSubDocIdManager::ProcessAddDocument(const document::NormalDocumentPtr& doc)
{
    assert(doc->GetDocId() == INVALID_DOCID);

    // step 1: add main doc, not set main join
    docid_t deletedOldMainDocId = INVALID_DOCID;
    if (!_mainDocIdManager->ProcessAddDocument(doc, &deletedOldMainDocId)) {
        return false;
    }
    assert(doc->GetDocId() != INVALID_DOCID);

    if (deletedOldMainDocId != INVALID_DOCID) {
        // 保持和旧代码相同的dedup顺序：先子doc，再主doc
        docid_t subDocIdBegin = INVALID_DOCID;
        docid_t subDocIdEnd = INVALID_DOCID;
        MainSubModifierUtil::GetSubDocIdRange(_mainToSubJoinAttributeReader.get(), deletedOldMainDocId, &subDocIdBegin,
                                              &subDocIdEnd);
        for (docid_t subDocId = subDocIdBegin; subDocId < subDocIdEnd; ++subDocId) {
            _subDocIdManager->DedupDocument(subDocId);
        }
        _mainDocIdManager->DedupDocument(deletedOldMainDocId);
    }

    // step 2: add sub doc, set sub join
    for (const document::NormalDocumentPtr& subDoc : doc->GetSubDocuments()) {
        bool res = _subDocIdManager->ProcessAddDocument(subDoc);
        assert(res);
        (void)res;
    }

    // step 3: update main join
    UpdateMainDocumentJoinValue(doc, _subDocIdManager->GetNextLocalDocId());
    _mainToSubJoinAttributeReader->Insert(doc->GetDocId(), _subDocIdManager->GetNextGlobalDocId());
    return true;
}

void MainSubDocIdManager::AddJoinFieldToMainDocument(const document::NormalDocumentPtr& doc)
{
    std::string mainJoinValueStr = autil::StringUtil::toString(INVALID_DOCID);
    autil::StringView mainConvertedJoinValue =
        _mainJoinFieldConverter->Encode(autil::StringView(mainJoinValueStr), doc->GetPool());
    if (!doc->GetAttributeDocument()) {
        // TODO: no pool
        doc->SetAttributeDocument(document::AttributeDocumentPtr(new document::AttributeDocument));
    }
    const document::AttributeDocumentPtr& mainAttrDocument = doc->GetAttributeDocument();
    mainAttrDocument->SetField(_mainJoinFieldId, mainConvertedJoinValue);
}

void MainSubDocIdManager::UpdateMainDocumentJoinValue(const document::NormalDocumentPtr& doc, docid_t subDocIdEnd)
{
    std::string mainJoinValueStr = autil::StringUtil::toString(subDocIdEnd);
    autil::StringView mainConvertedJoinValue =
        _mainJoinFieldConverter->Encode(autil::StringView(mainJoinValueStr), doc->GetPool());
    const document::AttributeDocumentPtr& mainAttrDocument = doc->GetAttributeDocument();
    mainAttrDocument->SetField(_mainJoinFieldId, mainConvertedJoinValue);
}

void MainSubDocIdManager::AddJoinFieldToSubDocuments(const document::NormalDocumentPtr& doc)
{
    // TODO: optimize with encoder
    std::string subJoinValueStr = autil::StringUtil::toString(_mainDocIdManager->GetNextLocalDocId());

    for (const document::NormalDocumentPtr& subDocument : doc->GetSubDocuments()) {
        assert(subDocument);
        if (!subDocument->GetAttributeDocument()) {
            subDocument->SetAttributeDocument(document::AttributeDocumentPtr(new document::AttributeDocument));
        }

        const document::AttributeDocumentPtr& subAttrDocument = subDocument->GetAttributeDocument();
        autil::StringView subConvertedJoinValue =
            _subJoinFieldConverter->Encode(autil::StringView(subJoinValueStr), subDocument->GetPool());
        subAttrDocument->SetField(_subJoinFieldId, subConvertedJoinValue);
    }
}
}} // namespace indexlib::partition
