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
#include "indexlib/partition/normal_doc_id_manager.h"

#include "indexlib/config/index_partition_schema.h"
#include "indexlib/config/primary_key_index_config.h"
#include "indexlib/document/index_document/normal_document/attribute_document_field_extractor.h"
#include "indexlib/document/index_document/normal_document/normal_document.h"
#include "indexlib/index/common/KeyHasherWrapper.h"
#include "indexlib/index/normal/attribute/default_attribute_field_appender.h"
#include "indexlib/index/normal/primarykey/composite_primary_key_reader.h"
#include "indexlib/index/partition_info.h"
#include "indexlib/index/primary_key/PrimaryKeyIndexReader.h"
#include "indexlib/index_base/partition_data.h"
#include "indexlib/partition/modifier/partition_modifier.h"
#include "indexlib/partition/segment/single_segment_writer.h"
#include "indexlib/partition/segment/sub_doc_segment_writer.h"

namespace indexlib { namespace partition {
IE_LOG_SETUP(partition, NormalDocIdManager);

NormalDocIdManager::NormalDocIdManager(const config::IndexPartitionSchemaPtr& schema, bool enableAutoAdd2Update)
    : _schema(schema)
    , _enableAutoAdd2Update(enableAutoAdd2Update)
    , _enableInsertOrIgnore(false)
    , _buildMode(PartitionWriter::BUILD_MODE_STREAM)
    , _delayDedupDocument(false)
    , _compositePrimaryKeyReader(new index::CompositePrimaryKeyReader())
{
    assert(_schema);

    _defaultValueAppender.reset(new index::DefaultAttributeFieldAppender);
    _defaultValueAppender->Init(_schema, index_base::InMemorySegmentReaderPtr());

    _enableInsertOrIgnore = _schema->SupportInsertOrIgnore();
    if (_enableAutoAdd2Update) {
        _attrFieldExtractor.reset(new document::AttributeDocumentFieldExtractor());
        _attrFieldExtractor->Init(_schema->GetAttributeSchema());
    }
}

NormalDocIdManager::~NormalDocIdManager() {}

void NormalDocIdManager::Reinit(const index_base::PartitionDataPtr& partitionData,
                                const PartitionModifierPtr& partitionModifier,
                                const index_base::SegmentWriterPtr& segmentWriter, PartitionWriter::BuildMode buildMode,
                                bool delayDedupDocument)
{
    assert(not(_enableAutoAdd2Update && !partitionModifier->SupportAutoAdd2Update()));
    _buildMode = buildMode;
    _delayDedupDocument = delayDedupDocument;
    _modifier = partitionModifier;
    _segmentWriter = DYNAMIC_POINTER_CAST(SingleSegmentWriter, segmentWriter);
    _partitionInfo = partitionData->GetPartitionInfo();
    _baseDocId = (_modifier == nullptr ? 0 : _modifier->GetBuildingSegmentBaseDocId()) +
                 (_segmentWriter == nullptr ? 0 : _segmentWriter->GetSegmentInfo()->docCount);
    _compositePrimaryKeyReader->Reinit(partitionModifier->GetPrimaryKeyIndexReader());
}

void NormalDocIdManager::TEST_Init(const std::shared_ptr<index::PrimaryKeyIndexReader>& primaryKeyIndexReader,
                                   const index::PartitionInfoPtr& partitionInfo, docid_t baseDocId,
                                   const PartitionModifierPtr& modifier, const SingleSegmentWriterPtr& segmentWriter,
                                   PartitionWriter::BuildMode buildMode, bool delayDedupDocument)
{
    _buildMode = buildMode;
    _delayDedupDocument = delayDedupDocument;
    _modifier = modifier;
    _segmentWriter = segmentWriter;
    _partitionInfo = partitionInfo;
    _baseDocId = baseDocId;
    _compositePrimaryKeyReader->Reinit(primaryKeyIndexReader);
}

docid_t NormalDocIdManager::GetNextGlobalDocId() const { return _baseDocId; }
docid_t NormalDocIdManager::GetNextLocalDocId() const { return _baseDocId - _modifier->GetBuildingSegmentBaseDocId(); }

bool NormalDocIdManager::Process(const document::DocumentPtr& document)
{
    document::NormalDocumentPtr doc = DYNAMIC_POINTER_CAST(document::NormalDocument, document);
    // assert(doc->GetDocId() == INVALID_DOCID);  // for som ut
    doc->SetDocId(INVALID_DOCID);

    DocOperateType opType = document->GetDocOperateType();
    switch (opType) {
    case ADD_DOC: {
        return ValidateAddDocument(doc) && ProcessAddDocument(doc);
    }
    case DELETE_DOC: {
        return ValidateDeleteDocument(doc) && ProcessDeleteDocument(doc);
    }
    case UPDATE_FIELD: {
        return ValidateUpdateDocument(doc) && ProcessUpdateDocument(doc);
    }
    default:
        return true;
    }
}

bool NormalDocIdManager::ValidateUpdateDocument(const document::NormalDocumentPtr& doc)
{
    if (!doc->HasPrimaryKey() || (doc->GetAttributeDocument() == nullptr and doc->GetIndexDocument() == nullptr)) {
        IE_LOG(WARN, "update document is invalid, IGNORE.");
        return false;
    }
    bool attributeUpdatable = (_schema->GetAttributeSchema() != nullptr);
    bool indexUpdatable = ((_schema->GetIndexSchema() != nullptr) and _schema->GetIndexSchema()->AnyIndexUpdatable());

    if (!attributeUpdatable and !indexUpdatable) {
        IE_LOG(WARN, "attribute and index not updatable.");
        return false;
    }
    return true;
}

bool NormalDocIdManager::ProcessUpdateDocument(const document::NormalDocumentPtr& doc)
{
    // 从 AutoAdd2Update 或 MainSubDocIdManager 过来的 UpdateDoc 已经填入了 docId
    if (doc->GetDocId() == INVALID_DOCID) {
        const std::string& pkString = doc->GetIndexDocument()->GetPrimaryKey();
        docid_t docId = LookupPrimaryKey(pkString);
        if (docId == INVALID_DOCID) {
            IE_LOG(TRACE1, "target update document [pk:%s] is not exist!", pkString.c_str());
            return false;
        }
        doc->SetDocId(docId);
    }
    doc->SetSegmentIdBeforeModified(_partitionInfo->GetSegmentId(doc->GetDocId()));
    return true;
}

bool NormalDocIdManager::ValidateDeleteDocument(const document::NormalDocumentPtr& doc)
{
    if (!doc->HasPrimaryKey()) {
        return false;
    }
    return true;
}

bool NormalDocIdManager::ProcessDeleteDocument(const document::NormalDocumentPtr& doc, docid_t* deletedOldDocId)
{
    // assert(doc->GetDocOperateType() == DELETE_DOC);  // may be MainSubDocIdManager delete sub doc

    const std::string& pkString = doc->GetIndexDocument()->GetPrimaryKey();

    docid_t docId = LookupPrimaryKey(pkString);
    if (docId == INVALID_DOCID) {
        IE_LOG(TRACE1, "target update document [pk:%s] is not exist!", pkString.c_str());
        return false;
    }
    doc->SetDocId(docId);
    doc->SetSegmentIdBeforeModified(_partitionInfo->GetSegmentId(docId));

    if (deletedOldDocId != nullptr) {
        *deletedOldDocId = doc->GetDocId();
        return true;
    } else {
        return DoDeleteDocument(doc->GetDocId());
    }
}

bool NormalDocIdManager::DoDeleteDocument(docid_t docId)
{
    if (_buildMode != PartitionWriter::BUILD_MODE_STREAM) {
        _compositePrimaryKeyReader->RemoveDocument(docId);
        // 非 batchMode 由外部负责删除和记 op log
        return _modifier->RemoveDocument(docId);
    }
    return true;
}

bool NormalDocIdManager::DedupDocument(docid_t docId)
{
    if (_buildMode != PartitionWriter::BUILD_MODE_STREAM) {
        _compositePrimaryKeyReader->RemoveDocument(docId);
    }
    return _modifier->RemoveDocument(docId);
}

bool NormalDocIdManager::ValidateAddDocument(const document::NormalDocumentPtr& doc)
{
    if (doc->GetIndexDocument() == nullptr) {
        IE_LOG(WARN, "AddDocument fail: Doc has no index document");
        ERROR_COLLECTOR_LOG(WARN, "AddDocument fail: Doc has no index document");
        return false;
    }
    if (_schema->GetIndexSchema()->HasPrimaryKeyIndex()) {
        if (!doc->HasPrimaryKey()) {
            IE_LOG(WARN, "AddDocument fail: Doc has no primary key");
            ERROR_COLLECTOR_LOG(WARN, "AddDocument fail: Doc has no primary key");
            return false;
        }
        const std::string& keyStr = doc->GetPrimaryKey();
        if (!IsPrimaryKeyStrValid(keyStr, _schema)) {
            IE_LOG(WARN, "AddDocument fail: Doc primary key [%s] is not valid", keyStr.c_str());
            ERROR_COLLECTOR_LOG(WARN, "AddDocument fail: Doc primary key [%s] is not valid", keyStr.c_str());
            return false;
        }
    }
    if (_schema->NeedStoreSummary() and doc->GetSummaryDocument() == nullptr) {
        IE_LOG(WARN, "AddDocument fail: Doc has no summary document");
        ERROR_COLLECTOR_LOG(WARN, "AddDocument fail: Doc has no summary document");
        return false;
    }

    if (_schema->GetAttributeSchema() and !CheckAttributeDocument(doc->GetAttributeDocument(), _schema)) {
        return false;
    }

    return true;
}

bool NormalDocIdManager::IsPrimaryKeyStrValid(const std::string& str, const config::IndexPartitionSchemaPtr& schema)
{
    const config::SingleFieldIndexConfigPtr& indexConfig = schema->GetIndexSchema()->GetPrimaryKeyIndexConfig();
    if (indexConfig == nullptr) {
        assert(false);
        return true;
    }
    InvertedIndexType indexType = indexConfig->GetInvertedIndexType();
    if (indexType == it_trie) {
        return true;
    }
    if (indexType == it_primarykey64 or indexType == it_primarykey128) {
        config::PrimaryKeyIndexConfigPtr primaryKeyIndexConfig =
            DYNAMIC_POINTER_CAST(config::PrimaryKeyIndexConfig, indexConfig);
        if (primaryKeyIndexConfig == nullptr) {
            assert(false);
            return false;
        }
        return index::KeyHasherWrapper::IsOriginalKeyValid(primaryKeyIndexConfig->GetFieldConfig()->GetFieldType(),
                                                           primaryKeyIndexConfig->GetPrimaryKeyHashType(), str.c_str(),
                                                           str.size(), indexType == it_primarykey64);
    }
    assert(false);
    return false;
}

bool NormalDocIdManager::CheckAttributeDocument(const document::AttributeDocumentPtr& attrDoc,
                                                const config::IndexPartitionSchemaPtr& schema)
{
    if (attrDoc == NULL) {
        IE_LOG(WARN, "AddDocument fail: Doc has no attribute document");
        ERROR_COLLECTOR_LOG(WARN, "AddDocument fail: Doc has no attribute document");
        return false;
    }

    config::AttributeSchemaPtr attrSchema = schema->GetAttributeSchema();
    size_t schemaPackFieldCount = attrSchema->GetPackAttributeCount();
    if (schemaPackFieldCount == 0) {
        return true;
    }

    size_t docPackFieldCount = attrDoc->GetPackFieldCount();
    if (docPackFieldCount != schemaPackFieldCount) {
        IE_LOG(ERROR, "AddDocument fail: doc pack attribute fields count [%lu] do not match schema [%lu]",
               docPackFieldCount, schemaPackFieldCount);
        ERROR_COLLECTOR_LOG(ERROR, "AddDocument fail: doc pack attribute fields count [%lu] do not match schema [%lu]",
                            docPackFieldCount, schemaPackFieldCount);
        return false;
    }
    for (size_t i = 0; i < docPackFieldCount; ++i) {
        const autil::StringView& packField = attrDoc->GetPackField(packattrid_t(i));
        if (packField.empty()) {
            IE_LOG(ERROR, "AddDocument fail: pack attribute format error");
            ERROR_COLLECTOR_LOG(ERROR, "AddDocument fail: pack attribute format error");
            return false;
        }
    }
    return true;
}

bool NormalDocIdManager::ProcessAddDocument(const document::NormalDocumentPtr& doc, docid_t* deletedOldDocId)
{
    _defaultValueAppender->AppendDefaultFieldValues(doc);

    const std::string& pkString = doc->GetIndexDocument()->GetPrimaryKey();

    if (_enableInsertOrIgnore) {
        docid_t oldDocId = LookupPrimaryKey(pkString);
        if (oldDocId != INVALID_DOCID) {
            doc->SetDocOperateType(SKIP_DOC);
            return true;
        }
    } else if (_enableAutoAdd2Update) {
        // 主子表不支持 autoAdd2Update，无需考虑
        docid_t oldDocId = LookupPrimaryKey(pkString);
        if (oldDocId != INVALID_DOCID) {
            RewriteDocumentForAutoAdd2Update(doc);
            doc->SetDocOperateType(UPDATE_FIELD);
            doc->SetDocId(oldDocId);
            return ProcessUpdateDocument(doc);
        }
    } else if (!_delayDedupDocument) {
        docid_t oldDocId = LookupPrimaryKey(pkString);
        if (oldDocId != INVALID_DOCID) {
            // For op log. 老逻辑中，该实现在 DedupDocument() -> RemoveDocument() 中
            doc->SetSegmentIdBeforeModified(_partitionInfo->GetSegmentId(oldDocId));
            if (deletedOldDocId != nullptr) {
                *deletedOldDocId = oldDocId;
            } else {
                bool isRemoveSuccess = DedupDocument(oldDocId);
                assert(isRemoveSuccess);
                (void)isRemoveSuccess;
            }
        }
    }

    docid_t docid = _baseDocId;
    doc->SetDocId(docid);
    ++_baseDocId;
    if (_buildMode != PartitionWriter::BUILD_MODE_STREAM) {
        _compositePrimaryKeyReader->InsertOrUpdate(pkString, docid);
        _segmentWriter->EndAddDocument(doc);
    }
    return true;
}

void NormalDocIdManager::RewriteDocumentForAutoAdd2Update(const document::NormalDocumentPtr& doc)
{
    const document::AttributeDocumentPtr& attrDoc = doc->GetAttributeDocument();
    autil::mem_pool::Pool* pool = doc->GetPool();

    const config::FieldSchemaPtr& fieldSchema = _schema->GetFieldSchema();
    const config::AttributeSchemaPtr& attrSchema = _schema->GetAttributeSchema();
    for (config::FieldSchema::Iterator it = fieldSchema->Begin(); it != fieldSchema->End(); ++it) {
        if ((*it)->IsDeleted()) {
            continue;
        }

        fieldid_t fieldId = (*it)->GetFieldId();
        if (!attrSchema->IsInAttribute(fieldId)) {
            continue;
        }
        if (attrDoc->HasField(fieldId)) {
            continue;
        }
        autil::StringView fieldValue = _attrFieldExtractor->GetField(attrDoc, fieldId, pool);
        if (fieldValue.empty()) {
            continue;
        }
        attrDoc->SetField(fieldId, fieldValue);
    }
}

index::CompositePrimaryKeyReader* NormalDocIdManager::GetPrimaryKeyReader() { return _compositePrimaryKeyReader.get(); }

docid_t NormalDocIdManager::LookupPrimaryKey(const std::string& pk)
{
    if (_buildMode != PartitionWriter::BUILD_MODE_STREAM) {
        return _compositePrimaryKeyReader->Lookup(pk);
    }
    return _compositePrimaryKeyReader->GetPrimaryKeyIndexReader()->Lookup(pk);
}

}} // namespace indexlib::partition
