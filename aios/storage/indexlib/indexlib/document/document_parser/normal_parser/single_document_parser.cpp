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
#include "indexlib/document/document_parser/normal_parser/single_document_parser.h"

#include "indexlib/config/index_partition_schema.h"
#include "indexlib/document/document_parser/normal_parser/multi_region_extend_doc_fields_convertor.h"
#include "indexlib/document/document_rewriter/multi_region_pack_attribute_appender.h"
#include "indexlib/document/document_rewriter/section_attribute_appender.h"
#include "indexlib/document/index_document/normal_document/summary_formatter.h"
#include "indexlib/misc/doc_tracer.h"
#include "indexlib/util/counter/AccumulativeCounter.h"
#include "indexlib/util/counter/CounterMap.h"

using namespace std;
using namespace autil;
using namespace indexlib::config;
using namespace indexlib::common;

using namespace indexlib::util;
namespace indexlib { namespace document {
IE_LOG_SETUP(document, SingleDocumentParser);

SingleDocumentParser::SingleDocumentParser() : mHasPrimaryKey(false) {}

SingleDocumentParser::~SingleDocumentParser() {}

bool SingleDocumentParser::Init(const IndexPartitionSchemaPtr& schema, AccumulativeCounterPtr& attrConvertErrorCounter)
{
    mSchema = schema;
    if (mSchema->GetTableType() == tt_kv || mSchema->GetTableType() == tt_kkv ||
        mSchema->GetTableType() == tt_linedata) {
        ERROR_COLLECTOR_LOG(ERROR,
                            "init SingleDocumentParser fail, "
                            "unsupport tableType [%s]!",
                            IndexPartitionSchema::TableType2Str(mSchema->GetTableType()).c_str());
        return false;
    }

    const IndexSchemaPtr& indexSchemaPtr = mSchema->GetIndexSchema();
    if (!indexSchemaPtr) {
        string errorMsg = "indexSchema must be configured";
        ERROR_COLLECTOR_LOG(ERROR, "%s", errorMsg.c_str());
        return false;
    }

    mNullFieldAppender.reset(new NullFieldAppender);
    if (!mNullFieldAppender->Init(schema->GetFieldSchema())) {
        mNullFieldAppender.reset();
    }

    mHasPrimaryKey = indexSchemaPtr->HasPrimaryKeyIndex();

    mFieldConvertPtr.reset(new MultiRegionExtendDocFieldsConvertor(mSchema));
    SectionAttributeAppenderPtr appender(new SectionAttributeAppender);
    if (appender->Init(mSchema)) {
        // do not use it directly, it is not thread-safe.
        mSectionAttrAppender = appender;
    }
    MultiRegionPackAttributeAppenderPtr packAttrAppender(new MultiRegionPackAttributeAppender());
    if (packAttrAppender->Init(mSchema)) {
        mPackAttrAppender = packAttrAppender;
    }

    mAttributeConvertErrorCounter = attrConvertErrorCounter;
    return true;
}

NormalDocumentPtr SingleDocumentParser::Parse(const IndexlibExtendDocumentPtr& document)
{
    const RawDocumentPtr& rawDoc = document->getRawDocument();
    if (!rawDoc) {
        IE_LOG(ERROR, "empty raw document!");
        return NormalDocumentPtr();
    }
    if (rawDoc->getDocOperateType() == SKIP_DOC || rawDoc->getDocOperateType() == CHECKPOINT_DOC ||
        rawDoc->getDocOperateType() == UNKNOWN_OP) {
        return CreateDocument(document);
    }

    if (mNullFieldAppender) {
        mNullFieldAppender->Append(rawDoc);
    }

    regionid_t regionId = document->getRegionId();
    const FieldSchemaPtr& fieldSchema = mSchema->GetFieldSchema(regionId);
    const IndexSchemaPtr& indexSchema = mSchema->GetIndexSchema(regionId);
    const AttributeSchemaPtr& attrSchema = mSchema->GetAttributeSchema(regionId);
    const SummarySchemaPtr& summarySchema = mSchema->GetSummarySchema(regionId);

    assert(indexSchema);
    SetPrimaryKeyField(document, indexSchema, regionId);
    if (document->getTokenizeDocument()->getFieldCount() == 0) {
        IE_LOG(DEBUG, "tokenizeDoc is empty while exists index schema");
    }

    DocOperateType op = rawDoc->getDocOperateType();
    string orderPreservingField;
    const RegionSchemaPtr& regionSchema = mSchema->GetRegionSchema(regionId);
    if (regionSchema) {
        orderPreservingField = regionSchema->GetOrderPreservingFieldName();
    }
    for (FieldSchema::Iterator it = fieldSchema->Begin(); it != fieldSchema->End(); ++it) {
        if ((*it)->IsDeleted()) {
            continue;
        }

        if ((op == DELETE_DOC || op == DELETE_SUB_DOC) && (*it)->GetFieldName() != orderPreservingField) {
            continue;
        }

        fieldid_t fieldId = (*it)->GetFieldId();

        if (indexSchema->IsInIndex(fieldId)) {
            mFieldConvertPtr->convertIndexField(document, *it);
        }

        // TODO: indexlib need to fix this
        // use full schema to tell if its updatable(set to attr doc)
        if (attrSchema && attrSchema->IsInAttribute(fieldId)) {
            if (rawDoc->getDocOperateType() == UPDATE_FIELD) {
                const AttributeConfigPtr& attributeConfig = attrSchema->GetAttributeConfigByFieldId(fieldId);
                if (attributeConfig->IsAttributeUpdatable() && rawDoc->exist(attributeConfig->GetAttrName()) &&
                    indexSchema->GetPrimaryKeyIndexFieldId() != fieldId) {
                    mFieldConvertPtr->convertAttributeField(document, *it);
                }
            } else {
                mFieldConvertPtr->convertAttributeField(document, *it);
            }
        } else if (summarySchema && summarySchema->IsInSummary(fieldId)) {
            if (rawDoc->getDocOperateType() != UPDATE_FIELD) {
                mFieldConvertPtr->convertSummaryField(document, *it);
            }
        }
    }

    const ClassifiedDocumentPtr& classifiedDocument = document->getClassifiedDocument();
    const AttributeDocumentPtr& attrDoc = classifiedDocument->getAttributeDoc();
    if (attrDoc && attrDoc->HasFormatError() && mAttributeConvertErrorCounter) {
        mAttributeConvertErrorCounter->Increase(1);
    }
    if (mSchema->NeedStoreSummary() && op != DELETE_DOC && op != DELETE_SUB_DOC) {
        SummaryFormatter formatter(summarySchema);
        classifiedDocument->serializeSummaryDocument(formatter);
    }
    if (mSectionAttrAppender && rawDoc->getDocOperateType() == ADD_DOC) {
        SectionAttributeAppenderPtr appender(mSectionAttrAppender->Clone());
        appender->AppendSectionAttribute(classifiedDocument->getIndexDocument());
    }
    if (mPackAttrAppender && rawDoc->getDocOperateType() == ADD_DOC) {
        if (!mPackAttrAppender->AppendPackAttribute(classifiedDocument->getAttributeDoc(),
                                                    classifiedDocument->getPool(), regionId)) {
            IE_RAW_DOC_TRACE(rawDoc, "parse error: append packAttribute failed.");
        }
    }

    if (!Validate(document)) {
        return NormalDocumentPtr();
    }
    return CreateDocument(document);
}

void SingleDocumentParser::SetPrimaryKeyField(const IndexlibExtendDocumentPtr& document,
                                              const IndexSchemaPtr& indexSchema, regionid_t regionId)
{
    const string& pkFieldName = indexSchema->GetPrimaryKeyIndexFieldName();
    if (pkFieldName.empty()) {
        return;
    }

    const RawDocumentPtr& rawDoc = document->getRawDocument();
    const ClassifiedDocumentPtr& classifiedDoc = document->getClassifiedDocument();
    string pkValue = rawDoc->getField(pkFieldName);
    document->setIdentifier(pkValue);

    IE_LOG(TRACE3, "the primary key is:%s", rawDoc->toString().c_str());
    classifiedDoc->setPrimaryKey(pkValue);
}

void SingleDocumentParser::AddModifiedFields(const IndexlibExtendDocumentPtr& document,
                                             const NormalDocumentPtr& indexDoc)
{
    const IndexlibExtendDocument::FieldIdSet& modifiedFieldSet = document->getModifiedFieldSet();
    for (IndexlibExtendDocument::FieldIdSet::const_iterator it = modifiedFieldSet.begin(); it != modifiedFieldSet.end();
         ++it) {
        indexDoc->AddModifiedField(*it);
    }

    const IndexlibExtendDocument::FieldIdSet& subModifiedFieldSet = document->getSubModifiedFieldSet();
    for (IndexlibExtendDocument::FieldIdSet::const_iterator it = subModifiedFieldSet.begin();
         it != subModifiedFieldSet.end(); ++it) {
        indexDoc->AddSubModifiedField(*it);
    }
}

NormalDocumentPtr SingleDocumentParser::CreateDocument(const IndexlibExtendDocumentPtr& document)
{
    const ClassifiedDocumentPtr& classifiedDoc = document->getClassifiedDocument();
    NormalDocumentPtr indexDoc(new NormalDocument(classifiedDoc->getPoolPtr()));
    indexDoc->SetIndexDocument(classifiedDoc->getIndexDocument());
    indexDoc->SetSummaryDocument(classifiedDoc->getSerSummaryDoc());
    indexDoc->SetAttributeDocument(classifiedDoc->getAttributeDoc());
    indexDoc->SetSourceDocument(createSerializedSourceDocument(classifiedDoc->getSourceDocument(),
                                                               mSchema->GetSourceSchema(), indexDoc->GetPool()));

    classifiedDoc->clear();

    // TODO: add ut check modified fields
    AddModifiedFields(document, indexDoc);
    return indexDoc;
}

bool SingleDocumentParser::Validate(const IndexlibExtendDocumentPtr& document)
{
    const RawDocumentPtr& rawDocumentPtr = document->getRawDocument();
    if (rawDocumentPtr == NULL) {
        IE_LOG(WARN, "raw document is NULL");
        return false;
    }
    // for alter index, read raw doc only part fields
    if (rawDocumentPtr->ignoreEmptyField()) {
        return true;
    }

    DocOperateType opType = rawDocumentPtr->getDocOperateType();
    const ClassifiedDocumentPtr& classifiedDocument = document->getClassifiedDocument();
    if (mHasPrimaryKey && classifiedDocument->getPrimaryKey().empty()) {
        IE_INTERVAL_LOG2(60, WARN, "primary key is empty");
        IE_RAW_DOC_TRACE(rawDocumentPtr, "parse error: primary key is empty");
        return false;
    }

    if (opType == ADD_DOC) {
        if (classifiedDocument->getIndexDocument()->GetFieldCount() == 0 && !mHasPrimaryKey) {
            IE_LOG(WARN, "add doc without index field");
            IE_RAW_DOC_TRACE(rawDocumentPtr, "parse error: lack of index field");
            return false;
        }
    } else if (opType == UPDATE_FIELD || opType == DELETE_DOC || opType == DELETE_SUB_DOC) {
        if (!mHasPrimaryKey) {
            IE_LOG(WARN, "update field or delete doc without primary key");
            IE_RAW_DOC_TRACE(rawDocumentPtr, "parse error: update field or delete doc without primary key");
            return false;
        }
    } else {
        // other type.
        stringstream ss;
        ss << "Not support doc_operation: [" << opType << "]";
        string errorMsg = ss.str();
        IE_LOG(WARN, "%s", errorMsg.c_str());
        IE_RAW_DOC_TRACE(rawDocumentPtr, errorMsg);
        return false;
    }
    return true;
}
}} // namespace indexlib::document
