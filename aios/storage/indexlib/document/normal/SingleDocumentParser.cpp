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
#include "indexlib/document/normal/SingleDocumentParser.h"

#include <memory>

#include "indexlib/config/TabletSchema.h"
#include "indexlib/document/normal/ExtendDocFieldsConvertor.h"
#include "indexlib/document/normal/SummaryFormatter.h"
#include "indexlib/document/normal/rewriter/PackAttributeAppender.h"
#include "indexlib/document/normal/rewriter/SectionAttributeAppender.h"
#include "indexlib/index/attribute/Common.h"
#include "indexlib/index/common/Constant.h"
#include "indexlib/index/inverted_index/Common.h"
#include "indexlib/index/primary_key/Common.h"
#include "indexlib/index/summary/Common.h"
#include "indexlib/index/summary/config/SummaryIndexConfig.h"
#include "indexlib/util/DocTracer.h"
#include "indexlib/util/ErrorLogCollector.h"
#include "indexlib/util/counter/AccumulativeCounter.h"
#include "indexlib/util/counter/CounterMap.h"

using namespace std;
using namespace autil;
using namespace indexlibv2::config;
using namespace indexlib::util;
using namespace indexlib::document;

namespace indexlibv2 { namespace document {
AUTIL_LOG_SETUP(indexlib.document, SingleDocumentParser);

SingleDocumentParser::SingleDocumentParser() : _hasPrimaryKey(false) {}

SingleDocumentParser::~SingleDocumentParser() {}

bool SingleDocumentParser::Init(const shared_ptr<TabletSchema>& schema,
                                shared_ptr<AccumulativeCounter>& attrConvertErrorCounter)
{
    _schema = schema;

    const auto& pkConfig = _schema->GetPrimaryKeyIndexConfig();
    const auto& invertedConfigs = _schema->GetIndexConfigs(indexlib::index::INVERTED_INDEX_TYPE_STR);
    if (!pkConfig && invertedConfigs.empty()) {
        string errorMsg = "pk or inverted index config must be configured";
        ERROR_COLLECTOR_LOG(ERROR, "%s", errorMsg.c_str());
        AUTIL_LOG(ERROR, "%s", errorMsg.c_str());
        return false;
    }

    if (pkConfig) {
        const auto& fields = pkConfig->GetFieldConfigs();
        assert(fields.size() == 1);
        _primaryKeyFieldId = fields[0]->GetFieldId();
    }
    _nullFieldAppender.reset(new NullFieldAppender);
    if (!_nullFieldAppender->Init(_schema->GetFieldConfigs())) {
        _nullFieldAppender.reset();
    }

    _hasPrimaryKey = (pkConfig != nullptr);

    auto convertor = CreateExtendDocFieldsConvertor();
    if (!convertor) {
        ERROR_COLLECTOR_LOG(ERROR, "%s", "create extend doc fields convertor failed");
        AUTIL_LOG(ERROR, "%s", "create extend doc fields convertor failed");
        return false;
    }
    _fieldConvertPtr.reset(convertor.release());
    _fieldConvertPtr->init();
    shared_ptr<SectionAttributeAppender> appender(new SectionAttributeAppender);
    if (appender->Init(_schema)) {
        // do not use it directly, it is not thread-safe.
        _sectionAttrAppender = appender;
    }
    shared_ptr<PackAttributeAppender> packAttrAppender(new PackAttributeAppender());
    auto [status, ret] = packAttrAppender->Init(_schema);
    if (!status.IsOK()) {
        ERROR_COLLECTOR_LOG(ERROR, "%s", "pack attribute appender init failed");
        AUTIL_LOG(ERROR, "%s", "pack attribute appender init failed");
        return false;
    }
    if (ret) {
        _packAttrAppender = packAttrAppender;
    }

    auto insertFieldIds = [](const vector<shared_ptr<FieldConfig>>& fieldConfigs, set<fieldid_t>& s) {
        for (const auto& fieldConfig : fieldConfigs) {
            s.insert(fieldConfig->GetFieldId());
        }
    };
    insertFieldIds(_schema->GetIndexFieldConfigs(indexlibv2::index::PRIMARY_KEY_INDEX_TYPE_STR), _invertedFieldIds);
    insertFieldIds(_schema->GetIndexFieldConfigs(indexlib::index::INVERTED_INDEX_TYPE_STR), _invertedFieldIds);
    insertFieldIds(_schema->GetIndexFieldConfigs(indexlibv2::GENERALIZED_VALUE_INDEX_TYPE_STR), _attributeFieldIds);
    insertFieldIds(_schema->GetIndexFieldConfigs(indexlibv2::index::SUMMARY_INDEX_TYPE_STR), _summaryFieldIds);

    auto [status1, orderPreservingField] = _schema->GetSetting<std::string>("order_preserving_field");
    if (status1.IsOK()) {
        _orderPreservingField = orderPreservingField;
    } else if (!status1.IsNotFound()) {
        ERROR_COLLECTOR_LOG(ERROR, "get order_preserving_field from schema failed");
        AUTIL_LOG(ERROR, "get order_preserving_field from schema failed");
        return false;
    }

    if (!prepareIndexConfigMap()) {
        ERROR_COLLECTOR_LOG(ERROR, "prepare index config map failed");
        AUTIL_LOG(ERROR, "prepare index config map failed");
        return false;
    }

    _attributeConvertErrorCounter = attrConvertErrorCounter;
    return true;
}

bool SingleDocumentParser::prepareIndexConfigMap()
{
    assert(_schema);
    size_t fieldCount = _schema->GetFieldCount();
    _fieldIdToAttrConfigs.clear();
    _fieldIdToAttrConfigs.resize(fieldCount);
    auto configs = _schema->GetIndexConfigs(indexlibv2::index::ATTRIBUTE_INDEX_TYPE_STR);
    for (auto indexConfig : configs) {
        auto attrConfig = std::dynamic_pointer_cast<config::AttributeConfig>(indexConfig);
        if (!attrConfig) {
            AUTIL_LOG(ERROR, "index config[%s][%s] is not attribute config", indexConfig->GetIndexType().c_str(),
                      indexConfig->GetIndexName().c_str());
            return false;
        }
        fieldid_t fieldId = attrConfig->GetFieldId();
        assert(fieldId >= 0 && static_cast<size_t>(fieldId) < fieldCount);
        _fieldIdToAttrConfigs[fieldId] = attrConfig;
    }

    const auto& config =
        _schema->GetIndexConfig(indexlibv2::index::SUMMARY_INDEX_TYPE_STR, indexlibv2::index::SUMMARY_INDEX_NAME);
    if (config) {
        _summaryIndexConfig = std::dynamic_pointer_cast<config::SummaryIndexConfig>(config);
        assert(_summaryIndexConfig);
    }
    return true;
}

shared_ptr<NormalDocument> SingleDocumentParser::Parse(NormalExtendDocument* document)
{
    if (!document) {
        AUTIL_LOG(ERROR, "document is null");
        return nullptr;
    }
    const shared_ptr<RawDocument>& rawDoc = document->getRawDocument();
    if (!rawDoc) {
        AUTIL_LOG(ERROR, "empty raw document!");
        return nullptr;
    }
    if (rawDoc->getDocOperateType() == SKIP_DOC || rawDoc->getDocOperateType() == CHECKPOINT_DOC ||
        rawDoc->getDocOperateType() == UNKNOWN_OP) {
        return CreateDocument(document);
    }

    if (_nullFieldAppender) {
        _nullFieldAppender->Append(rawDoc);
    }

    SetPrimaryKeyField(document);
    if (document->getTokenizeDocument()->getFieldCount() == 0) {
        AUTIL_LOG(DEBUG, "tokenizeDoc is empty");
    }

    DocOperateType op = rawDoc->getDocOperateType();
    const auto& fieldConfigs = _schema->GetFieldConfigs();

    for (const auto& fieldConfig : fieldConfigs) {
        if ((op == DELETE_DOC || op == DELETE_SUB_DOC) && fieldConfig->GetFieldName() != _orderPreservingField) {
            continue;
        }

        fieldid_t fieldId = fieldConfig->GetFieldId();
        if (IsInvertedIndexField(fieldId)) {
            _fieldConvertPtr->convertIndexField(document, fieldConfig);
        }

        // TODO: indexlib need to fix this
        // use full schema to tell if its updatable(set to attr doc)
        if (IsAttributeIndexField(fieldId)) {
            if (rawDoc->getDocOperateType() == UPDATE_FIELD) {
                const auto& attributeConfig = GetAttributeConfig(fieldId);
                assert(attributeConfig);
                if (attributeConfig->IsAttributeUpdatable() && rawDoc->exist(attributeConfig->GetAttrName()) &&
                    _primaryKeyFieldId != fieldId) {
                    _fieldConvertPtr->convertAttributeField(document, fieldConfig);
                }
            } else {
                _fieldConvertPtr->convertAttributeField(document, fieldConfig);
            }
        } else if (IsSummaryIndexField(fieldId)) {
            if (rawDoc->getDocOperateType() != UPDATE_FIELD) {
                _fieldConvertPtr->convertSummaryField(document, fieldConfig);
            }
        }
    }

    const shared_ptr<ClassifiedDocument>& classifiedDocument = document->getClassifiedDocument();
    const shared_ptr<AttributeDocument>& attrDoc = classifiedDocument->getAttributeDoc();
    if (attrDoc && attrDoc->HasFormatError() && _attributeConvertErrorCounter) {
        _attributeConvertErrorCounter->Increase(1);
    }

    if (_summaryIndexConfig && _summaryIndexConfig->NeedStoreSummary() && op != DELETE_DOC && op != DELETE_SUB_DOC) {
        SummaryFormatter formatter(_summaryIndexConfig);
        auto status = classifiedDocument->serializeSummaryDocument(formatter);
        if (!status.IsOK()) {
            AUTIL_LOG(ERROR, "serialize summary document failed");
            return nullptr;
        }
    }
    if (_sectionAttrAppender && rawDoc->getDocOperateType() == ADD_DOC) {
        shared_ptr<SectionAttributeAppender> appender(_sectionAttrAppender->Clone());
        appender->AppendSectionAttribute(classifiedDocument->getIndexDocument());
    }
    if (_packAttrAppender && rawDoc->getDocOperateType() == ADD_DOC) {
        if (!_packAttrAppender->AppendPackAttribute(classifiedDocument->getAttributeDoc(),
                                                    classifiedDocument->getPool())) {
            IE_RAW_DOC_TRACE(rawDoc, "parse error: append packAttribute failed.");
        }
    }

    if (!Validate(document)) {
        return shared_ptr<NormalDocument>();
    }
    return CreateDocument(document);
}

void SingleDocumentParser::SetPrimaryKeyField(NormalExtendDocument* document)
{
    const auto& fields = _schema->GetIndexFieldConfigs(indexlibv2::index::PRIMARY_KEY_INDEX_TYPE_STR);
    if (fields.size() != 1) {
        return;
    }
    const string& pkFieldName = fields[0]->GetFieldName();
    if (pkFieldName.empty()) {
        return;
    }

    const shared_ptr<RawDocument>& rawDoc = document->getRawDocument();
    const shared_ptr<ClassifiedDocument>& classifiedDoc = document->getClassifiedDocument();
    string pkValue = rawDoc->getField(pkFieldName);
    document->setIdentifier(pkValue);

    AUTIL_LOG(TRACE3, "the primary key is:%s", rawDoc->toString().c_str());
    classifiedDoc->setPrimaryKey(pkValue);
}

void SingleDocumentParser::AddModifiedFields(const NormalExtendDocument* document,
                                             const shared_ptr<NormalDocument>& indexDoc)
{
    const NormalExtendDocument::FieldIdSet& modifiedFieldSet = document->getModifiedFieldSet();
    for (NormalExtendDocument::FieldIdSet::const_iterator it = modifiedFieldSet.begin(); it != modifiedFieldSet.end();
         ++it) {
        indexDoc->AddModifiedField(*it);
    }
}

shared_ptr<NormalDocument> SingleDocumentParser::CreateDocument(const NormalExtendDocument* document)
{
    const shared_ptr<ClassifiedDocument>& classifiedDoc = document->getClassifiedDocument();
    shared_ptr<NormalDocument> indexDoc(new NormalDocument(classifiedDoc->getPoolPtr()));
    indexDoc->SetIndexDocument(classifiedDoc->getIndexDocument());
    indexDoc->SetSummaryDocument(classifiedDoc->getSerSummaryDoc());
    indexDoc->SetAttributeDocument(classifiedDoc->getAttributeDoc());
    // TODO: support source
    // indexDoc->SetSourceDocument(
    //     classifiedDoc->getSerializedSourceDocument(_schema->GetSourceSchema(), indexDoc->GetPool()));

    classifiedDoc->clear();

    // TODO: add ut check modified fields
    AddModifiedFields(document, indexDoc);
    return indexDoc;
}

bool SingleDocumentParser::Validate(const NormalExtendDocument* document)
{
    const shared_ptr<RawDocument>& rawDocumentPtr = document->getRawDocument();
    if (rawDocumentPtr == NULL) {
        AUTIL_LOG(WARN, "raw document is NULL");
        return false;
    }
    // for alter index, read raw doc only part fields
    if (rawDocumentPtr->ignoreEmptyField()) {
        return true;
    }

    DocOperateType opType = rawDocumentPtr->getDocOperateType();
    const shared_ptr<ClassifiedDocument>& classifiedDocument = document->getClassifiedDocument();
    if (_hasPrimaryKey && classifiedDocument->getPrimaryKey().empty()) {
        AUTIL_INTERVAL_LOG2(60, WARN, "primary key is empty");
        IE_RAW_DOC_TRACE(rawDocumentPtr, "parse error: primary key is empty");
        return false;
    }

    if (opType == ADD_DOC) {
        if (classifiedDocument->getIndexDocument()->GetFieldCount() == 0 && !_hasPrimaryKey) {
            AUTIL_LOG(WARN, "add doc without index field");
            IE_RAW_DOC_TRACE(rawDocumentPtr, "parse error: lack of index field");
            return false;
        }
    } else if (opType == UPDATE_FIELD || opType == DELETE_DOC || opType == DELETE_SUB_DOC) {
        if (!_hasPrimaryKey) {
            AUTIL_LOG(WARN, "update field or delete doc without primary key");
            IE_RAW_DOC_TRACE(rawDocumentPtr, "parse error: update field or delete doc without primary key");
            return false;
        }
    } else {
        // other type.
        stringstream ss;
        ss << "Not support doc_operation: [" << opType << "]";
        string errorMsg = ss.str();
        AUTIL_LOG(WARN, "%s", errorMsg.c_str());
        IE_RAW_DOC_TRACE(rawDocumentPtr, errorMsg);
        return false;
    }
    return true;
}

const std::shared_ptr<indexlibv2::config::AttributeConfig>&
SingleDocumentParser::GetAttributeConfig(fieldid_t fieldId) const
{
    assert(fieldId >= 0 && static_cast<size_t>(fieldId) < _fieldIdToAttrConfigs.size());
    return _fieldIdToAttrConfigs[fieldId];
}

std::unique_ptr<ExtendDocFieldsConvertor> SingleDocumentParser::CreateExtendDocFieldsConvertor() const
{
    return std::make_unique<ExtendDocFieldsConvertor>(_schema);
}

}} // namespace indexlibv2::document
