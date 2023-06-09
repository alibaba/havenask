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
#include "indexlib/document/normal/NormalDocumentParser.h"

#include "autil/EnvUtil.h"
#include "indexlib/config/FieldConfig.h"
#include "indexlib/config/TabletSchema.h"
#include "indexlib/document/BuiltinParserInitParam.h"
#include "indexlib/document/DocumentBatch.h"
#include "indexlib/document/RawDocumentDefine.h"
#include "indexlib/document/document_rewriter/IDocumentRewriter.h"
#include "indexlib/document/normal/NormalDocument.h"
#include "indexlib/document/normal/NormalExtendDocument.h"
#include "indexlib/document/normal/SingleDocumentParser.h"
#include "indexlib/util/ErrorLogCollector.h"
#include "indexlib/util/counter/CounterMap.h"

namespace indexlibv2::document {
AUTIL_LOG_SETUP(indexlib.document, NormalDocumentParser);

NormalDocumentParser::NormalDocumentParser(const std::shared_ptr<analyzer::IAnalyzerFactory>& analyzerFactory,
                                           bool needParseRawDoc)
    : _analyzerFactory(analyzerFactory)
    , _needParseRawDoc(needParseRawDoc)
{
    _enableModifyFieldStat = autil::EnvUtil::getEnv("INDEXLIB_ENABLE_STAT_MODIFY_FIELDS", _enableModifyFieldStat);
}

NormalDocumentParser::~NormalDocumentParser() {}

Status NormalDocumentParser::Init(const std::shared_ptr<config::TabletSchema>& schema,
                                  const std::shared_ptr<DocumentInitParam>& initParam)
{
    if (!schema) {
        ERROR_COLLECTOR_LOG(ERROR, "schema is nullptr");
        RETURN_IF_STATUS_ERROR(Status::InvalidArgs(), "schema is nullptr");
    }
    _schema = schema;
    const auto& fieldConfigs = _schema->GetFieldConfigs();
    for (const auto& fieldConfig : fieldConfigs) {
        if (fieldConfig->IsEnableNullField()) {
            _supportNull = true;
            break;
        }
    }
    if (_needParseRawDoc) {
        _tokenizeDocConvertor = std::make_shared<TokenizeDocumentConvertor>();
        auto status = _tokenizeDocConvertor->Init(schema, _analyzerFactory.get());
        if (!status.IsOK()) {
            ERROR_COLLECTOR_LOG(ERROR, "tokenize document convertor init failed");
            RETURN_IF_STATUS_ERROR(status, "tokenize document convertor init failed");
        }
    }
    auto status = InitFromDocumentParam(initParam);
    if (!status.IsOK()) {
        ERROR_COLLECTOR_LOG(ERROR, "normal document parser init from document param failed");
        RETURN_IF_STATUS_ERROR(status, "normal document parser init from document param failed");
    }
    auto singleParser = CreateSingleDocumentParser();
    if (!singleParser) {
        ERROR_COLLECTOR_LOG(ERROR, "create single document parser failed");
        RETURN_IF_STATUS_ERROR(Status::InternalError(), "create single document parser failed");
    }
    _mainParser.reset(singleParser.release());
    if (!_mainParser->Init(_schema, _attributeConvertErrorCounter)) {
        ERROR_COLLECTOR_LOG(ERROR, "init single document parser fail!");
        RETURN_IF_STATUS_ERROR(Status::InternalError(), "init single document parser fail!");
    }
    return Status::OK();
}
Status NormalDocumentParser::InitFromDocumentParam(const std::shared_ptr<DocumentInitParam>& initParam)
{
    if (!initParam) {
        AUTIL_LOG(DEBUG, "empty init param!");
        return Status::OK();
    }

    auto param = std::dynamic_pointer_cast<BuiltInParserInitParam>(initParam);
    if (!param) {
        AUTIL_LOG(WARN, "can not use BuiltInParserInitParam");
        return Status::OK();
    }

    const BuiltInParserInitResource& resource = param->GetResource();
    _docRewriters = resource.docRewriters;
    IE_INIT_METRIC_GROUP(resource.metricProvider, UselessUpdateQps,
                         "debug/uselessUpdateQps_chain_" + _schema->GetTableName(), kmonitor::QPS, "count");
    IE_INIT_METRIC_GROUP(resource.metricProvider, IndexUselessUpdateQps,
                         "debug/indexUselessUpdateQps_chain_" + _schema->GetTableName(), kmonitor::QPS, "count");

    if (_enableModifyFieldStat) {
        IE_INIT_METRIC_GROUP(resource.metricProvider, ModifyFieldQps, "debug/modifyField/qps", kmonitor::QPS, "count");
        IE_INIT_METRIC_GROUP(resource.metricProvider, IndexUpdateQps, "debug/modifyField/indexQps", kmonitor::QPS,
                             "count");
        IE_INIT_METRIC_GROUP(resource.metricProvider, AttributeUpdateQps, "debug/modifyField/attributeQps",
                             kmonitor::QPS, "count");
        IE_INIT_METRIC_GROUP(resource.metricProvider, SummaryUpdateQps, "debug/modifyField/summaryQps", kmonitor::QPS,
                             "count");
        IE_INIT_METRIC_GROUP(resource.metricProvider, SingleModifyFieldQps, "debug/modifyField/singleFieldQps",
                             kmonitor::QPS, "count");
        IE_INIT_METRIC_GROUP(resource.metricProvider, DocParserQps, "debug/modifyField/docParserQps", kmonitor::QPS,
                             "count");
        IE_INIT_METRIC_GROUP(resource.metricProvider, IndexAddToUpdateQps, "debug/modifyField/indexAddToUpdateQps",
                             kmonitor::QPS, "count");
        IE_INIT_METRIC_GROUP(resource.metricProvider, AddToUpdateFailedQps, "debug/modifyField/addToUpdateFailedQps",
                             kmonitor::QPS, "count");
    }

    if (resource.counterMap) {
        _attributeConvertErrorCounter = resource.counterMap->GetAccCounter(ATTRIBUTE_CONVERT_ERROR_COUNTER_NAME);
        if (!_attributeConvertErrorCounter) {
            AUTIL_LOG(ERROR, "create attributeConvertErrorCounter failed");
        }
    }
    return Status::OK();
}
std::unique_ptr<IDocumentBatch> NormalDocumentParser::Parse(const std::string& docString,
                                                            const std::string& docFormat) const
{
    assert(false);
    return {};
}

std::pair<Status, std::unique_ptr<IDocumentBatch>> NormalDocumentParser::Parse(ExtendDocument& extendDoc) const
{
    auto normalExtendDoc = dynamic_cast<NormalExtendDocument*>(&extendDoc);
    if (!normalExtendDoc) {
        ERROR_COLLECTOR_LOG(ERROR, "cast to normal extend document failed");
        RETURN2_IF_STATUS_ERROR(Status::InternalError(), nullptr, "cast to normal extend document failed");
    }
    std::shared_ptr<NormalDocument> indexDoc = _mainParser->Parse(normalExtendDoc);
    if (!indexDoc) {
        return {Status::OK(), nullptr};
    }

    const auto& rawDoc = normalExtendDoc->getRawDocument();
    assert(rawDoc);
    int64_t timestamp = rawDoc->getDocTimestamp();
    DocOperateType opType = rawDoc->getDocOperateType();
    auto docInfo = indexDoc->GetDocInfo();
    docInfo.timestamp = timestamp;
    indexDoc->SetDocInfo(docInfo);
    indexDoc->SetSource(rawDoc->getDocSource());
    indexDoc->SetDocOperateType(opType);
    // indexDoc->SetSchemaVersionId(mSchema->GetSchemaVersionId());
    indexDoc->SetIngestionTimestamp(rawDoc->GetIngestionTimestamp());

    if (_enableModifyFieldStat) {
        ReportModifyFieldQps(indexDoc);
    }

    auto docBatch = std::make_unique<DocumentBatch>();
    docBatch->AddDocument(indexDoc);
    for (auto& rewriter : _docRewriters) {
        if (rewriter) {
            auto status = rewriter->Rewrite(docBatch.get());
            if (!status.IsOK()) {
                ERROR_COLLECTOR_LOG(ERROR, "rewrite document batch failed");
                RETURN2_IF_STATUS_ERROR(status, nullptr, "rewrite document batch failed");
            }
        }
    }

    if (IsEmptyUpdate(indexDoc)) {
        indexDoc->SetDocOperateType(SKIP_DOC);
        IE_INCREASE_QPS(UselessUpdateQps);
        if (indexDoc->HasIndexUpdate()) {
            IE_INCREASE_QPS(IndexUselessUpdateQps);
        }
    }
    if (_enableModifyFieldStat) {
        ReportIndexAddToUpdateQps(*indexDoc);
        ReportAddToUpdateFailQps(*indexDoc);
    }

    if (_enableModifyFieldStat) {
        const std::string& flag = rawDoc->GetTag(DOC_FLAG_FOR_METRIC);
        kmonitor::MetricsTags tag("schema_name", _schema->GetTableName());
        DocOperateType type = indexDoc->GetDocOperateType();
        DocOperateType originType = indexDoc->GetOriginalOperateType();
        tag.AddTag("operate_type", RawDocument::getCmdString(type));
        tag.AddTag("origin_operate_type", RawDocument::getCmdString(originType));
        tag.AddTag("flag", flag.empty() ? "NONE" : flag);
        IE_REPORT_METRIC_WITH_TAGS(DocParserQps, &tag, 1.0);
    }

    bool hasTermStatistics = false; // empty
    if (indexDoc->GetIndexDocument() != nullptr && !indexDoc->GetIndexDocument()->GetTermOriginValueMap().empty()) {
        hasTermStatistics = true;
    }

    // attention: make binary_version=11 document not used for modify operation and docTrace and null and source and
    // index update and statistics term, atomicly serialize to version6, to make online compitable remove this code when
    // binary version 6 is useless

    if (!(rawDoc->NeedTrace()) && !_supportNull && !indexDoc->GetSourceDocument() && !indexDoc->HasIndexUpdate() &&
        !hasTermStatistics && indexDoc->GetSerializedVersion() == DOCUMENT_BINARY_VERSION) {
        auto status = indexDoc->SetSerializedVersion(6);
        RETURN2_IF_STATUS_ERROR(status, nullptr, "normal document set serialized version [6] failed");
    }
    return {Status::OK(), std::move(docBatch)};
}

std::pair<Status, std::unique_ptr<IDocumentBatch>>
NormalDocumentParser::Parse(const autil::StringView& serializedData) const
{
    autil::DataBuffer newDataBuffer(const_cast<char*>(serializedData.data()), serializedData.length());
    std::shared_ptr<NormalDocument> document;
    try {
        bool hasObj = false;
        newDataBuffer.read(hasObj);
        if (hasObj) {
            uint32_t serializedVersion;
            newDataBuffer.read(serializedVersion);
        } else {
            RETURN2_IF_STATUS_ERROR(Status::Corruption(), nullptr, "document deserialize failed");
        }
        autil::DataBuffer dataBuffer(const_cast<char*>(serializedData.data()), serializedData.length());
        dataBuffer.read(document);
    } catch (const std::exception& e) {
        RETURN2_IF_STATUS_ERROR(Status::Corruption(), nullptr, "normal document deserialize failed, exception[%s]",
                                e.what());
    } catch (...) {
        RETURN2_IF_STATUS_ERROR(Status::Corruption(), nullptr, "normal document deserialize failed, unknown exception");
    }
    auto docBatch = std::make_unique<DocumentBatch>();
    docBatch->AddDocument(document);
    return {Status::OK(), std::move(docBatch)};
}

std::pair<Status, std::unique_ptr<IDocumentBatch>>
NormalDocumentParser::ParseRawDoc(const std::shared_ptr<RawDocument>& rawDoc) const
{
    if (!_tokenizeDocConvertor) {
        RETURN2_IF_STATUS_ERROR(Status::InternalError(), nullptr, "tokenize document convertor is empty");
    }
    auto extendDoc = std::make_shared<NormalExtendDocument>();
    extendDoc->setRawDocument(rawDoc);
    std::map<fieldid_t, std::string> fieldAnalyzerNameMap;
    auto status = _tokenizeDocConvertor->Convert(rawDoc.get(), fieldAnalyzerNameMap, extendDoc->getTokenizeDocument(),
                                                 extendDoc->getLastTokenizeDocument());
    if (!status.IsOK()) {
        AUTIL_LOG(ERROR, "convert raw doc to tokenize doc failed");
        return std::make_pair(status, nullptr);
    }
    return Parse(*extendDoc);
}

bool NormalDocumentParser::IsEmptyUpdate(const std::shared_ptr<NormalDocument>& document) const
{
    if (UPDATE_FIELD != document->GetDocOperateType()) {
        return false;
    }
    if (0 != document->GetAttributeDocument()->GetNotEmptyFieldCount()) {
        return false;
    }

    if (0 != document->GetAttributeDocument()->GetNullFieldCount()) {
        return false;
    }
    const auto& indexDoc = document->GetIndexDocument();
    if (indexDoc) {
        const auto& modifiedTokens = indexDoc->GetModifiedTokens();
        for (const auto& tokens : modifiedTokens) {
            if (tokens.Valid() and !tokens.Empty()) {
                return false;
            }
        }
    }

    return true;
}

void NormalDocumentParser::ReportModifyFieldQps(const std::shared_ptr<NormalDocument>& doc) const
{
    if (!_schema || !doc || doc->GetDocOperateType() != ADD_DOC) {
        return;
    }

    const auto& ids = doc->GetModifiedFields();
    if (ids.empty()) {
        kmonitor::MetricsTags tag("modify_field", "NONE");
        tag.AddTag("schema_name", _schema->GetTableName());
        IE_REPORT_METRIC_WITH_TAGS(ModifyFieldQps, &tag, 1.0);
        return;
    }

    for (size_t i = 0; i < ids.size(); i++) {
        auto id = ids[i];
        auto fieldConfig = _schema->GetFieldConfig(id);
        if (!fieldConfig) {
            continue;
        }
        kmonitor::MetricsTags tag("modify_field", fieldConfig->GetFieldName());
        tag.AddTag("schema_name", _schema->GetTableName());
        IE_REPORT_METRIC_WITH_TAGS(ModifyFieldQps, &tag, 1.0);

        if (i == 0 && ids.size() == 1) {
            IE_REPORT_METRIC_WITH_TAGS(SingleModifyFieldQps, &tag, 1.0);
        }
        if (_mainParser->IsInvertedIndexField(id)) {
            IE_REPORT_METRIC_WITH_TAGS(IndexUpdateQps, &tag, 1.0);
        }
        if (_mainParser->IsAttributeIndexField(id)) {
            IE_REPORT_METRIC_WITH_TAGS(AttributeUpdateQps, &tag, 1.0);
        }
        if (_mainParser->IsSummaryIndexField(id)) {
            IE_REPORT_METRIC_WITH_TAGS(SummaryUpdateQps, &tag, 1.0);
        }
    }
}

void NormalDocumentParser::ReportIndexAddToUpdateQps(const NormalDocument& doc) const
{
    if (!_schema || doc.GetDocOperateType() != UPDATE_FIELD || doc.GetOriginalOperateType() != ADD_DOC) {
        return;
    }

    auto reportSingleDoc = [this](const NormalDocument& doc, const config::TabletSchema& schema) mutable {
        const auto& indexDoc = doc.GetIndexDocument();
        if (!indexDoc) {
            return;
        }
        const auto& modifiedTokens = indexDoc->GetModifiedTokens();
        for (const auto& tokens : modifiedTokens) {
            if (tokens.Valid() and !tokens.Empty()) {
                const auto& fieldConfig = schema.GetFieldConfig(tokens.FieldId());
                kmonitor::MetricsTags tag("modify_field", fieldConfig->GetFieldName());
                tag.AddTag("schema_name", schema.GetTableName());
                IE_REPORT_METRIC_WITH_TAGS(IndexAddToUpdateQps, &tag, 1.0);
            }
        }
    };
    reportSingleDoc(doc, *_schema);
}

void NormalDocumentParser::ReportAddToUpdateFailQps(const NormalDocument& doc) const
{
    if (!_schema || doc.GetDocOperateType() != ADD_DOC || doc.GetOriginalOperateType() != ADD_DOC) {
        return;
    }

    auto reportSingleDoc = [this](const NormalDocument& doc, const config::TabletSchema& schema) mutable {
        const auto& modifyFailedFields = doc.GetModifyFailedFields();
        if (modifyFailedFields.empty()) {
            return;
        }
        for (auto fid : modifyFailedFields) {
            const auto& fieldConfig = schema.GetFieldConfig(fid);
            kmonitor::MetricsTags tag("modify_field", fieldConfig->GetFieldName());
            tag.AddTag("schema_name", schema.GetTableName());
            IE_REPORT_METRIC_WITH_TAGS(AddToUpdateFailedQps, &tag, 1.0);
        }
    };
    reportSingleDoc(doc, *_schema);
}

std::unique_ptr<SingleDocumentParser> NormalDocumentParser::CreateSingleDocumentParser() const
{
    return std::make_unique<SingleDocumentParser>();
}

} // namespace indexlibv2::document
