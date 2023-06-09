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
#include "indexlib/document/document_parser/normal_parser/normal_document_parser.h"

#include "autil/EnvUtil.h"
#include "indexlib/config/attribute_config.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/document/builtin_parser_init_param.h"
#include "indexlib/document/index_document/normal_document/normal_document.h"
#include "indexlib/document/raw_document/default_raw_document.h"
#include "indexlib/document/raw_document/raw_document_define.h"
#include "indexlib/document/source_timestamp_parser.h"
#include "indexlib/util/counter/AccumulativeCounter.h"
#include "indexlib/util/counter/CounterMap.h"

using namespace std;
using namespace autil;

using namespace indexlib::config;
using namespace indexlib::util;
using namespace indexlib::common;

namespace indexlib { namespace document {
IE_LOG_SETUP(document, NormalDocumentParser);

string NormalDocumentParser::ATTRIBUTE_CONVERT_ERROR_COUNTER_NAME = "bs.processor.attributeConvertError";

NormalDocumentParser::NormalDocumentParser(const IndexPartitionSchemaPtr& schema)
    : DocumentParser(schema)
    , mSupportNull(false)
    , mEnableModifyFieldStat(false)
{
    const auto& fieldConfigs = mSchema->GetFieldConfigs();
    for (const auto& fieldConfig : fieldConfigs) {
        if (fieldConfig->IsEnableNullField()) {
            mSupportNull = true;
            break;
        }
    }
    mEnableModifyFieldStat = autil::EnvUtil::getEnv("INDEXLIB_ENABLE_STAT_MODIFY_FIELDS", mEnableModifyFieldStat);
    mSourceTimestampParser = std::make_unique<SourceTimestampParser>(schema);
}

NormalDocumentParser::~NormalDocumentParser() {}

bool NormalDocumentParser::DoInit()
{
    if (!InitFromDocumentParam()) {
        return false;
    }
    mMainParser.reset(new SingleDocumentParser);
    if (!mMainParser->Init(mSchema, mAttributeConvertErrorCounter)) {
        ERROR_COLLECTOR_LOG(ERROR, "init single document parser fail!");
        return false;
    }
    const IndexPartitionSchemaPtr& subSchema = mSchema->GetSubIndexPartitionSchema();
    if (subSchema) {
        mSubParser.reset(new SingleDocumentParser);
        if (!mSubParser->Init(mSchema->GetSubIndexPartitionSchema(), mAttributeConvertErrorCounter)) {
            ERROR_COLLECTOR_LOG(ERROR, "init single document parser for sub schema fail!");
            return false;
        }
    }
    mSourceTimestampParser->Init();
    return true;
}

DocumentPtr NormalDocumentParser::Parse(const IndexlibExtendDocumentPtr& document)
{
    NormalDocumentPtr indexDoc = mMainParser->Parse(document);
    if (!indexDoc) {
        return DocumentPtr();
    }

    const RawDocumentPtr& rawDoc = document->getRawDocument();
    assert(rawDoc);
    int64_t timestamp = rawDoc->getDocTimestamp();
    DocOperateType opType = rawDoc->getDocOperateType();
    indexDoc->SetTimestamp(timestamp);
    indexDoc->SetSource(rawDoc->getDocSource());
    indexDoc->SetDocOperateType(opType);
    indexDoc->SetRegionId(document->getRegionId());
    indexDoc->SetSchemaVersionId(mSchema->GetSchemaVersionId());
    indexDoc->SetIngestionTimestamp(rawDoc->GetIngestionTimestamp());
    mSourceTimestampParser->Parse(document, indexDoc.get());

    const IndexlibExtendDocument::IndexlibExtendDocumentVec& subDocuments = document->getAllSubDocuments();
    if (!mSubParser && !subDocuments.empty()) {
        ERROR_COLLECTOR_LOG(ERROR, "not support parse sub documents!");
        return DocumentPtr();
    }
    for (size_t i = 0; i < subDocuments.size(); i++) {
        NormalDocumentPtr subIndexDoc = mSubParser->Parse(subDocuments[i]);
        if (!subIndexDoc) {
            continue;
        }
        subIndexDoc->SetTimestamp(timestamp);
        subIndexDoc->SetDocOperateType(opType);
        subIndexDoc->SetSchemaVersionId(mSchema->GetSchemaVersionId());
        indexDoc->AddSubDocument(subIndexDoc);
    }

    if (mEnableModifyFieldStat) {
        ReportModifyFieldQps(indexDoc);
    }

    for (auto& rewriter : mDocRewriters) {
        if (rewriter) {
            rewriter->Rewrite(indexDoc);
        }
    }

    if (IsEmptyUpdate(indexDoc)) {
        indexDoc->SetDocOperateType(SKIP_DOC);
        IE_INCREASE_QPS(UselessUpdateQps);
        if (indexDoc->HasIndexUpdate()) {
            IE_INCREASE_QPS(IndexUselessUpdateQps);
        }
    }
    if (mEnableModifyFieldStat) {
        ReportIndexAddToUpdateQps(*indexDoc);
        ReportAddToUpdateFailQps(*indexDoc);
    }

    if (mEnableModifyFieldStat) {
        const std::string& flag = rawDoc->GetTag(DOC_FLAG_FOR_METRIC);
        kmonitor::MetricsTags tag("schema_name", mSchema->GetSchemaName());
        DocOperateType type = indexDoc->GetDocOperateType();
        DocOperateType originType = indexDoc->GetOriginalOperateType();
        tag.AddTag("operate_type", DefaultRawDocument::getCmdString(type));
        tag.AddTag("origin_operate_type", DefaultRawDocument::getCmdString(originType));
        tag.AddTag("flag", flag.empty() ? "NONE" : flag);
        IE_REPORT_METRIC_WITH_TAGS(DocParserQps, &tag, 1.0);
    }

    // attention: make binary_version=10 document not used for modify operation and docTrace and null and source and
    // index update, atomicly serialize to version6, to make online compitable remove this code when binary version
    // 6 is useless
    if (!mSchema->HasModifyOperations() && !(document->getRawDocument()->NeedTrace()) && !mSupportNull &&
        !indexDoc->GetSourceDocument() && !indexDoc->HasIndexUpdate() &&
        indexDoc->GetSerializedVersion() == LEGACY_DOCUMENT_BINARY_VERSION) {
        indexDoc->SetSerializedVersion(6);
    }
    return indexDoc;
}

DocumentPtr NormalDocumentParser::Parse(const StringView& serializedData)
{
    DataBuffer newDataBuffer(const_cast<char*>(serializedData.data()), serializedData.length());
    bool hasObj;
    newDataBuffer.read(hasObj);
    if (hasObj) {
        uint32_t serializedVersion;
        newDataBuffer.read(serializedVersion);
        if (serializedVersion > LEGACY_DOCUMENT_BINARY_VERSION && mSchema->GetSubIndexPartitionSchema() != NULL) {
            INDEXLIB_THROW(util::BadParameterException,
                           "document serialize failed, "
                           "do not support serialize document with version %u",
                           serializedVersion);
        }
    } else {
        INDEXLIB_THROW(util::BadParameterException, "document serialize failed");
    }
    DataBuffer dataBuffer(const_cast<char*>(serializedData.data()), serializedData.length());
    NormalDocumentPtr document;
    dataBuffer.read(document);
    return document;
}

bool NormalDocumentParser::IsEmptyUpdate(const NormalDocumentPtr& document)
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

    const NormalDocument::DocumentVector& subDocuments = document->GetSubDocuments();
    for (size_t i = 0; i < subDocuments.size(); i++) {
        if (!IsEmptyUpdate(subDocuments[i])) {
            return false;
        }
    }
    return true;
}

bool NormalDocumentParser::InitFromDocumentParam()
{
    if (!mInitParam) {
        IE_LOG(DEBUG, "empty init param!");
        return true;
    }

    BuiltInParserInitParamPtr param = DYNAMIC_POINTER_CAST(BuiltInParserInitParam, mInitParam);
    if (!param) {
        IE_LOG(WARN, "can not use BuiltInParserInitParam");
        return true;
    }

    const BuiltInParserInitResource& resource = param->GetResource();
    mDocRewriters = resource.docRewriters;
    IE_INIT_METRIC_GROUP(resource.metricProvider, UselessUpdateQps,
                         "debug/uselessUpdateQps_chain_" + mSchema->GetSchemaName(), kmonitor::QPS, "count");
    IE_INIT_METRIC_GROUP(resource.metricProvider, IndexUselessUpdateQps,
                         "debug/indexUselessUpdateQps_chain_" + mSchema->GetSchemaName(), kmonitor::QPS, "count");

    if (mEnableModifyFieldStat) {
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
        mAttributeConvertErrorCounter = resource.counterMap->GetAccCounter(ATTRIBUTE_CONVERT_ERROR_COUNTER_NAME);
        if (!mAttributeConvertErrorCounter) {
            IE_LOG(ERROR, "create attributeConvertErrorCounter failed");
        }
    }
    return true;
}

void NormalDocumentParser::ReportModifyFieldQps(const NormalDocumentPtr& doc)
{
    if (!mSchema || !doc || doc->GetDocOperateType() != ADD_DOC) {
        return;
    }

    const auto& ids = doc->GetModifiedFields();
    if (ids.empty()) {
        kmonitor::MetricsTags tag("modify_field", "NONE");
        tag.AddTag("schema_name", mSchema->GetSchemaName());
        IE_REPORT_METRIC_WITH_TAGS(ModifyFieldQps, &tag, 1.0);
        return;
    }

    const IndexSchemaPtr& indexSchema = mSchema->GetIndexSchema();
    const AttributeSchemaPtr& attrSchema = mSchema->GetAttributeSchema();
    const SummarySchemaPtr& summarySchema = mSchema->GetSummarySchema();

    for (size_t i = 0; i < ids.size(); i++) {
        auto id = ids[i];
        auto fieldConfig = mSchema->GetFieldConfig(id);
        if (!fieldConfig) {
            continue;
        }
        kmonitor::MetricsTags tag("modify_field", fieldConfig->GetFieldName());
        tag.AddTag("schema_name", mSchema->GetSchemaName());
        IE_REPORT_METRIC_WITH_TAGS(ModifyFieldQps, &tag, 1.0);

        if (i == 0 && ids.size() == 1) {
            IE_REPORT_METRIC_WITH_TAGS(SingleModifyFieldQps, &tag, 1.0);
        }
        if (indexSchema && indexSchema->IsInIndex(id)) {
            IE_REPORT_METRIC_WITH_TAGS(IndexUpdateQps, &tag, 1.0);
        }
        if (attrSchema && attrSchema->IsInAttribute(id)) {
            IE_REPORT_METRIC_WITH_TAGS(AttributeUpdateQps, &tag, 1.0);
        }
        if (summarySchema && summarySchema->IsInSummary(id)) {
            IE_REPORT_METRIC_WITH_TAGS(SummaryUpdateQps, &tag, 1.0);
        }
    }
}

void NormalDocumentParser::ReportIndexAddToUpdateQps(const NormalDocument& doc)
{
    if (!mSchema || doc.GetDocOperateType() != UPDATE_FIELD || doc.GetOriginalOperateType() != ADD_DOC) {
        return;
    }

    auto reportSingleDoc = [this](const NormalDocument& doc, const config::IndexPartitionSchema& schema) mutable {
        const auto& indexDoc = doc.GetIndexDocument();
        if (!indexDoc) {
            return;
        }
        const auto& modifiedTokens = indexDoc->GetModifiedTokens();
        for (const auto& tokens : modifiedTokens) {
            if (tokens.Valid() and !tokens.Empty()) {
                const auto& fieldConfig = schema.GetFieldConfig(tokens.FieldId());
                kmonitor::MetricsTags tag("modify_field", fieldConfig->GetFieldName());
                tag.AddTag("schema_name", schema.GetSchemaName());
                IE_REPORT_METRIC_WITH_TAGS(IndexAddToUpdateQps, &tag, 1.0);
            }
        }
    };
    reportSingleDoc(doc, *mSchema);
    for (const auto& subDoc : doc.GetSubDocuments()) {
        reportSingleDoc(*subDoc, *(mSchema->GetSubIndexPartitionSchema()));
    }
}

void NormalDocumentParser::ReportAddToUpdateFailQps(const NormalDocument& doc)
{
    if (!mSchema || doc.GetDocOperateType() != ADD_DOC || doc.GetOriginalOperateType() != ADD_DOC) {
        return;
    }

    auto reportSingleDoc = [this](const NormalDocument& doc, const config::IndexPartitionSchema& schema) mutable {
        const FieldSchemaPtr& fieldSchema = schema.GetFieldSchema();
        const auto& modifyFailedFields = doc.GetModifyFailedFields();
        if (modifyFailedFields.empty()) {
            return;
        }
        for (auto fid : modifyFailedFields) {
            const auto& fieldConfig = fieldSchema->GetFieldConfig(fid);
            kmonitor::MetricsTags tag("modify_field", fieldConfig->GetFieldName());
            tag.AddTag("schema_name", schema.GetSchemaName());
            IE_REPORT_METRIC_WITH_TAGS(AddToUpdateFailedQps, &tag, 1.0);
        }
    };
    reportSingleDoc(doc, *mSchema);
    for (const auto& subDoc : doc.GetSubDocuments()) {
        reportSingleDoc(*subDoc, *(mSchema->GetSubIndexPartitionSchema()));
    }
}
}} // namespace indexlib::document
