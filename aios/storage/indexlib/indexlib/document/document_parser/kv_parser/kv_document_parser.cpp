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
#include "indexlib/document/document_parser/kv_parser/kv_document_parser.h"

#include "autil/EnvUtil.h"
#include "indexlib/common/field_format/attribute/attribute_convertor_factory.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/document/builtin_parser_init_param.h"
#include "indexlib/document/document_parser/kv_parser/multi_region_kv_key_extractor.h"
#include "indexlib/document/document_parser/normal_parser/multi_region_extend_doc_fields_convertor.h"
#include "indexlib/document/document_rewriter/multi_region_pack_attribute_appender.h"
#include "indexlib/document/index_document/normal_document/normal_document.h"
#include "indexlib/document/source_timestamp_parser.h"
#include "indexlib/index_define.h"
#include "indexlib/misc/doc_tracer.h"
#include "indexlib/util/counter/AccumulativeCounter.h"
#include "indexlib/util/counter/CounterMap.h"

using namespace std;
using namespace autil;
using namespace indexlib::config;
using namespace indexlib::common;

namespace indexlib { namespace document {
IE_LOG_SETUP(document, KvDocumentParser);

namespace {
static const std::string ATTRIBUTE_CONVERT_ERROR_COUNTER_NAME = "bs.processor.attributeConvertError";
}

KvDocumentParser::KvDocumentParser(const IndexPartitionSchemaPtr& schema)
    : DocumentParser(schema)
    , mDenyEmptyPrimaryKey(false)
    , mValueAttrFieldId(INVALID_FIELDID)
{
    mSourceTimestampParser = std::make_unique<SourceTimestampParser>(schema);
}

KvDocumentParser::~KvDocumentParser() {}

bool KvDocumentParser::DoInit()
{
    if (!InitFromDocumentParam()) {
        return false;
    }

    const auto& userDefinedParam = mSchema->GetUserDefinedParam();
    auto itr = userDefinedParam.find("deny_empty_pkey");
    if (userDefinedParam.end() != itr) {
        mDenyEmptyPrimaryKey = autil::legacy::AnyCast<bool>(itr->second);
        IE_LOG(INFO, "deny_empty_pkey is [%s]", mDenyEmptyPrimaryKey ? "true" : "false");
    }

    mFieldConvertPtr.reset(new MultiRegionExtendDocFieldsConvertor(mSchema));
    MultiRegionPackAttributeAppenderPtr packAttrAppender(new MultiRegionPackAttributeAppender());
    if (packAttrAppender->Init(mSchema)) {
        mPackAttrAppender = packAttrAppender;
    }
    mValueAttrFieldId = AttrValueFieldId();

    InitStorePkey();
    mSourceTimestampParser->Init();

    return InitKeyExtractor();
}

void KvDocumentParser::InitStorePkey()
{
    bool storePKey = false;
    if (autil::EnvUtil::getEnvWithoutDefault(NEED_STORE_PKEY_VALUE, storePKey)) {
        mNeedStorePKeyValue = storePKey;
    }
    IE_LOG(INFO, "store pkey value = [%d]", mNeedStorePKeyValue);
}

bool KvDocumentParser::InitFromDocumentParam()
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

    if (resource.counterMap) {
        mAttributeConvertErrorCounter = resource.counterMap->GetAccCounter(ATTRIBUTE_CONVERT_ERROR_COUNTER_NAME);
        if (!mAttributeConvertErrorCounter) {
            IE_LOG(ERROR, "create attributeConvertErrorCounter failed");
        }
    }
    return true;
}

DocumentPtr KvDocumentParser::Parse(const IndexlibExtendDocumentPtr& document)
{
    const RawDocumentPtr& rawDoc = document->getRawDocument();
    if (!rawDoc) {
        IE_LOG(ERROR, "empty raw document!");
        return DocumentPtr();
    }
    auto kvRawDoc = DYNAMIC_POINTER_CAST(document::KVDocument, rawDoc);
    if (kvRawDoc && kvRawDoc->GetDocumentCount() > 0u) {
        return kvRawDoc;
    }

    auto kvDoc = std::make_shared<document::KVDocument>();
    bool ret = Parse(document, kvDoc.get());
    return ret ? kvDoc : DocumentPtr();
}

bool KvDocumentParser::Parse(const IndexlibExtendDocumentPtr& document, document::KVDocument* kvDoc)
{
    kvDoc->SetNeedStorePKeyValue(mNeedStorePKeyValue);
    const RawDocumentPtr& rawDoc = document->getRawDocument();
    if (!rawDoc) {
        IE_LOG(ERROR, "empty raw document!");
        return false;
    }

    DocOperateType opType = rawDoc->getDocOperateType();
    if (opType == SKIP_DOC || opType == UNKNOWN_OP) {
        return CreateDocument(document, kvDoc);
    }

    if (opType == DELETE_SUB_DOC) {
        ERROR_COLLECTOR_LOG(ERROR, "unsupported document cmd type [%d]", opType);
        IE_RAW_DOC_FORMAT_TRACE(rawDoc, "unsupported document cmd type [%d]", opType);
        return false;
    }

    regionid_t regionId = document->getRegionId();

    const FieldSchemaPtr& fieldSchema = mSchema->GetFieldSchema(regionId);
    const AttributeSchemaPtr& attrSchema = mSchema->GetAttributeSchema(regionId);

    if (opType == DELETE_DOC) {
        return CreateDocument(document, kvDoc);
    }

    for (FieldSchema::Iterator it = fieldSchema->Begin(); it != fieldSchema->End(); ++it) {
        assert((*it)->IsNormal());
        fieldid_t fieldId = (*it)->GetFieldId();
        if (attrSchema && attrSchema->IsInAttribute(fieldId)) {
            if (opType == ADD_DOC) {
                mFieldConvertPtr->convertAttributeField(document, *it, false);
            } else {
                assert(opType == UPDATE_FIELD);
                mFieldConvertPtr->convertAttributeField(document, *it, true);
            }
        }
    }

    const ClassifiedDocumentPtr& classifiedDocument = document->getClassifiedDocument();
    const auto& attrDoc = classifiedDocument->getAttributeDoc();
    if (attrDoc->HasFormatError()) {
        kvDoc->SetFormatError();
        if (mAttributeConvertErrorCounter) {
            mAttributeConvertErrorCounter->Increase(1);
        }
    }

    if (mPackAttrAppender) {
        if (opType == ADD_DOC) {
            mPackAttrAppender->AppendPackAttribute(classifiedDocument->getAttributeDoc(), classifiedDocument->getPool(),
                                                   regionId);
        } else {
            assert(opType == UPDATE_FIELD);
            mPackAttrAppender->EncodePatchValues(classifiedDocument->getAttributeDoc(), classifiedDocument->getPool(),
                                                 regionId);
        }
    }
    return CreateDocument(document, kvDoc);
}

DocumentPtr KvDocumentParser::Parse(const StringView& serializedData)
{
    DataBuffer dataBuffer(const_cast<char*>(serializedData.data()), serializedData.length());
    document::KVDocumentPtr kvDoc;
    dataBuffer.read(kvDoc);
    if (kvDoc->NeedDeserializeFromLegacy()) {
        DataBuffer legacyDataBuffer(const_cast<char*>(serializedData.data()), serializedData.length());
        document::NormalDocumentPtr normalDoc;
        legacyDataBuffer.read(normalDoc);
        kvDoc = std::make_shared<document::KVDocument>();
        auto kvIndexDoc = kvDoc->CreateOneDoc();

        kvIndexDoc->SetDocOperateType(normalDoc->GetOriginalOperateType());
        const legacy::KVIndexDocumentPtr& legacyIndexDocument = normalDoc->GetLegacyKVIndexDocument();
        if (!legacyIndexDocument) {
            INDEXLIB_FATAL_ERROR(DocumentDeserialize, "no KVIndexDocument in NormalDocument, serializedVersion");
        }
        kvIndexDoc->SetPKeyHash(legacyIndexDocument->GetPkeyHash());
        if (legacyIndexDocument->HasSkey()) {
            kvIndexDoc->SetSKeyHash(legacyIndexDocument->GetSkeyHash());
        }
        if (kvIndexDoc->GetDocOperateType() == ADD_DOC) {
            const auto& attrDoc = normalDoc->GetAttributeDocument();
            if (mValueAttrFieldId == INVALID_FIELDID) {
                if (attrDoc->GetPackFieldCount() != 1) {
                    IE_LOG(ERROR, "kkv only support value in one pack attribute yet! ");
                    return DocumentPtr();
                }
                autil::StringView fieldValue = attrDoc->GetPackField(0);
                kvIndexDoc->SetValue(fieldValue);
            } else {
                autil::StringView fieldValue = attrDoc->GetField(mValueAttrFieldId);
                kvIndexDoc->SetValue(fieldValue);
            }
        }
        auto regionId = normalDoc->GetRegionId();
        kvIndexDoc->SetRegionId(regionId);
        kvIndexDoc->SetTimestamp(normalDoc->GetTimestamp());
        kvIndexDoc->SetUserTimestamp(normalDoc->GetUserTimestamp());
        kvDoc->SetTimestamp(normalDoc->GetTimestamp());
        auto& tagValue = normalDoc->GetTag(SourceTimestampParser::SOURCE_TIMESTAMP_TAG_NAME);
        if (!tagValue.empty()) {
            kvDoc->AddTag(SourceTimestampParser::SOURCE_TIMESTAMP_TAG_NAME, tagValue);
        }
        return kvDoc;
    } else {
        return kvDoc;
    }
}

bool KvDocumentParser::SetPrimaryKeyField(const IndexlibExtendDocumentPtr& document, const IndexSchemaPtr& indexSchema,
                                          regionid_t regionId, DocOperateType opType,
                                          document::KVIndexDocument* kvIndexDoc)
{
    const string& pkFieldName = indexSchema->GetPrimaryKeyIndexFieldName();
    const RawDocumentPtr& rawDoc = document->getRawDocument();
    if (pkFieldName.empty()) {
        ERROR_COLLECTOR_LOG(ERROR, "key field is empty!");
        IE_RAW_DOC_TRACE(rawDoc, "parse error: key field is empty");
        return true;
    }

    string pkValue = rawDoc->getField(pkFieldName);
    if (mDenyEmptyPrimaryKey && pkValue.empty()) {
        ERROR_COLLECTOR_LOG(ERROR, "key value is empty!");
        IE_RAW_DOC_TRACE(rawDoc, "parse error: key value is empty");
        return false;
    }

    document->setIdentifier(pkValue);
    kvIndexDoc->SetPkField(autil::StringView(pkFieldName), autil::StringView(pkValue));

    uint64_t keyHash = 0;
    mKvKeyExtractor->HashKey(pkValue, keyHash, regionId);
    kvIndexDoc->SetPKeyHash(keyHash);

    return true;
}

bool KvDocumentParser::InitKeyExtractor()
{
    if (mSchema->GetTableType() != tt_kv) {
        ERROR_COLLECTOR_LOG(ERROR,
                            "init KvDocumentParser fail, "
                            "unsupport tableType [%s]!",
                            IndexPartitionSchema::TableType2Str(mSchema->GetTableType()).c_str());
        return false;
    }
    mKvKeyExtractor.reset(new MultiRegionKVKeyExtractor(mSchema));
    return true;
}

bool KvDocumentParser::CreateDocument(const IndexlibExtendDocumentPtr& document, document::KVDocument* kvDoc)
{
    const RawDocumentPtr& rawDoc = document->getRawDocument();
    const ClassifiedDocumentPtr& classifiedDoc = document->getClassifiedDocument();
    DocOperateType opType = rawDoc->getDocOperateType();
    if (opType == SKIP_DOC || opType == UNKNOWN_OP) {
        return false;
    }
    kvDoc->SetDocOperateType(ADD_DOC); // KVDocument is a doc wrapper, always add.
    auto kvIndexDoc = kvDoc->CreateOneDoc();
    regionid_t regionId = document->getRegionId();

    const auto& indexSchema = mSchema->GetIndexSchema(regionId);
    if (!SetPrimaryKeyField(document, indexSchema, regionId, opType, kvIndexDoc)) {
        return false;
    }

    if (mSchema->TTLEnabled(regionId) && mSchema->TTLFromDoc(regionId)) {
        uint32_t ttl = 0;
        const auto& ttlFieldName = mSchema->GetTTLFieldName(regionId);
        autil::StringView ttlField = rawDoc->getField(autil::StringView(ttlFieldName.data(), ttlFieldName.size()));
        if (!ttlField.empty() && autil::StringUtil::strToUInt32(ttlField.data(), ttl)) {
            kvIndexDoc->SetTTL(ttl);
        }
    }
    kvDoc->SetSource(rawDoc->getDocSource());

    int64_t timestamp = rawDoc->getDocTimestamp(); // userts
    kvIndexDoc->SetUserTimestamp(timestamp);
    kvIndexDoc->SetTimestamp(timestamp);
    kvIndexDoc->SetDocOperateType(opType);
    kvIndexDoc->SetRegionId(document->getRegionId());
    mSourceTimestampParser->Parse(document, kvDoc);

    if (opType == DELETE_DOC) {
        return true;
    }

    if (mValueAttrFieldId == INVALID_FIELDID) {
        if (classifiedDoc->getAttributeDoc()->GetPackFieldCount() != 1) {
            string identifier = document->getIdentifier();
            IE_LOG(ERROR, "kkv only support value in one pack attribute yet!, identifier[%s]", identifier.c_str());
            kvDoc->EraseLastDoc();
            return false;
        }
        autil::StringView fieldValue = classifiedDoc->getAttributeDoc()->GetPackField(0);
        kvIndexDoc->SetValue(fieldValue);
    } else {
        // kv value inline
        autil::StringView fieldValue = classifiedDoc->getAttributeDoc()->GetField(mValueAttrFieldId);
        kvIndexDoc->SetValue(fieldValue);
    }
    document->getClassifiedDocument()->clear();
    return true;
}

fieldid_t KvDocumentParser::AttrValueFieldId() const
{
    if (mSchema->GetTableType() == tt_kkv) {
        return INVALID_FIELDID;
    }
    if (mSchema->GetRegionCount() != 1) {
        return INVALID_FIELDID;
    }
    const auto& indexSchema = mSchema->GetIndexSchema(DEFAULT_REGIONID);
    const auto& pkConfig = indexSchema->GetPrimaryKeyIndexConfig();
    auto kvConfig = static_cast<config::KVIndexConfig*>(pkConfig.get());
    const auto& valueConfig = kvConfig->GetValueConfig();
    if (valueConfig->IsSimpleValue()) {
        const config::AttributeConfigPtr& attrConfig = valueConfig->GetAttributeConfig(0);
        return attrConfig->GetFieldId();
    }
    return INVALID_FIELDID;
}
}} // namespace indexlib::document
