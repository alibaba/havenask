#include "indexlib/test/document_creator.h"

#include "autil/StringUtil.h"
#include "indexlib/common/field_format/attribute/attribute_convertor_factory.h"
#include "indexlib/config/kkv_index_config.h"
#include "indexlib/document/document_parser/kv_parser/kkv_keys_extractor.h"
#include "indexlib/document/document_parser/kv_parser/kv_key_extractor.h"
#include "indexlib/document/document_rewriter/multi_region_pack_attribute_appender.h"
#include "indexlib/document/document_rewriter/section_attribute_rewriter.h"
#include "indexlib/document/index_document/normal_document/serialized_source_document.h"
#include "indexlib/document/index_document/normal_document/source_document.h"
#include "indexlib/document/index_document/normal_document/source_document_formatter.h"
#include "indexlib/document/index_document/normal_document/summary_formatter.h"
#include "indexlib/document/kv_document/kv_document.h"
#include "indexlib/document/kv_document/kv_message_generated.h"
#include "indexlib/index/kv/kv_define.h"
#include "indexlib/test/ExtendDocField2IndexDocField.h"
#include "indexlib/test/document_parser.h"
#include "indexlib/test/sub_document_extractor.h"

using namespace std;
using namespace autil;

using namespace indexlib::document;
using namespace indexlib::config;
using namespace indexlib::index;

namespace indexlib { namespace test {
IE_LOG_SETUP(test, DocumentCreator);

NormalDocumentPtr DocumentCreator::CreateNormalDocument(const IndexPartitionSchemaPtr& schema, const string& docString,
                                                        bool needRewriteSectionAttribute, autil::mem_pool::Pool* pool)
{
    RawDocumentPtr rawDoc = DocumentParser::Parse(docString);
    assert(rawDoc);

    IndexPartitionSchemaPtr subSchema = schema->GetSubIndexPartitionSchema();
    vector<RawDocumentPtr> subDocs;
    if (subSchema) {
        SubDocumentExtractor extractor(schema);
        extractor.extractSubDocuments(rawDoc, subDocs);
    }

    NormalDocumentPtr doc = CreateNormalDocument(schema, rawDoc, pool);
    if (subSchema) {
        for (size_t i = 0; i < subDocs.size(); ++i) {
            NormalDocumentPtr subDoc = CreateNormalDocument(subSchema, subDocs[i], pool);
            doc->AddSubDocument(subDoc);
        }
    }

    if (needRewriteSectionAttribute) {
        SectionAttributeRewriter rewriter;
        if (rewriter.Init(schema)) {
            rewriter.Rewrite(doc);
        }
    }

    return doc;
}

void DocumentCreator::SetPrimaryKey(config::IndexPartitionSchemaPtr schema, RawDocumentPtr rawDoc, DocumentPtr document)
{
    assert(document);
    NormalDocumentPtr doc = DYNAMIC_POINTER_CAST(NormalDocument, document);
    IndexSchemaPtr indexSchema = schema->GetIndexSchema(doc->GetRegionId());
    if (!indexSchema->HasPrimaryKeyIndex()) {
        return;
    }
    // inverted index doc
    if (schema->GetTableType() == tt_index || schema->GetTableType() == tt_orc) {
        string pk;
        string pkFieldName = indexSchema->GetPrimaryKeyIndexFieldName();
        pk = rawDoc->GetField(pkFieldName);
        IndexDocumentPtr indexDoc = doc->GetIndexDocument();
        assert(indexDoc);
        indexDoc->SetPrimaryKey(pk);
        return;
    }
}

std::string DocumentCreator::makeFbKvMessage(const std::map<std::string, std::string>& fields,
                                             const std::string& region, DocOperateType opType)
{
    flatbuffers::FlatBufferBuilder builder;
    std::vector<flatbuffers::Offset<document::proto::kv::Field>> fbFields;
    for (const auto& item : fields) {
        auto fieldName = builder.CreateString(item.first.c_str(), item.first.size());

        if (item.first == "nid") {
            // read from int value
            document::proto::kv::FieldBuilder fieldBuilder(builder);
            fieldBuilder.add_integer_value(autil::StringUtil::numberFromString<int64_t>(item.second));
            fieldBuilder.add_name(fieldName);
            auto field = fieldBuilder.Finish();
            fbFields.push_back(field);
        } else {
            auto fieldValue = builder.CreateString(item.second.c_str(), item.second.size());
            document::proto::kv::FieldBuilder fieldBuilder(builder);
            fieldBuilder.add_string_value(fieldValue);
            fieldBuilder.add_name(fieldName);
            auto field = fieldBuilder.Finish();
            fbFields.push_back(field);
        }
    }
    assert(opType == ADD_DOC || opType == DELETE_DOC);
    document::proto::kv::OperationType fbOpType = document::proto::kv::OperationType::OperationType_Add;
    if (opType == DELETE_DOC) {
        fbOpType = document::proto::kv::OperationType::OperationType_Delete;
    }

    flatbuffers::Offset<flatbuffers::String> regionName = 0;
    if (!region.empty()) {
        regionName = builder.CreateString(region.c_str(), region.size());
    }

    auto message = document::proto::kv::CreateDocMessage(
        builder, builder.CreateVector(fbFields.data(), fbFields.size()), fbOpType, regionName);
    builder.Finish(message);
    return std::string((char*)builder.GetBufferPointer(), builder.GetSize());
}

void DocumentCreator::SetKVPrimaryKey(config::IndexPartitionSchemaPtr schema, RawDocumentPtr rawDoc,
                                      document::KVIndexDocument* doc)
{
    assert(doc);
    IndexSchemaPtr indexSchema = schema->GetIndexSchema(doc->GetRegionId());
    if (!indexSchema->HasPrimaryKeyIndex()) {
        return;
    }
    if (schema->GetTableType() == tt_kv) {
        KVIndexConfigPtr kvIndexConfig = DYNAMIC_POINTER_CAST(KVIndexConfig, indexSchema->GetPrimaryKeyIndexConfig());
        string primaryKey = rawDoc->GetField(kvIndexConfig->GetKeyFieldName());
        KVKeyExtractorPtr kvKeyExtractor(new KVKeyExtractor(kvIndexConfig));
        keytype_t key;
        kvKeyExtractor->HashKey(primaryKey, key);
        doc->SetPKeyHash(key);
        doc->SetPkField(autil::StringView(kvIndexConfig->GetKeyFieldName()), autil::StringView(primaryKey));
        return;
    }
    // kkv Doc
    KKVIndexConfigPtr kkvIndexConfig = DYNAMIC_POINTER_CAST(KKVIndexConfig, indexSchema->GetPrimaryKeyIndexConfig());
    assert(kkvIndexConfig);
    string skey = rawDoc->GetField(kkvIndexConfig->GetSuffixFieldName());
    string pkey = rawDoc->GetField(kkvIndexConfig->GetPrefixFieldName());
    KKVKeysExtractorPtr keysExtractor(new KKVKeysExtractor(kkvIndexConfig));
    uint64_t pkeyHash;
    keysExtractor->HashPrefixKey(pkey, pkeyHash);
    doc->SetPKeyHash(pkeyHash);
    doc->SetPkField(autil::StringView(kkvIndexConfig->GetKeyFieldName()), autil::StringView(pkey));
    if (skey != RawDocument::EMPTY_STRING) {
        uint64_t skeyHash;
        keysExtractor->HashSuffixKey(skey, skeyHash);
        doc->SetSKeyHash(skeyHash);
    }
}

void DocumentCreator::SetDocumentValue(config::IndexPartitionSchemaPtr schema, RawDocumentPtr rawDoc,
                                       DocumentPtr document, autil::mem_pool::Pool* pool)
{
    NormalDocumentPtr doc = DYNAMIC_POINTER_CAST(NormalDocument, document);
    regionid_t regionId = doc->GetRegionId();

    ExtendDocField2IndexDocField convert(schema, regionId);
    FieldSchemaPtr fieldSchema = schema->GetFieldSchema(regionId);
    assert(fieldSchema);
    AttributeSchemaPtr attrSchema = schema->GetAttributeSchema(regionId);
    IndexSchemaPtr indexSchema = schema->GetIndexSchema(regionId);
    SummarySchemaPtr summarySchema = schema->GetSummarySchema(regionId);

    SummaryDocumentPtr summaryDoc(new SummaryDocument);

    for (FieldSchema::Iterator it = fieldSchema->Begin(); it != fieldSchema->End(); ++it) {
        fieldid_t fieldId = (*it)->GetFieldId();

        if (indexSchema && indexSchema->IsInIndex(fieldId)) {
            convert.convertIndexField(doc, *it, rawDoc);
        }

        if (attrSchema && attrSchema->IsInAttribute(fieldId)) {
            convert.convertAttributeField(doc, *it, rawDoc);
        } else if (summarySchema && summarySchema->IsInSummary(fieldId)) {
            convert.convertSummaryField(summaryDoc, *it, rawDoc);
        }
    }

    indexlib::document::SerializedSummaryDocumentPtr serSummaryDoc;
    if (schema->NeedStoreSummary()) {
        indexlib::document::SummaryFormatter summaryFormatter(summarySchema);
        summaryFormatter.SerializeSummaryDoc(summaryDoc, serSummaryDoc);
    }
    doc->SetSummaryDocument(serSummaryDoc);

    SourceSchemaPtr sourceSchema = schema->GetSourceSchema();
    if (sourceSchema) {
        groupid_t groupId = 0;
        SourceDocumentPtr srcDocument(new SourceDocument(pool));
        for (auto iter = sourceSchema->Begin(); iter != sourceSchema->End(); iter++, groupId++) {
            vector<string> fieldNames = (*iter)->GetSpecifiedFields();
            for (auto fn : fieldNames) {
                if (!rawDoc->Exist(fn)) {
                    srcDocument->AppendNonExistField(groupId, fn);
                    continue;
                }
                string value = rawDoc->GetField(fn);
                srcDocument->Append(groupId, fn, StringView(value), true);
            }
        }
        SerializedSourceDocumentPtr serDoc(new SerializedSourceDocument);
        SourceDocumentFormatter formatter;
        formatter.Init(sourceSchema);
        formatter.SerializeSourceDocument(srcDocument, pool, serDoc);
        doc->SetSourceDocument(serDoc);
    }

    string modifyFields = rawDoc->GetField(RESERVED_MODIFY_FIELDS);
    convert.convertModifyFields(doc, modifyFields);
}

void DocumentCreator::SetKVDocumentValue(config::IndexPartitionSchemaPtr schema, RawDocumentPtr rawDoc,
                                         document::KVIndexDocument* doc, bool useMultiIndex)
{
    regionid_t regionId = doc->GetRegionId();

    auto normalDoc = std::make_shared<document::NormalDocument>();
    normalDoc->SetAttributeDocument(std::make_shared<document::AttributeDocument>());
    normalDoc->SetDocOperateType(doc->GetDocOperateType());

    ExtendDocField2IndexDocField convert(schema, regionId);
    FieldSchemaPtr fieldSchema = schema->GetFieldSchema(regionId);
    AttributeSchemaPtr attrSchema = schema->GetAttributeSchema(regionId);

    for (FieldSchema::Iterator it = fieldSchema->Begin(); it != fieldSchema->End(); ++it) {
        fieldid_t fieldId = (*it)->GetFieldId();

        if (attrSchema && attrSchema->IsInAttribute(fieldId)) {
            convert.convertAttributeField(normalDoc, *it, rawDoc);
        }
    }

    // TODO(xinfei.sxf) add update field for simple value
    if (schema->GetTableType() == tt_kv) {
        auto indexSchema = schema->GetIndexSchema(DEFAULT_REGIONID);
        if (useMultiIndex) {
            indexSchema = schema->GetIndexSchema(regionId);
        }
        const auto& pkConfig = indexSchema->GetPrimaryKeyIndexConfig();
        auto kvConfig = static_cast<config::KVIndexConfig*>(pkConfig.get());
        const auto& valueConfig = kvConfig->GetValueConfig();
        if (valueConfig->IsSimpleValue() && (useMultiIndex || schema->GetRegionCount() == 1u)) {
            const config::AttributeConfigPtr& attrConfig = valueConfig->GetAttributeConfig(0);
            auto simpleValueFieldId = attrConfig->GetFieldId();
            auto fieldConfig = attrConfig->GetFieldConfig();
            doc->SetValue(normalDoc->GetAttributeDocument()->GetField(simpleValueFieldId));
            return;
        }
    }

    auto packAttrAppender = std::make_shared<MultiRegionPackAttributeAppender>();
    autil::mem_pool::Pool pool;
    packAttrAppender->Init(schema);
    if (doc->GetDocOperateType() == ADD_DOC) {
        auto ret = packAttrAppender->AppendPackAttribute(normalDoc->GetAttributeDocument(), &pool, regionId);
        if (!ret) {
            IE_LOG(WARN, "append pack attribute failed");
        }
        doc->SetValue(normalDoc->GetAttributeDocument()->GetPackField(0));
    } else {
        assert(doc->GetDocOperateType() == UPDATE_FIELD);
        auto ret = packAttrAppender->EncodePatchValues(normalDoc->GetAttributeDocument(), &pool, regionId);
        if (!ret) {
            IE_LOG(WARN, "encode patch values failed");
        }
        doc->SetValue(normalDoc->GetAttributeDocument()->GetPackField(0));
    }
}

vector<NormalDocumentPtr> DocumentCreator::CreateNormalDocuments(const config::IndexPartitionSchemaPtr& schema,
                                                                 const string& docString,
                                                                 bool needRewriteSectionAttribute)
{
    vector<string> docStrings = StringUtil::split(docString, DocumentParser::DP_CMD_SEPARATOR);
    vector<NormalDocumentPtr> docs;
    for (size_t i = 0; i < docStrings.size(); ++i) {
        NormalDocumentPtr doc = CreateNormalDocument(schema, docStrings[i], needRewriteSectionAttribute);
        assert(doc);
        docs.push_back(doc);
    }
    return docs;
}

vector<document::KVDocumentPtr> DocumentCreator::CreateKVDocuments(const config::IndexPartitionSchemaPtr& schema,
                                                                   const string& docString,
                                                                   const string& defaultRegionName, bool useMultiIndex)
{
    vector<string> docStrings = StringUtil::split(docString, DocumentParser::DP_CMD_SEPARATOR);
    vector<document::KVDocumentPtr> docs;
    for (size_t i = 0; i < docStrings.size(); ++i) {
        auto doc = CreateKVDocument(schema, docStrings[i], defaultRegionName, useMultiIndex);
        assert(doc);
        docs.push_back(doc);
    }
    return docs;
}

document::KVDocumentPtr DocumentCreator::CreateKVDocument(const config::IndexPartitionSchemaPtr& schema,
                                                          const string& docString, const string& defaultRegionName,
                                                          bool useMultiIndex)
{
    RawDocumentPtr rawDoc = DocumentParser::Parse(docString);
    assert(rawDoc);
    return CreateKVDocument(schema, rawDoc, defaultRegionName, useMultiIndex);
}

document::KVDocumentPtr DocumentCreator::CreateKVDocument(const config::IndexPartitionSchemaPtr& schema,
                                                          RawDocumentPtr rawDoc, const string& defaultRegionName,
                                                          bool useMultiIndex)
{
    assert(schema);
    TableType tableType = schema->GetTableType();
    if (tableType != tt_kv && tableType != tt_kkv) {
        INDEXLIB_FATAL_ERROR(BadParameter, "cannot create kv document for non-kv table");
    }

    auto kvDoc = std::make_shared<document::KVDocument>();
    indexlibv2::framework::Locator locator;
    locator.Deserialize(rawDoc->GetLocator().ToString());
    kvDoc->SetLocator(locator);
    kvDoc->SetDocOperateType(ADD_DOC);
    auto doc = kvDoc->CreateOneDoc();
    doc->SetDocOperateType(rawDoc->GetDocOperateType());
    doc->SetTimestamp(rawDoc->GetTimestamp());
    doc->SetUserTimestamp(rawDoc->GetTimestamp());
    int64_t userTimestamp =
        autil::StringUtil::strToInt64WithDefault(rawDoc->GetField(HA_RESERVED_TIMESTAMP).c_str(), INVALID_TIMESTAMP);
    if (userTimestamp != INVALID_TIMESTAMP) {
        doc->SetUserTimestamp(userTimestamp);
    }
    kvDoc->SetTimestamp(rawDoc->GetTimestamp());
    kvDoc->SetDocInfo({0, rawDoc->GetTimestamp(), 0});

    regionid_t regionId = ExtractRegionIdFromRawDocument(schema, rawDoc, defaultRegionName);
    if (regionId == INVALID_REGIONID) {
        kvDoc->SetDocOperateType(SKIP_DOC);
        return kvDoc;
    }
    doc->SetRegionId(regionId);
    auto str = rawDoc->GetField("index_name_hash");
    if (!str.empty()) {
        auto indexNameHash = std::stoul(str);
        doc->SetIndexNameHash(indexNameHash);
    } else {
        doc->SetIndexNameHash(0); // 当doc上没有indexNameHash时，设为默认值0
    }
    if (schema->TTLEnabled(regionId) && schema->TTLFromDoc(regionId)) {
        uint32_t ttl = 0;
        const auto& ttlFieldName = schema->GetTTLFieldName(regionId);
        auto ttlField = rawDoc->GetField(ttlFieldName);
        if (!ttlField.empty() && autil::StringUtil::strToUInt32(ttlField.data(), ttl)) {
            doc->SetTTL(ttl);
        }
    }

    SetKVPrimaryKey(schema, rawDoc, doc);

    DocOperateType op = doc->GetDocOperateType();
    if (op == ADD_DOC || op == UPDATE_FIELD) {
        SetKVDocumentValue(schema, rawDoc, doc, useMultiIndex);
    }
    return kvDoc;
}

NormalDocumentPtr DocumentCreator::CreateNormalDocument(const config::IndexPartitionSchemaPtr& schema,
                                                        RawDocumentPtr rawDoc, autil::mem_pool::Pool* pool)
{
    NormalDocumentPtr doc(new NormalDocument);
    doc->SetDocOperateType(rawDoc->GetDocOperateType());
    doc->SetTimestamp(rawDoc->GetTimestamp());
    doc->SetDocInfo({0, rawDoc->GetTimestamp(), 0});
    doc->SetUserTimestamp(
        autil::StringUtil::strToInt64WithDefault(rawDoc->GetField(HA_RESERVED_TIMESTAMP).c_str(), INVALID_TIMESTAMP));
    doc->SetLocator(rawDoc->GetLocator());
    IndexDocumentPtr indexDoc(new IndexDocument(doc->GetPool()));
    doc->SetIndexDocument(indexDoc);
    AttributeDocumentPtr attrDoc;
    attrDoc.reset(new AttributeDocument);
    doc->SetAttributeDocument(attrDoc);
    doc->SetSchemaVersionId(schema->GetSchemaVersionId());

    SetPrimaryKey(schema, rawDoc, doc);

    DocOperateType op = doc->GetDocOperateType();
    if (op == ADD_DOC || op == UPDATE_FIELD || op == DELETE_DOC) {
        SetDocumentValue(schema, rawDoc, doc, pool);
    }
    return doc;
}

regionid_t DocumentCreator::ExtractRegionIdFromRawDocument(const IndexPartitionSchemaPtr& schema,
                                                           const RawDocumentPtr& rawDocument,
                                                           const string& defaultRegionName)
{
    if (schema->GetRegionCount() == 1) {
        return DEFAULT_REGIONID;
    }

    const string& field = rawDocument->GetField(RESERVED_REGION_NAME);

    if (field.empty() && !defaultRegionName.empty()) {
        return schema->GetRegionId(defaultRegionName);
    }
    return schema->GetRegionId(field);
}

std::string DocumentCreator::GetDocumentTypeStr(DocOperateType opType)
{
    switch (opType) {
    case ADD_DOC:
        return "add";
    case UPDATE_FIELD:
        return "update_field";
    case DELETE_DOC:
        return "delete";
    case DELETE_SUB_DOC:
        return "delete_sub";
    default:
        assert(false);
    }
    return "";
}

}} // namespace indexlib::test
