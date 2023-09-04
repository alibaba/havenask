#include "indexlib/config/field_schema.h"
#include "indexlib/config/summary_schema.h"
#include "indexlib/document/document_rewriter/pack_attribute_appender.h"
#include "indexlib/document/index_document/normal_document/normal_document.h"
#include "indexlib/document/index_document/normal_document/summary_formatter.h"
#include "indexlib/document/kv_document/kv_document.h"
#include "indexlib/test/document_creator.h"
#include "indexlib/test/schema_maker.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/util/Exception.h"
#include "indexlib/util/KeyHasherTyped.h"

using namespace std;
using namespace autil;
using namespace autil::mem_pool;

using namespace indexlib::config;
using namespace indexlib::partition;
using namespace indexlib::common;
using namespace indexlib::test;
using namespace indexlib::util;

namespace indexlib { namespace document {

// TODO(hanyao)

class KVDocumentTest : public INDEXLIB_TESTBASE
{
public:
    KVDocumentTest() {}
    ~KVDocumentTest() {}

    DECLARE_CLASS_NAME(KVDocumentTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;

    void TestCaseSerializeKKVDoc();
    void TestCaseSerializeKVDoc();
    void TestCaseSerializeKVDocWithTag();
    void TestCaseSerializeKKVDocWithTag();
    // void TestSerializeLegacyVersion();
    // void TestNullField();

    // private:
    //     static void CreateDocument(std::vector<NormalDocumentPtr>& docs);
    //     static NormalDocumentPtr CreateDocument(uint32_t idx);
    //     static void CreateFields(const std::vector<fieldid_t>& fieldIds, std::vector<IndexTokenizeField*>& fieldVec,
    //                              autil::mem_pool::Pool* pool);

    //     void SerializeInLegacyFormat(uint32_t docVersionId, NormalDocument& doc, autil::DataBuffer& dataBuffer,
    //                                  bool needSetDocVersion = true);

    //     void CheckSerializeLegacyVersion(uint32_t version, const NormalDocumentPtr& doc);

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(KVDocumentTest, TestCaseSerializeKKVDoc);
INDEXLIB_UNIT_TEST_CASE(KVDocumentTest, TestCaseSerializeKVDoc);
INDEXLIB_UNIT_TEST_CASE(KVDocumentTest, TestCaseSerializeKVDocWithTag);
INDEXLIB_UNIT_TEST_CASE(KVDocumentTest, TestCaseSerializeKKVDocWithTag);
// INDEXLIB_UNIT_TEST_CASE(KVDocumentTest, TestSerializeLegacyVersion);
// INDEXLIB_UNIT_TEST_CASE(KVDocumentTest, TestNullField);

void KVDocumentTest::CaseSetUp() {}

void KVDocumentTest::CaseTearDown() {}

// TODO: add a test case: deserialize from legacy normal document

void KVDocumentTest::TestCaseSerializeKKVDoc()
{
    string field = "pkey:string;skey:int32;value:uint32;";
    IndexPartitionSchemaPtr schema = SchemaMaker::MakeKKVSchema(field, "pkey", "skey", "value;skey;");
    string docString = "cmd=add,pkey=pkey1,skey=1,value=1,ts=101;";
    auto doc = DocumentCreator::CreateKVDocument(schema, docString, /*region*/ "");
    auto kvDoc = DYNAMIC_POINTER_CAST(KVDocument, doc);
    autil::DataBuffer dataBuffer;
    dataBuffer.write(kvDoc);
    KVDocumentPtr newDoc;
    dataBuffer.read(newDoc);
    ASSERT_TRUE(*kvDoc == *newDoc);
}

void KVDocumentTest::TestCaseSerializeKVDoc()
{
    string field = "key:int32;value:uint64;";
    config::IndexPartitionSchemaPtr schema = SchemaMaker::MakeKVSchema(field, "key", "value");
    {
        string docString = "cmd=add,key=10,value=100,ts=101;";
        auto doc = DocumentCreator::CreateKVDocument(schema, docString, /*region*/ "");
        auto kvDoc = DYNAMIC_POINTER_CAST(KVDocument, doc);
        autil::DataBuffer dataBuffer;
        dataBuffer.write(kvDoc);
        KVDocumentPtr newDoc;
        dataBuffer.read(newDoc);
        ASSERT_TRUE(*kvDoc == *newDoc);
        ASSERT_EQ(1u, newDoc->GetMessageCount());
        ASSERT_EQ(1u, newDoc->GetDocumentCount());
    }
}

void KVDocumentTest::TestCaseSerializeKVDocWithTag()
{
    string field = "key:int32;value:uint64;";
    config::IndexPartitionSchemaPtr schema = SchemaMaker::MakeKVSchema(field, "key", "value");
    {
        string docString = "cmd=add,key=10,value=100,ts=101;";
        auto doc = DocumentCreator::CreateKVDocument(schema, docString, /*region*/ "");
        auto kvDoc = DYNAMIC_POINTER_CAST(KVDocument, doc);
        kvDoc->AddTag("_source_timestamp_tag_", "f1|1000;");
        autil::DataBuffer dataBuffer;
        dataBuffer.write(kvDoc);
        KVDocumentPtr newDoc;
        dataBuffer.read(newDoc);
        // ASSERT_TRUE(*kvDoc == *newDoc);
        ASSERT_EQ(1u, newDoc->GetMessageCount());
        ASSERT_EQ(1u, newDoc->GetDocumentCount());
        ASSERT_EQ("f1|1000;", newDoc->GetTag("_source_timestamp_tag_"));
    }
}

void KVDocumentTest::TestCaseSerializeKKVDocWithTag()
{
    string field = "pkey:string;skey:int32;value:uint32;";
    IndexPartitionSchemaPtr schema = SchemaMaker::MakeKKVSchema(field, "pkey", "skey", "value;skey;");
    string docString = "cmd=add,pkey=pkey1,skey=1,value=1,ts=101;";
    auto doc = DocumentCreator::CreateKVDocument(schema, docString, /*region*/ "");
    auto kvDoc = DYNAMIC_POINTER_CAST(KVDocument, doc);
    kvDoc->AddTag("timestamp_f1", "10000");
    autil::DataBuffer dataBuffer;
    dataBuffer.write(kvDoc);
    KVDocumentPtr newDoc;
    dataBuffer.read(newDoc);
    ASSERT_TRUE(*kvDoc == *newDoc);
    ASSERT_EQ("10000", newDoc->GetTag("timestamp_f1"));
}

// TODO: test deserialize from legacy normal document
// void KVDocumentTest::TestSerializeLegacyVersion()
// {
//     string field = "key:int32;value:uint64;";
//     config::IndexPartitionSchemaPtr schema =
//         SchemaMaker::MakeKVSchema(field, "key", "value");

//     string docString = "cmd=add,key=10,value=100,ts=101;";
//     NormalDocumentPtr doc = DocumentCreator::CreateKVDocument(schema, docString);

//     CheckSerializeLegacyVersion(3, doc);
//     CheckSerializeLegacyVersion(4, doc);
//     CheckSerializeLegacyVersion(5, doc);
//     CheckSerializeLegacyVersion(6, doc);
//     CheckSerializeLegacyVersion(7, doc);

//     doc = DocumentCreator::CreateKVDocument(schema, docString, true);
//     CheckSerializeLegacyVersion(3, doc);
//     CheckSerializeLegacyVersion(4, doc);
//     CheckSerializeLegacyVersion(5, doc);
//     CheckSerializeLegacyVersion(6, doc);
//     CheckSerializeLegacyVersion(7, doc);
// }

// void KVDocumentTest::SerializeInLegacyFormat(uint32_t docVersionId, NormalDocument& doc, DataBuffer& dataBuffer,
//                                              bool needSetDocVersion)
// {
//     if (needSetDocVersion) {
//         doc.SetSerializedVersion(docVersionId);
//     }
//     dataBuffer.write(docVersionId);

//     assert(doc.mIndexDocument);
//     dataBuffer.write(doc.mIndexDocument);
//     dataBuffer.write(doc.mAttributeDocument);
//     dataBuffer.write(doc.mSummaryDocument);
//     dataBuffer.write(doc.mSubDocuments);
//     dataBuffer.write(doc.mModifiedFields);
//     dataBuffer.write(doc.mSubModifiedFields);
//     dataBuffer.write(doc.mTimestamp);
//     if (docVersionId >= 5) {
//         bool isValidUserTS = doc.mUserTimestamp != INVALID_TIMESTAMP;
//         dataBuffer.write(isValidUserTS);
//         if (isValidUserTS) {
//             dataBuffer.write(doc.mUserTimestamp);
//         }
//     }
//     if (docVersionId >= 6) {
//         dataBuffer.write(doc.mRegionId);
//     }
//     dataBuffer.write(doc.mSource);
//     dataBuffer.write(doc.mLocator);
//     dataBuffer.write(doc.mOpType);
//     dataBuffer.write(doc.mOriginalOpType);

//     if (docVersionId >= 4) {
//         dataBuffer.write(doc.mKVIndexDocument);
//     }
//     if (docVersionId >= 7) {
//         dataBuffer.write(doc.mTrace);
//     }
// }

// NormalDocumentPtr KVDocumentTest::CreateDocument(uint32_t idx)
// {
//     NormalDocumentPtr doc(new NormalDocument());
//     doc->SetDocOperateType(ADD_DOC);
//     doc->SetTimestamp(idx);
//     doc->SetSource(StringUtil::toString(idx));
//     doc->SetRegionId(idx);
//     doc->SetTrace(true);

//     autil::mem_pool::Pool* pool = doc->GetPool();
//     uint32_t docId = idx;
//     const uint32_t fieldCount = FIELD_COUNT + idx;
//     vector<fieldid_t> fieldIdVec;
//     for (uint32_t j = 0; j < fieldCount; ++j) {
//         fieldIdVec.push_back((fieldid_t)(j * 2));
//     }

//     vector<IndexTokenizeField*> fieldVec;
//     CreateFields(fieldIdVec, fieldVec, pool);
//     IndexDocumentPtr indexDoc(new IndexDocument(pool));
//     indexDoc->SetDocId(docId);
//     for (uint32_t j = 0; j < fieldVec.size(); ++j) {
//         indexDoc->AddField(fieldVec[j]);
//     }

//     SummaryDocumentPtr sumDoc(new SummaryDocument(pool));
//     sumDoc->SetDocId(docId);
//     SummarySchemaPtr summarySchema(new SummarySchema);
//     AttributeDocumentPtr attDoc(new AttributeDocument(pool));
//     attDoc->SetDocId(docId);
//     for (uint32_t j = 0; j < fieldCount; ++j) {
//         stringstream ss;
//         ss << "test field" << j;
//         sumDoc->SetField(j, autil::StringView(ss.str(), pool));
//         FieldConfigPtr fieldConfig(new FieldConfig(ss.str(), ft_text, false));
//         fieldConfig->SetFieldId(j);
//         std::shared_ptr<SummaryConfig> summaryConfig(new SummaryConfig());
//         summaryConfig->SetFieldConfig(fieldConfig);
//         summarySchema->AddSummaryConfig(summaryConfig);
//         ss.clear();
//         ss << j;
//         attDoc->SetField(j, autil::StringView(ss.str(), pool));
//     }
//     summarySchema->SetNeedStoreSummary(true);
//     doc->SetIndexDocument(indexDoc);
//     doc->SetAttributeDocument(attDoc);
//     SummaryFormatter formatter(summarySchema);
//     SerializedSummaryDocumentPtr serSummaryDoc;
//     formatter.SerializeSummaryDoc(sumDoc, serSummaryDoc);
//     doc->SetSummaryDocument(serSummaryDoc);
//     doc->AddModifiedField(idx);
//     doc->AddSubModifiedField(idx + 1);
//     return doc;
// }

// void KVDocumentTest::CreateDocument(vector<NormalDocumentPtr>& docs)
// {
//     docs.clear();
//     for (uint32_t i = 0; i < DOC_COUNT; ++i) {
//         docs.push_back(CreateDocument(i));
//     }
// }

// void KVDocumentTest::CreateFields(const vector<fieldid_t>& fieldIds, vector<IndexTokenizeField*>& fieldVec,
//                                   autil::mem_pool::Pool* pool)
// {
//     vector<Section*> sectionVec;
//     for (uint32_t i = 0; i < fieldIds.size(); i++) {
//         Section* section = IE_POOL_COMPATIBLE_NEW_CLASS(pool, Section, 8, pool);
//         DefaultHasher hasher;
//         uint64_t hashKey;
//         for (size_t j = 0; j < 3; ++j) {
//             hasher.GetHashKey("hello", hashKey);
//             section->CreateToken(hashKey, j, j);
//         }
//         sectionVec.push_back(section);
//     }

//     for (uint32_t i = 0; i < (uint32_t)fieldIds.size(); ++i) {
//         auto field = IE_POOL_COMPATIBLE_NEW_CLASS(pool, IndexTokenizeField, pool);
//         field->AddSection(sectionVec[i]);
//         field->SetFieldId(fieldIds[i]);
//         fieldVec.push_back(field);
//     }
// }

// void KVDocumentTest::CheckSerializeLegacyVersion(uint32_t version, const NormalDocumentPtr& doc)
// {
//     doc->SetSerializedVersion(version);
//     autil::DataBuffer buffer;
//     doc->serialize(buffer);
//     string str = string(buffer.getData(), buffer.getDataLen());

//     autil::DataBuffer dataBuffer(const_cast<char*>(str.data()), str.length());
//     ASSERT_EQ((int32_t)str.size(), dataBuffer.getDataLen());
//     NormalDocumentPtr deserializeDoc(new NormalDocument);
//     deserializeDoc->deserialize(dataBuffer);
//     ASSERT_EQ(version, deserializeDoc->GetSerializedVersion());
//     ASSERT_EQ(0, dataBuffer.getDataLen());
// }

}} // namespace indexlib::document
