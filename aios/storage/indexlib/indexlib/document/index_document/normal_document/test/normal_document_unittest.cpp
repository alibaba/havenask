#include "indexlib/document/index_document/normal_document/test/normal_document_unittest.h"

#include "indexlib/common/field_format/attribute/single_value_attribute_convertor.h"
#include "indexlib/config/field_schema.h"
#include "indexlib/config/summary_schema.h"
#include "indexlib/config/test/schema_maker.h"
#include "indexlib/document/document_rewriter/pack_attribute_appender.h"
#include "indexlib/document/index_document/normal_document/index_tokenize_field.h"
#include "indexlib/document/index_document/normal_document/summary_formatter.h"
#include "indexlib/document/index_document/normal_document/test/document_serialize_test_helper.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/test/document_creator.h"
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

static const uint32_t DOC_COUNT = 10;
static const uint32_t FIELD_COUNT = 3;

void NormalDocumentTest::CaseSetUp() {}

void NormalDocumentTest::CaseTearDown() {}

void NormalDocumentTest::TestCaseForOperatorEqual()
{
    vector<NormalDocumentPtr> docs1;
    vector<NormalDocumentPtr> docs2;
    CreateDocument(docs1);
    CreateDocument(docs2);
    for (uint32_t i = 0; i < DOC_COUNT; ++i) {
        INDEXLIB_TEST_TRUE(*docs1[i] == *docs2[i]);
        for (uint32_t j = 0; j < DOC_COUNT; ++j) {
            if (i == j) {
                INDEXLIB_TEST_TRUE(*docs1[i] == *docs2[j]);
            } else {
                INDEXLIB_TEST_TRUE(*docs1[i] != *docs2[j]);
            }
        }
    }
}

void NormalDocumentTest::TestCaseForCompatibleHighVersion()
{
    IndexPartitionSchemaPtr schema = SchemaMaker::MakeSchema(
        "pk:string;demo_field:raw;string1:string",
        "pk:primarykey64:pk;test_customize:customized:demo_field;index1:string:string1", "", "");
    string docString = "cmd=add,pk=pk0,demo_field=hello,string1=world,ts=1";
    NormalDocumentPtr doc = DocumentCreator::CreateNormalDocument(schema, docString);
    ASSERT_TRUE(doc);
    AttributeDocumentPtr attDoc(new AttributeDocument());
    doc->SetAttributeDocument(attDoc);
    for (size_t i = 0; i < doc->GetSubDocuments().size(); ++i) {
        doc->GetSubDocuments()[i]->SetAttributeDocument(attDoc);
    }
    NormalDocumentPtr originalDoc = DYNAMIC_POINTER_CAST(NormalDocument, doc);

    // for compatibility
    uint32_t serializeVersion = LEGACY_DOCUMENT_BINARY_VERSION + 1;
    autil::DataBuffer dataBuffer;
    originalDoc->serializeVersion7(dataBuffer);
    originalDoc->serializeVersion8(dataBuffer);
    originalDoc->serializeVersion9(dataBuffer);
    originalDoc->serializeVersion10(dataBuffer);
    string appendValue = "appendField";
    dataBuffer.write(appendValue);
    dataBuffer.write(appendValue);
    dataBuffer.write(appendValue);

    NormalDocumentPtr newDoc(new NormalDocument());
    newDoc->DoDeserialize(dataBuffer, serializeVersion);
    ASSERT_TRUE(*originalDoc == *newDoc);
}

void NormalDocumentTest::TestCaseSerializeDocument()
{
    vector<NormalDocumentPtr> docs1;
    vector<NormalDocumentPtr> subdocs1;
    vector<NormalDocumentPtr> docs2;
    CreateDocument(docs1);
    CreateDocument(subdocs1);
    for (size_t i = 0; i < docs1.size(); i++) {
        for (size_t j = 0; j < subdocs1.size(); j++) {
            docs1[i]->AddSubDocument(subdocs1[j]);
        }
    }
    autil::DataBuffer dataBuffer;
    dataBuffer.write(docs1);
    dataBuffer.read(docs2);

    INDEXLIB_TEST_EQUAL(docs1.size(), docs2.size());
    for (size_t i = 0; i < docs1.size(); ++i) {
        TEST_DOCUMENT_SERIALIZE_EQUAL((*docs1[i]), (*docs2[i]));
        vector<NormalDocumentPtr> subDocs1 = docs1[i]->GetSubDocuments();
        vector<NormalDocumentPtr> subDocs2 = docs2[i]->GetSubDocuments();
        INDEXLIB_TEST_EQUAL(subDocs1.size(), subDocs2.size());
        for (size_t j = 0; j < subDocs1.size(); j++) {
            TEST_DOCUMENT_SERIALIZE_EQUAL((*subDocs1[j]), (*subDocs2[j]));
        }
    }
}

void NormalDocumentTest::TestCaseSerializeDocumentWithIndexRawFields()
{
    IndexPartitionSchemaPtr schema = SchemaMaker::MakeSchema(
        "pk:string;demo_field:raw;string1:string",
        "pk:primarykey64:pk;test_customize:customized:demo_field;index1:string:string1", "", "");
    string docString = "cmd=add,pk=pk0,demo_field=hello,string1=world,ts=1";
    NormalDocumentPtr doc = DocumentCreator::CreateNormalDocument(schema, docString);
    ASSERT_TRUE(doc);
    AttributeDocumentPtr attDoc(new AttributeDocument());
    doc->SetAttributeDocument(attDoc);
    for (size_t i = 0; i < doc->GetSubDocuments().size(); ++i) {
        doc->GetSubDocuments()[i]->SetAttributeDocument(attDoc);
    }

    NormalDocumentPtr normalDoc = DYNAMIC_POINTER_CAST(NormalDocument, doc);
    autil::DataBuffer dataBuffer;
    dataBuffer.write(normalDoc);
    NormalDocumentPtr newDoc;
    dataBuffer.read(newDoc);
    ASSERT_TRUE(*normalDoc == *newDoc);
}

void NormalDocumentTest::TestCaseForDeserializeLegacyDocument()
{
    string value1 = "123456";
    string value2 = "abcdef";

    NormalDocument doc;
    Pool* pool = doc.GetPool();
    IndexDocumentPtr indexDoc(new IndexDocument(pool));
    indexDoc->SetPrimaryKey("abc");
    doc.SetIndexDocument(indexDoc);

    AttributeDocumentPtr attrDoc(new AttributeDocument(pool));
    attrDoc->SetField(10, StringView(value1));
    attrDoc->SetField(40, StringView(value2));
    doc.SetAttributeDocument(attrDoc);
    SerializedSummaryDocumentPtr summaryDoc(new SerializedSummaryDocument);
    char* str = new char[3];
    summaryDoc->SetSerializedDoc(str, 3);
    doc.SetSummaryDocument(summaryDoc);
    doc.SetTrace(true);
    // TODO: Set schema version below and fix compare logic.
    // doc.SetSchemaVersionId(999);

    {
        // legacy 1, current 10
        DataBuffer dataBuffer;
        SerializeInLegacyFormat(1, doc, dataBuffer);

        NormalDocument serializeDoc;
        ASSERT_THROW(serializeDoc.deserialize(dataBuffer), util::DocumentDeserializeException);
    }

    {
        // legacy 3, current 10
        DataBuffer dataBuffer;
        SerializeInLegacyFormat(3, doc, dataBuffer);

        NormalDocument serializeDoc;
        serializeDoc.deserialize(dataBuffer);
        EXPECT_TRUE(doc == serializeDoc);
    }

    {
        // legacy 4, current 10
        DataBuffer dataBuffer;
        SerializeInLegacyFormat(4, doc, dataBuffer);

        NormalDocument serializeDoc;
        serializeDoc.deserialize(dataBuffer);
        EXPECT_TRUE(doc == serializeDoc);
    }

    {
        // legacy 5, current 10
        DataBuffer dataBuffer;
        SerializeInLegacyFormat(5, doc, dataBuffer);

        NormalDocument serializeDoc;
        serializeDoc.deserialize(dataBuffer);
        EXPECT_TRUE(doc == serializeDoc);
    }

    {
        // legacy 6, current 10
        DataBuffer dataBuffer;
        SerializeInLegacyFormat(6, doc, dataBuffer);

        NormalDocument serializeDoc;
        serializeDoc.deserialize(dataBuffer);
        EXPECT_TRUE(doc == serializeDoc);
        ASSERT_FALSE(serializeDoc.mTrace);
    }

    {
        // legacy 7, current 10
        DataBuffer dataBuffer;
        SerializeInLegacyFormat(7, doc, dataBuffer);
        NormalDocument serializeDoc;
        ASSERT_NO_THROW(serializeDoc.deserialize(dataBuffer));
        EXPECT_TRUE(doc == serializeDoc);
        ASSERT_TRUE(doc == serializeDoc);
    }

    {
        // legacy 8, current 10
        DataBuffer dataBuffer;
        SerializeInLegacyFormat(8, doc, dataBuffer);
        NormalDocument serializeDoc;
        ASSERT_NO_THROW(serializeDoc.deserialize(dataBuffer));
    }

    {
        // legacy 9, current 10
        DataBuffer dataBuffer;
        SerializeInLegacyFormat(9, doc, dataBuffer);
        NormalDocument serializeDoc;
        ASSERT_NO_THROW(serializeDoc.deserialize(dataBuffer));
    }

    {
        DataBuffer dataBuffer;
        ASSERT_THROW(SerializeInLegacyFormat(LEGACY_DOCUMENT_BINARY_VERSION + 1, doc, dataBuffer),
                     util::BadParameterException);
        SerializeInLegacyFormat(LEGACY_DOCUMENT_BINARY_VERSION + 1, doc, dataBuffer, false);
        NormalDocument serializeDoc;
        ASSERT_NO_THROW(serializeDoc.deserialize(dataBuffer));
    }

    {
        string legacyDocPath = GET_PRIVATE_TEST_DATA_PATH() + "/3_0_document";
        // legacy format document is generated by the following code in branch release/3.0
        // auto doc = CreateDocument(0);
        // DataBuffer dataBuffer;
        // dataBuffer.write(doc);
        // file_system::FslibWrapper::AtomicStoreE(legacyDocPath,
        //         string(dataBuffer.getStart(), dataBuffer.getDataLen()));
        docid_t docId = 0;
        NormalDocumentPtr originalDoc = CreateDocument(docId);
        // for compatibility
        originalDoc->SetSerializedVersion(4);
        NormalDocumentPtr newDoc;
        string docStr;
        file_system::FslibWrapper::AtomicLoadE(legacyDocPath, docStr);
        autil::DataBuffer dataBuffer((void*)docStr.c_str(), docStr.length());
        ASSERT_NO_THROW(dataBuffer.read(newDoc));
        newDoc->SetDocId(docId);
        ASSERT_TRUE(*originalDoc == *newDoc);
    }
}

void NormalDocumentTest::SerializeInLegacyFormat(uint32_t docVersionId, NormalDocument& doc, DataBuffer& dataBuffer,
                                                 bool needSetDocVersion)
{
    if (needSetDocVersion) {
        doc.SetSerializedVersion(docVersionId);
    }
    dataBuffer.write(docVersionId);

    assert(doc.mIndexDocument);
    dataBuffer.write(doc.mIndexDocument);
    dataBuffer.write(doc.mAttributeDocument);
    dataBuffer.write(doc.mSummaryDocument);
    dataBuffer.write(doc.mSubDocuments);
    dataBuffer.write(doc.mModifiedFields);
    dataBuffer.write(doc.mSubModifiedFields);
    dataBuffer.write(doc.mTimestamp);
    if (docVersionId >= 5) {
        bool isValidUserTS = doc.mUserTimestamp != INVALID_TIMESTAMP;
        dataBuffer.write(isValidUserTS);
        if (isValidUserTS) {
            dataBuffer.write(doc.mUserTimestamp);
        }
    }
    if (docVersionId >= 6) {
        dataBuffer.write(doc.mRegionId);
    }
    dataBuffer.write(doc.TagInfoToString());
    dataBuffer.write(doc.mLocator);
    dataBuffer.write(doc.mOpType);
    dataBuffer.write(doc.mOriginalOpType);

    if (docVersionId >= 4) {
        dataBuffer.write(doc.mKVIndexDocument);
    }
    if (docVersionId >= 7) {
        dataBuffer.write(doc.mSchemaId);
        dataBuffer.write(doc.mTrace);
    }
    if (docVersionId >= 8 && doc.mAttributeDocument) {
        dataBuffer.write(doc.mAttributeDocument->_nullFields);
    }
    if (docVersionId >= 9) {
        dataBuffer.write(doc.mSourceDocument);
    }
    if (docVersionId >= 10 and doc.mIndexDocument) {
        dataBuffer.write(doc.mIndexDocument->_modifiedTokens);
    }
}

NormalDocumentPtr NormalDocumentTest::CreateDocument(uint32_t idx)
{
    NormalDocumentPtr doc(new NormalDocument());
    doc->SetDocOperateType(ADD_DOC);
    doc->SetTimestamp(idx);
    doc->SetSource(StringUtil::toString(idx));
    doc->SetRegionId(idx);
    doc->SetTrace(true);

    autil::mem_pool::Pool* pool = doc->GetPool();
    uint32_t docId = idx;
    const uint32_t fieldCount = FIELD_COUNT + idx;
    vector<fieldid_t> fieldIdVec;
    for (uint32_t j = 0; j < fieldCount; ++j) {
        fieldIdVec.push_back((fieldid_t)(j * 2));
    }

    vector<IndexTokenizeField*> fieldVec;
    CreateFields(fieldIdVec, fieldVec, pool);
    IndexDocumentPtr indexDoc(new IndexDocument(pool));
    indexDoc->SetDocId(docId);
    for (uint32_t j = 0; j < fieldVec.size(); ++j) {
        indexDoc->AddField(fieldVec[j]);
    }

    SummaryDocumentPtr sumDoc(new SummaryDocument(pool));
    sumDoc->SetDocId(docId);
    SummarySchemaPtr summarySchema(new SummarySchema);
    AttributeDocumentPtr attDoc(new AttributeDocument(pool));
    attDoc->SetDocId(docId);
    for (uint32_t j = 0; j < fieldCount; ++j) {
        stringstream ss;
        ss << "test field" << j;
        sumDoc->SetField(j, autil::MakeCString(ss.str(), pool));
        FieldConfigPtr fieldConfig(new FieldConfig(ss.str(), ft_text, false));
        fieldConfig->SetFieldId(j);
        std::shared_ptr<SummaryConfig> summaryConfig(new SummaryConfig());
        summaryConfig->SetFieldConfig(fieldConfig);
        summarySchema->AddSummaryConfig(summaryConfig);
        ss.clear();
        ss << j;
        attDoc->SetField(j, autil::MakeCString(ss.str(), pool));
    }
    summarySchema->SetNeedStoreSummary(true);
    doc->SetIndexDocument(indexDoc);
    doc->SetAttributeDocument(attDoc);
    SummaryFormatter formatter(summarySchema);
    SerializedSummaryDocumentPtr serSummaryDoc;
    formatter.SerializeSummaryDoc(sumDoc, serSummaryDoc);
    doc->SetSummaryDocument(serSummaryDoc);
    doc->AddModifiedField(idx);
    doc->AddSubModifiedField(idx + 1);
    return doc;
}

void NormalDocumentTest::CreateDocument(vector<NormalDocumentPtr>& docs)
{
    docs.clear();
    for (uint32_t i = 0; i < DOC_COUNT; ++i) {
        docs.push_back(CreateDocument(i));
    }
}

void NormalDocumentTest::CreateFields(const vector<fieldid_t>& fieldIds, vector<IndexTokenizeField*>& fieldVec,
                                      autil::mem_pool::Pool* pool)
{
    vector<Section*> sectionVec;
    for (uint32_t i = 0; i < fieldIds.size(); i++) {
        Section* section = IE_POOL_COMPATIBLE_NEW_CLASS(pool, Section, 8, pool);
        DefaultHasher hasher;
        uint64_t hashKey;
        for (size_t j = 0; j < 3; ++j) {
            string str("hello");
            hasher.GetHashKey(str.c_str(), str.length(), hashKey);
            section->CreateToken(hashKey, j, j);
        }
        sectionVec.push_back(section);
    }

    for (uint32_t i = 0; i < (uint32_t)fieldIds.size(); ++i) {
        auto field = IE_POOL_COMPATIBLE_NEW_CLASS(pool, IndexTokenizeField, pool);
        field->AddSection(sectionVec[i]);
        field->SetFieldId(fieldIds[i]);
        fieldVec.push_back(field);
    }
}

void NormalDocumentTest::CheckSerializeLegacyVersion(uint32_t version, const NormalDocumentPtr& doc)
{
    doc->SetSerializedVersion(version);
    autil::DataBuffer buffer;
    doc->serialize(buffer);
    string str = string(buffer.getData(), buffer.getDataLen());

    autil::DataBuffer dataBuffer(const_cast<char*>(str.data()), str.length());
    ASSERT_EQ((int32_t)str.size(), dataBuffer.getDataLen());
    NormalDocumentPtr deserializeDoc(new NormalDocument);
    deserializeDoc->deserialize(dataBuffer);
    ASSERT_EQ(version, deserializeDoc->GetSerializedVersion());
    ASSERT_EQ(0, dataBuffer.getDataLen());
}

void NormalDocumentTest::TestNullField()
{
    IndexPartitionSchemaPtr schema =
        SchemaMaker::MakeSchema("pk:string;demo_field:raw;string1:string;score1:float;score2:int64",
                                "pk:primarykey64:pk;test_customize:customized:demo_field;index1:string:string1",
                                "pk;string1;score1;score2", "");
    string docString = "cmd=add,pk=pk0,demo_field=hello,string1=world,ts=1,score1=10.1,score2=100";
    NormalDocumentPtr doc = DocumentCreator::CreateNormalDocument(schema, docString);
    doc->GetAttributeDocument()->SetNullField(0);
    doc->GetAttributeDocument()->SetNullField(2);
    ASSERT_TRUE(doc);

    autil::DataBuffer dataBuffer;
    dataBuffer.write(doc);

    NormalDocumentPtr newDoc(new NormalDocument);
    dataBuffer.read(newDoc);
    set<fieldid_t> expectedNullFields = {0, 2};
    ASSERT_EQ(expectedNullFields, newDoc->GetAttributeDocument()->_nullFields);
    ASSERT_TRUE(*doc == *newDoc);
}

void NormalDocumentTest::TestSerializeSourceDocument()
{
    NormalDocumentPtr doc1;
    doc1 = CreateDocument(0);

    string tmp1 = "f1,f2";
    StringView meta(tmp1);
    SerializedSourceDocumentPtr srcDoc(new SerializedSourceDocument(NULL));
    srcDoc->SetMeta(meta);
    string tmp2 = "v1,v2";
    StringView groupData(tmp2);
    srcDoc->SetGroupValue(0, groupData);
    doc1->SetSourceDocument(srcDoc);

    {
        autil::DataBuffer dataBuffer;
        dataBuffer.write(doc1);

        NormalDocumentPtr doc2(new NormalDocument);
        dataBuffer.read(doc2);

        ASSERT_TRUE(doc2->mSourceDocument);
        ASSERT_EQ(meta, doc2->mSourceDocument->GetMeta());
        ASSERT_EQ(groupData, doc2->mSourceDocument->GetGroupValue(0));

        TEST_DOCUMENT_SERIALIZE_EQUAL((*doc1), (*doc2));
    }
    {
        // old binary read new version doc
        autil::DataBuffer dataBuffer;
        doc1->DoSerialize(dataBuffer, 9);

        NormalDocumentPtr doc2(new NormalDocument);
        doc2->DoDeserialize(dataBuffer, 8);
        ASSERT_FALSE(doc2->mSourceDocument);
        TEST_DOCUMENT_SERIALIZE_EQUAL((*doc1), (*doc2));
    }
}

void NormalDocumentTest::TestAddTag()
{
    NormalDocumentPtr doc;
    doc = CreateDocument(INVALID_DOCID);
    doc->SetSource("test_source");
    doc->AddTag(DOCUMENT_HASHID_TAG_KEY, "12345");
    autil::DataBuffer dataBuffer;
    dataBuffer.write(doc);

    NormalDocumentPtr newDoc(new NormalDocument);
    dataBuffer.read(newDoc);
    ASSERT_EQ(string("12345"), newDoc->GetTag(DOCUMENT_HASHID_TAG_KEY));
    ASSERT_EQ(autil::StringView("test_source"), newDoc->GetSource());

    ASSERT_EQ(*doc, *newDoc);
    doc->AddTag(DOCUMENT_HASHID_TAG_KEY, "1234");
    ASSERT_NE(*doc, *newDoc);

    std::map<std::string, std::string> expectTagMap;
    expectTagMap[DOCUMENT_HASHID_TAG_KEY] = "1234";
    expectTagMap[DOCUMENT_SOURCE_TAG_KEY] = "test_source";
    const auto& tagMap = doc->GetTagInfoMap();
    ASSERT_EQ(expectTagMap, tagMap);
}

void NormalDocumentTest::TestSpecialTag()
{
    NormalDocumentPtr doc;
    doc = CreateDocument(INVALID_DOCID);
    doc->SetSource("test_source");
    doc->AddTag(DOCUMENT_HASHID_TAG_KEY, "");
    autil::DataBuffer dataBuffer;
    dataBuffer.write(doc);

    NormalDocumentPtr newDoc(new NormalDocument);
    dataBuffer.read(newDoc);
    ASSERT_EQ(string(""), newDoc->GetTag(DOCUMENT_HASHID_TAG_KEY));
    ASSERT_EQ(autil::StringView("test_source"), newDoc->GetSource());
}

void NormalDocumentTest::TestSerializeModifiedTokens()
{
    NormalDocumentPtr doc1;
    doc1 = CreateDocument(0);

    const auto& indexDoc = doc1->GetIndexDocument();
    assert(indexDoc);
    indexDoc->PushModifiedToken(0, 0, ModifiedTokens::Operation::REMOVE);
    indexDoc->SetNullTermModifiedOperation(0, ModifiedTokens::Operation::ADD);

    {
        autil::DataBuffer dataBuffer;
        dataBuffer.write(doc1);

        NormalDocumentPtr doc2(new NormalDocument);
        dataBuffer.read(doc2);
        auto indexDoc = doc2->GetIndexDocument();
        ASSERT_TRUE(indexDoc);
        auto tokens = indexDoc->GetFieldModifiedTokens(0);
        ASSERT_TRUE(tokens);
        ASSERT_EQ(1u, tokens->NonNullTermSize());
        auto tokenPair = (*tokens)[0];
        ASSERT_EQ(0, tokenPair.first);
        ASSERT_EQ(ModifiedTokens::Operation::REMOVE, tokenPair.second);
        ModifiedTokens::Operation op;
        ASSERT_TRUE(tokens->GetNullTermOperation(&op));
        ASSERT_EQ(ModifiedTokens::Operation::ADD, op);
        TEST_DOCUMENT_SERIALIZE_EQUAL((*doc1), (*doc2));
    }
    {
        // old binary read new version doc
        autil::DataBuffer dataBuffer;
        doc1->DoSerialize(dataBuffer, 10);

        NormalDocumentPtr doc2(new NormalDocument);
        doc2->DoDeserialize(dataBuffer, 8);
        TEST_DOCUMENT_SERIALIZE_EQUAL((*doc1), (*doc2));
    }
}

}} // namespace indexlib::document
