#include "indexlib/document/index_document/normal_document/test/normal_document_unittest.h"
#include "indexlib/config/summary_schema.h"
#include "indexlib/document/index_document/normal_document/summary_formatter.h"
#include "indexlib/config/field_schema.h"
#include "indexlib/util/key_hasher_typed.h"
#include "indexlib/common/field_format/attribute/single_value_attribute_convertor.h"
#include "indexlib/document/index_document/normal_document/test/document_serialize_test_helper.h"
#include "indexlib/document/document_rewriter/pack_attribute_appender.h"
#include "indexlib/misc/exception.h"
#include "indexlib/test/document_creator.h"
#include "indexlib/test/schema_maker.h"

using namespace std;
using namespace autil;
using namespace autil::mem_pool;

IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(partition);
IE_NAMESPACE_USE(index);
IE_NAMESPACE_USE(common);
IE_NAMESPACE_USE(misc);
IE_NAMESPACE_USE(test);
IE_NAMESPACE_USE(util);

IE_NAMESPACE_BEGIN(document);

static const uint32_t DOC_COUNT = 10;
static const uint32_t FIELD_COUNT = 3;

void NormalDocumentTest::CaseSetUp()
{
}

void NormalDocumentTest::CaseTearDown()
{
}

void NormalDocumentTest::TestCaseForOperatorEqual()
{
    vector<NormalDocumentPtr> docs1;
    vector<NormalDocumentPtr> docs2;
    CreateDocument(docs1);
    CreateDocument(docs2);
    for (uint32_t i = 0; i < DOC_COUNT; ++i)
    {
        INDEXLIB_TEST_TRUE(*docs1[i] == *docs2[i]);
        for (uint32_t j = 0; j < DOC_COUNT; ++j)
        {
            if (i == j)
            {
                INDEXLIB_TEST_TRUE(*docs1[i] == *docs2[j]);
            }
            else
            {
                INDEXLIB_TEST_TRUE(*docs1[i] != *docs2[j]);
            }
        }
    }
}


void NormalDocumentTest::TestCaseForCompatibleHighVersion()
{
    IndexPartitionSchemaPtr schema = SchemaMaker::MakeSchema(
            "pk:string;demo_field:raw;string1:string",
            "pk:primarykey64:pk;test_customize:customized:demo_field;index1:string:string1",
            "",
            "");
    string docString = "cmd=add,pk=pk0,demo_field=hello,string1=world,ts=1";
    NormalDocumentPtr doc = DocumentCreator::CreateDocument(schema, docString);
    ASSERT_TRUE(doc);

    NormalDocumentPtr originalDoc = DYNAMIC_POINTER_CAST(NormalDocument, doc);
    // for compatibility
    uint32_t serializeVersion = DOCUMENT_BINARY_VERSION + 1;
    autil::DataBuffer dataBuffer;
    originalDoc->serializeVersion7(dataBuffer);
    string appendValue = "appendField";
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
    for (size_t i = 0; i < docs1.size(); i++)
    {
        for (size_t j = 0; j < subdocs1.size(); j++)
        {
            docs1[i]->AddSubDocument(subdocs1[j]);
        }
    }
    autil::DataBuffer dataBuffer;
    dataBuffer.write(docs1);
    dataBuffer.read(docs2);

    INDEXLIB_TEST_EQUAL(docs1.size(), docs2.size());
    for (size_t i = 0; i < docs1.size(); ++i)
    {
        TEST_DOCUMENT_SERIALIZE_EQUAL((*docs1[i]), (*docs2[i]));
        vector<NormalDocumentPtr> subDocs1 = docs1[i]->GetSubDocuments();
        vector<NormalDocumentPtr> subDocs2 = docs2[i]->GetSubDocuments();
        INDEXLIB_TEST_EQUAL(subDocs1.size(),
                            subDocs2.size());
        for (size_t j = 0; j < subDocs1.size(); j++)
        {
            TEST_DOCUMENT_SERIALIZE_EQUAL((*subDocs1[j]), (*subDocs2[j]));
        }
    }
}

void NormalDocumentTest::TestCaseSerializeDocumentWithIndexRawFields()
{
    IndexPartitionSchemaPtr schema = SchemaMaker::MakeSchema(
            "pk:string;demo_field:raw;string1:string",
            "pk:primarykey64:pk;test_customize:customized:demo_field;index1:string:string1",
            "",
            "");
    string docString = "cmd=add,pk=pk0,demo_field=hello,string1=world,ts=1";
    NormalDocumentPtr doc = DocumentCreator::CreateDocument(schema, docString);
    ASSERT_TRUE(doc);

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
    Pool *pool = doc.GetPool();
    IndexDocumentPtr indexDoc(new IndexDocument(pool));
    indexDoc->SetPrimaryKey("abc");
    doc.SetIndexDocument(indexDoc);

    AttributeDocumentPtr attrDoc(new AttributeDocument(pool));
    attrDoc->SetField(10, ConstString(value1));
    attrDoc->SetField(40, ConstString(value2));
    doc.SetAttributeDocument(attrDoc);
    SerializedSummaryDocumentPtr summaryDoc(new SerializedSummaryDocument);
    char* str = new char[3];
    summaryDoc->SetSerializedDoc(str, 3);
    doc.SetSummaryDocument(summaryDoc);
    doc.SetTrace(true);

    {
        // legacy 3, current 7
        DataBuffer dataBuffer;
        SerializeInLegacyFormat(3, doc, dataBuffer);

        NormalDocument serializeDoc;
        serializeDoc.deserialize(dataBuffer);
        ASSERT_TRUE(doc == serializeDoc);
    }

    {
        // legacy 4, current 7
        DataBuffer dataBuffer;
        SerializeInLegacyFormat(4, doc, dataBuffer);

        NormalDocument serializeDoc;
        serializeDoc.deserialize(dataBuffer);
        ASSERT_TRUE(doc == serializeDoc);
    }

    {
        // legacy 5, current 7
        DataBuffer dataBuffer;
        SerializeInLegacyFormat(5, doc, dataBuffer);

        NormalDocument serializeDoc;
        serializeDoc.deserialize(dataBuffer);
        ASSERT_TRUE(doc == serializeDoc);
    }

    {
        // legacy 6, current 7
        DataBuffer dataBuffer;
        SerializeInLegacyFormat(6, doc, dataBuffer);

        NormalDocument serializeDoc;
        serializeDoc.deserialize(dataBuffer);
        ASSERT_FALSE(serializeDoc.mTrace);
    }

    {
        // legacy 1, current 7
        DataBuffer dataBuffer;
        SerializeInLegacyFormat(1, doc, dataBuffer);

        NormalDocument serializeDoc;
        ASSERT_THROW(serializeDoc.deserialize(dataBuffer),
                     misc::DocumentDeserializeException);
    }

    {
        // legacy 8, current 7
        DataBuffer dataBuffer;
        ASSERT_THROW(SerializeInLegacyFormat(8, doc, dataBuffer), misc::BadParameterException);
        SerializeInLegacyFormat(8, doc, dataBuffer, false);

        NormalDocument serializeDoc;
        ASSERT_NO_THROW(serializeDoc.deserialize(dataBuffer));
    }

    {
        string legacyDocPath = TEST_DATA_PATH"/3_0_document";
        // legacy format document is generated by the following code in branch release/3.0
        // auto doc = CreateDocument(0);
        // DataBuffer dataBuffer;
        // dataBuffer.write(doc);
        // storage::FileSystemWrapper::AtomicStore(legacyDocPath,
        //         string(dataBuffer.getStart(), dataBuffer.getDataLen()));
        docid_t docId = 0;
        NormalDocumentPtr originalDoc = CreateDocument(docId);
        // for compatibility
        originalDoc->SetSerializedVersion(4);
        NormalDocumentPtr newDoc;
        string docStr;
        storage::FileSystemWrapper::AtomicLoad(legacyDocPath, docStr);
        autil::DataBuffer dataBuffer((void*)docStr.c_str(), docStr.length());
        ASSERT_NO_THROW(dataBuffer.read(newDoc));
        newDoc->SetDocId(docId);
        ASSERT_TRUE(*originalDoc == *newDoc);
    }
}

void NormalDocumentTest::SerializeInLegacyFormat(
        uint32_t docVersionId, NormalDocument& doc,
        DataBuffer& dataBuffer, bool needSetDocVersion)
{
    if (needSetDocVersion)
    {
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
    if (docVersionId >= 5)
    {
        bool isValidUserTS = doc.mUserTimestamp != INVALID_TIMESTAMP;
        dataBuffer.write(isValidUserTS);
        if (isValidUserTS)
        {
            dataBuffer.write(doc.mUserTimestamp);
        }
    }
    if (docVersionId >= 6)
    {
        dataBuffer.write(doc.mRegionId);
    }
    dataBuffer.write(doc.TagInfoToString());
    dataBuffer.write(doc.mLocator);
    dataBuffer.write(doc.mOpType);
    dataBuffer.write(doc.mOriginalOpType);

    if (docVersionId >= 4)
    {
        dataBuffer.write(doc.mKVIndexDocument);
    }
    if (docVersionId >= 7)
    {
        dataBuffer.write(doc.mTrace);
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

    autil::mem_pool::Pool *pool = doc->GetPool();
    uint32_t docId = idx;
    const uint32_t fieldCount = FIELD_COUNT + idx;
    vector<fieldid_t> fieldIdVec;
    for (uint32_t j = 0; j < fieldCount; ++j)
    {
        fieldIdVec.push_back((fieldid_t)(j * 2));
    }

    vector<IndexTokenizeField*> fieldVec;
    CreateFields(fieldIdVec, fieldVec, pool);
    IndexDocumentPtr indexDoc(new IndexDocument(pool));
    indexDoc->SetDocId(docId);
    for (uint32_t j = 0; j < fieldVec.size(); ++j)
    {
        indexDoc->AddField(fieldVec[j]);
    }

    SummaryDocumentPtr sumDoc(new SummaryDocument(pool));
    sumDoc->SetDocId(docId);
    SummarySchemaPtr summarySchema(new SummarySchema);
    AttributeDocumentPtr attDoc(new AttributeDocument(pool));
    attDoc->SetDocId(docId);
    for (uint32_t j = 0; j < fieldCount; ++j)
    {
        stringstream ss;
        ss << "test field" << j;
        sumDoc->SetField(j, autil::ConstString(ss.str(), pool));
        FieldConfigPtr fieldConfig(new FieldConfig(ss.str(), ft_text , false));
        fieldConfig->SetFieldId(j);
        SummaryConfigPtr summaryConfig(new SummaryConfig());
        summaryConfig->SetFieldConfig(fieldConfig);
        summarySchema->AddSummaryConfig(summaryConfig);
        ss.clear();
        ss << j;
        attDoc->SetField(j, autil::ConstString(ss.str(), pool));
    }
    summarySchema->SetNeedStoreSummary(true);
    doc->SetIndexDocument(indexDoc);
    doc->SetAttributeDocument(attDoc);
    SummaryFormatter formatter(summarySchema);
    SerializedSummaryDocumentPtr serSummaryDoc;
    formatter.SerializeSummaryDoc(sumDoc, serSummaryDoc);
    doc->SetSummaryDocument(serSummaryDoc);
    doc->AddModifiedField(idx);
    doc->AddSubModifiedField(idx+1);
    return doc;
}

void NormalDocumentTest::CreateDocument(vector<NormalDocumentPtr>& docs)
{
    docs.clear();
    for (uint32_t i = 0; i < DOC_COUNT; ++i)
    {
        docs.push_back(CreateDocument(i));
    }
}

void NormalDocumentTest::CreateFields(
        const vector<fieldid_t>& fieldIds,
        vector<IndexTokenizeField*>& fieldVec,
        autil::mem_pool::Pool* pool)
{
    vector<Section*> sectionVec;
    for (uint32_t i = 0; i < fieldIds.size(); i++)
    {
        Section* section = IE_POOL_COMPATIBLE_NEW_CLASS(pool, Section, 8, pool);
        DefaultHasher hasher;
        uint64_t hashKey;
        for (size_t j = 0; j < 3; ++j)
        {
            hasher.GetHashKey("hello", hashKey);
            section->CreateToken(hashKey,j,j);
        }
        sectionVec.push_back(section);
    }

    for (uint32_t i = 0; i < (uint32_t)fieldIds.size(); ++i)
    {
        auto field = IE_POOL_COMPATIBLE_NEW_CLASS(pool, IndexTokenizeField, pool);
        field->AddSection(sectionVec[i]);
        field->SetFieldId(fieldIds[i]);
        fieldVec.push_back(field);
    }
}

void NormalDocumentTest::CheckSerializeLegacyVersion(
        uint32_t version, const NormalDocumentPtr& doc)
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
    ASSERT_EQ(string("test_source"), newDoc->GetSource());

    ASSERT_EQ(*doc, *newDoc);
    doc->AddTag(DOCUMENT_HASHID_TAG_KEY, "1234");
    ASSERT_NE(*doc, *newDoc);

    auto iter = doc->CreateTagIterator();
    ASSERT_TRUE(iter.HasNext());
    string key,value;
    iter.Next(key, value);
    ASSERT_EQ(DOCUMENT_HASHID_TAG_KEY, key);
    ASSERT_EQ(string("1234"), value);

    ASSERT_TRUE(iter.HasNext());
    iter.Next(key, value);

    ASSERT_EQ(DOCUMENT_SOURCE_TAG_KEY, key);
    ASSERT_EQ(string("test_source"), value);
    ASSERT_FALSE(iter.HasNext());
}

IE_NAMESPACE_END(document);
