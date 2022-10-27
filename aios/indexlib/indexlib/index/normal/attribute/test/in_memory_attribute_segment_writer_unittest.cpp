#include "indexlib/index/normal/attribute/test/in_memory_attribute_segment_writer_unittest.h"
#include "indexlib/index/test/partition_schema_maker.h"
#include "indexlib/test/document_creator.h"
#include "indexlib/document/index_document/normal_document/normal_document.h"

using namespace std;
using namespace autil;
using namespace autil::mem_pool;

IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(common);
IE_NAMESPACE_USE(document);
IE_NAMESPACE_USE(util);
IE_NAMESPACE_USE(test);

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, InMemoryAttributeSegmentWriterTest);

InMemoryAttributeSegmentWriterTest::InMemoryAttributeSegmentWriterTest()
{
}

InMemoryAttributeSegmentWriterTest::~InMemoryAttributeSegmentWriterTest()
{
}

void InMemoryAttributeSegmentWriterTest::CaseSetUp()
{
    mSchema.reset(new IndexPartitionSchema);
    const string fieldSchemaStr =
        "pk:HASH_64;price:UINT32;weight:UINT32;equalCompressSinleValue:UINT32:false:false:equal";
    const string indexSchemaStr = "pk:primarykey64:pk;";
    const string attributeSchemaStr = "price;weight;equalCompressSinleValue";
    const string summarySchemaStr = "price;";
    PartitionSchemaMaker::MakeSchema(mSchema, fieldSchemaStr,
            indexSchemaStr, attributeSchemaStr, summarySchemaStr);
}

void InMemoryAttributeSegmentWriterTest::CaseTearDown()
{
}

void InMemoryAttributeSegmentWriterTest::TestInitWithEmptyParameters()
{
    {
        SCOPED_TRACE("empty schema and empty SegBaseDocIdInfoVect");
        InMemoryAttributeSegmentWriter writer;
        IndexPartitionSchemaPtr schema(new IndexPartitionSchema());
        writer.Init(schema, mOptions);
        ASSERT_EQ((size_t)0, writer.mAttrIdToAttributeWriters.size());
    }
    {
        InMemoryAttributeSegmentWriter writer;
        writer.Init(mSchema, mOptions);
        ASSERT_EQ((size_t)3, writer.mAttrIdToAttributeWriters.size());
        ASSERT_TRUE(writer.mAttrIdToAttributeWriters[0]);
        ASSERT_TRUE(writer.mAttrIdToAttributeWriters[1]);
        ASSERT_TRUE(writer.mAttrIdToAttributeWriters[2]);
    }
}

void InMemoryAttributeSegmentWriterTest::TestAddDocument()
{
    InMemoryAttributeSegmentWriter writer;
    writer.Init(mSchema, mOptions);
    ASSERT_EQ((size_t)3, writer.mAttrIdToAttributeWriters.size());
    const string docStr = "cmd=add,pk=200,price=200,weight=20";
    NormalDocumentPtr document = DocumentCreator::CreateDocument(mSchema,docStr);
    writer.AddDocument(document);
    
    CheckValue(writer, 0, "price", 200);
    CheckValue(writer, 0, "weight", 20);
}

void InMemoryAttributeSegmentWriterTest::TestUpdateDocument()
{
    InMemoryAttributeSegmentWriter writer;
    writer.Init(mSchema, mOptions);
    ASSERT_EQ((size_t)3, writer.mAttrIdToAttributeWriters.size());

    const string docStr =
        "cmd=add,pk=200,price=200,weight=200;"
        "cmd=update_field,pk=200,price=99,weight=99;"
        "cmd=update_field,pk=200,price=2000,weight=2001;"
        "cmd=update_field,pk=200,equalCompressSinleValue=2000;";
    vector<NormalDocumentPtr> docs = DocumentCreator::CreateDocuments(mSchema, docStr);
    writer.AddDocument(DYNAMIC_POINTER_CAST(NormalDocument, docs[0]));
    ASSERT_TRUE(writer.UpdateDocument(0, DYNAMIC_POINTER_CAST(NormalDocument, docs[1])));
    ASSERT_TRUE(writer.UpdateDocument(0, DYNAMIC_POINTER_CAST(NormalDocument, docs[2])));
    ASSERT_TRUE(writer.UpdateDocument(0, DYNAMIC_POINTER_CAST(NormalDocument, docs[3])));

    CheckValue(writer, 0, "price", 2000);
    CheckValue(writer, 0, "weight", 2001);
}

void InMemoryAttributeSegmentWriterTest::CheckValue(
        InMemoryAttributeSegmentWriter& writer, 
        docid_t docId, const string& attrName, uint32_t expectValue)
{
    InMemorySegmentReader::String2AttrReaderMap attrReaders;
    writer.CreateInMemSegmentReaders(attrReaders);
    AttributeSegmentReaderPtr segReader = attrReaders[attrName];
    ASSERT_TRUE(segReader);

    uint32_t value;
    segReader->Read(0, (uint8_t*)&value, sizeof(uint32_t));
    ASSERT_EQ(expectValue, value);
}

IE_NAMESPACE_END(index);

