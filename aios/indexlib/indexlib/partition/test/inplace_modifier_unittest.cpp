#include <autil/StringUtil.h>
#include "indexlib/partition/test/inplace_modifier_unittest.h"
#include "indexlib/partition/online_partition_reader.h"
#include "indexlib/index_base/segment/segment_writer.h"
#include "indexlib/index/normal/primarykey/primary_key_index_reader_typed.h"
#include "indexlib/test/schema_maker.h"
#include "indexlib/test/partition_data_maker.h"
#include "indexlib/test/single_field_partition_data_provider.h"
#include "indexlib/document/index_document/normal_document/normal_document.h"

using namespace std;
using namespace autil;
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(document);
IE_NAMESPACE_USE(index);
IE_NAMESPACE_USE(test);
IE_NAMESPACE_USE(index_base);

IE_NAMESPACE_BEGIN(partition);
IE_LOG_SETUP(partition, InplaceModifierTest);

InplaceModifierTest::InplaceModifierTest()
{
}

InplaceModifierTest::~InplaceModifierTest()
{
}

void InplaceModifierTest::CaseSetUp()
{
    mRootDir = GET_TEST_DATA_PATH();
}

void InplaceModifierTest::CaseTearDown()
{
}

void InplaceModifierTest::TestSimpleProcess()
{

    SingleFieldPartitionDataProvider provider;
    provider.Init(mRootDir, "int32", SFP_ATTRIBUTE);
    provider.Build("0,1,2,3,4#5,6,7", SFP_OFFLINE);
    IndexPartitionSchemaPtr schema = provider.GetSchema();
    PartitionDataPtr partitionData = provider.GetPartitionData();
    InMemorySegmentPtr inMemSegment = partitionData->CreateNewSegment();
    IndexPartitionOptions options;
    IndexPartitionReaderPtr reader(
            new OnlinePartitionReader(options, schema, util::SearchCachePartitionWrapperPtr()));
    reader->Open(partitionData);
    InplaceModifier modifier(schema);
    modifier.Init(reader, inMemSegment);

    NormalDocumentPtr document(new NormalDocument);
    IndexDocumentPtr indexDocument(new IndexDocument(document->GetPool()));
    document->SetIndexDocument(indexDocument);

    indexDocument->SetPrimaryKey("0");
    modifier.RemoveDocument(document);

    indexDocument->SetPrimaryKey("10");
    modifier.RemoveDocument(document);

    DeletionMapReaderPtr deletionmapReader = 
        partitionData->GetDeletionMapReader();
    ASSERT_TRUE(deletionmapReader->IsDeleted(0));
    ASSERT_TRUE(deletionmapReader->IsDeleted(10));
    ASSERT_FALSE(deletionmapReader->IsDeleted(1));
}

void InplaceModifierTest::TestDeleteDocument()
{
    SingleFieldPartitionDataProvider provider;
    provider.Init(mRootDir, "int32", SFP_PK_INDEX);
    provider.Build("0,1,2,3", SFP_OFFLINE);

    PartitionDataPtr partitionData = provider.GetPartitionData();
    InMemorySegmentPtr inMemSegment = partitionData->CreateNewSegment();
    IndexPartitionSchemaPtr schema = provider.GetSchema();
    InplaceModifier modifier(schema);

    IndexPartitionOptions options;
    IndexPartitionReaderPtr onlineReader(
            new OnlinePartitionReader(options, schema, util::SearchCachePartitionWrapperPtr()));
    onlineReader->Open(partitionData);
    modifier.Init(onlineReader, inMemSegment);

    DeletionMapReaderPtr reader = partitionData->GetDeletionMapReader();
    CheckDeletionMap(reader, "0,1,2,3", false);

    //check delete built and building doc
    vector<NormalDocumentPtr> docs = provider.CreateDocuments(
            "delete:0,add:4,delete:4");

    SegmentWriterPtr segWriter = inMemSegment->GetSegmentWriter();
    ASSERT_TRUE(modifier.RemoveDocument(docs[0]));
    CheckDeletionMap(reader, "0", true);
    CheckDeletionMap(reader, "1,2,3", false);

    segWriter->AddDocument(docs[1]);
    CheckDeletionMap(reader, "1,2,3,4", false);

    ASSERT_TRUE(modifier.RemoveDocument(docs[2]));
    CheckDeletionMap(reader, "0,4", true);
    CheckDeletionMap(reader, "1,2,3", false);

    //check delete fail
    docs = provider.CreateDocuments("delete:5,delete:1");
    //check remove non exist doc
    ASSERT_FALSE(modifier.RemoveDocument(docs[0]));

    //check update doc without pk
    docs[1]->SetIndexDocument(IndexDocumentPtr());
    ASSERT_FALSE(modifier.RemoveDocument(docs[1]));
}

void InplaceModifierTest::TestUpdateDocument()
{
    SingleFieldPartitionDataProvider provider;
    provider.Init(mRootDir, "int32", SFP_ATTRIBUTE);
    provider.Build("0,1,2,3", SFP_OFFLINE);

    AttributeConfigPtr attrConfig = provider.GetAttributeConfig();
    PartitionDataPtr partitionData = provider.GetPartitionData();
    InMemorySegmentPtr inMemSegment = partitionData->CreateNewSegment();
    IndexPartitionSchemaPtr schema = provider.GetSchema();
    InplaceModifier modifier(schema);

    IndexPartitionOptions options;
    IndexPartitionReaderPtr partReader(
            new OnlinePartitionReader(options, schema, util::SearchCachePartitionWrapperPtr()));
    partReader->Open(partitionData);
    modifier.Init(partReader, inMemSegment);

    //check update built and building doc
    vector<NormalDocumentPtr> docs = provider.CreateDocuments(
            "update_field:0:3,add:4,update_field:4:6");

    SingleValueAttributeReader<int32_t> reader;
    reader.Open(attrConfig, partitionData);

    SegmentWriterPtr segWriter = inMemSegment->GetSegmentWriter();
    ASSERT_TRUE(modifier.UpdateDocument(docs[0]));
    segWriter->AddDocument(docs[1]);
    ASSERT_TRUE(modifier.UpdateDocument(docs[2]));

    CheckAttribute(reader, 0, 3);
    CheckAttribute(reader, 4, 6);

    //check update fail
    docs = provider.CreateDocuments(
            "update_field:5:3,update_field:0:6,update_field:1:7");
    //check update non exist doc
    ASSERT_FALSE(modifier.UpdateDocument(docs[0]));

    //check update doc without pk
    docs[1]->SetIndexDocument(IndexDocumentPtr());
    ASSERT_FALSE(modifier.UpdateDocument(docs[1]));

    //check update doc without attribute doc
    docs[1]->SetAttributeDocument(AttributeDocumentPtr());
    ASSERT_FALSE(modifier.UpdateDocument(docs[1]));
}

void InplaceModifierTest::CheckAttribute(
        const AttributeReader& reader,
        docid_t docid, int32_t expectValue)
{    
    int32_t value;
    reader.Read(docid, (uint8_t*)&value, sizeof(value));
    ASSERT_EQ(expectValue, value);
}

void InplaceModifierTest::CheckDeletionMap(
        const DeletionMapReaderPtr& reader,
        const string& docIdStrs, bool isDeleted)
{
    vector<docid_t> docIds;
    StringUtil::fromString(docIdStrs, docIds, ",");
    for (size_t i = 0; i < docIds.size(); i++)
    {
        ASSERT_EQ(isDeleted, reader->IsDeleted(docIds[i]));
    }
}

void InplaceModifierTest::TestUpdateDocumentWithoutSchema()
{
    SingleFieldPartitionDataProvider provider;
    provider.Init(mRootDir, "text", SFP_INDEX);
    provider.Build("0", SFP_OFFLINE);
    provider.Build("", SFP_REALTIME);

    PartitionDataPtr partitionData = provider.GetPartitionData();
    InMemorySegmentPtr inMemSegment = partitionData->CreateNewSegment();
    IndexPartitionSchemaPtr schema = provider.GetSchema();

    IndexPartitionOptions options;
    IndexPartitionReaderPtr partReader(
            new OnlinePartitionReader(options, schema, util::SearchCachePartitionWrapperPtr()));
    partReader->Open(partitionData);
    InplaceModifier modifier(schema);
    modifier.Init(partReader, inMemSegment);
    provider.Build("1", SFP_REALTIME);

    NormalDocumentPtr doc(new NormalDocument);    
    doc->SetDocOperateType(UPDATE_FIELD);
    IndexDocumentPtr indexDoc(new IndexDocument(doc->GetPool()));
    doc->SetIndexDocument(indexDoc);
    AttributeDocumentPtr attrDoc(new AttributeDocument);
    doc->SetAttributeDocument(attrDoc);

    indexDoc->SetPrimaryKey("0");
    ASSERT_FALSE(modifier.UpdateDocument(doc));
    indexDoc->SetPrimaryKey("1");
    ASSERT_FALSE(modifier.UpdateDocument(doc));
}

IE_NAMESPACE_END(partition);

