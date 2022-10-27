#include "indexlib/partition/segment/test/in_memory_segment_modifier_unittest.h"
#include "indexlib/index/normal/attribute/accessor/string_attribute_writer.h"
#include "indexlib/index/normal/attribute/accessor/in_memory_attribute_segment_writer.h"
#include "indexlib/index/normal/deletionmap/deletion_map_segment_writer.h"
#include "indexlib/test/schema_maker.h"
#include "indexlib/test/document_creator.h"

using namespace std;
using namespace autil;

IE_NAMESPACE_USE(test);
IE_NAMESPACE_USE(index);
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(document);

IE_NAMESPACE_BEGIN(partition);
IE_LOG_SETUP(partition, InMemorySegmentModifierTest);

namespace
{
class MockInMemoryAttributeSegmentWriter: public InMemoryAttributeSegmentWriter
{
public:
    MockInMemoryAttributeSegmentWriter()
        : InMemoryAttributeSegmentWriter(false)
    {}

    MOCK_CONST_METHOD3(CreateAttributeWriter, AttributeWriterPtr(
                    const config::AttributeConfigPtr& attrConfig,
                    const config::IndexPartitionOptions& options,
                    util::BuildResourceMetrics* buildResourceMetrics));
};
DEFINE_SHARED_PTR(MockInMemoryAttributeSegmentWriter);

class MockAttributeWriter : public StringAttributeWriter
{
public:
    MockAttributeWriter(config::AttributeConfigPtr attrConfig)
        : StringAttributeWriter(attrConfig)
    {}

    MOCK_METHOD2(Init, void(const FSWriterParamDeciderPtr& fsWriterParamDecider,
                            util::BuildResourceMetrics* buildResourceMetrics));
    MOCK_METHOD2(UpdateField, bool(docid_t docId, 
                                   const autil::ConstString& attributeValue));
};
DEFINE_SHARED_PTR(MockAttributeWriter);
};

InMemorySegmentModifierTest::InMemorySegmentModifierTest()
{
}

InMemorySegmentModifierTest::~InMemorySegmentModifierTest()
{
}

void InMemorySegmentModifierTest::CaseSetUp()
{
    string field = "string1:string;biz30day:int32;price:uint32";
    string index = "pk:primarykey64:string1";
    string attribute = "biz30day;price";
    mSchema = SchemaMaker::MakeSchema(field, index, attribute, "");
}

void InMemorySegmentModifierTest::CaseTearDown()
{
}

void InMemorySegmentModifierTest::TestUpdateDocument()
{
    IndexPartitionOptions options;
    MockInMemoryAttributeSegmentWriterPtr mockWriter(
            new MockInMemoryAttributeSegmentWriter);
    const AttributeSchemaPtr& attrSchema = mSchema->GetAttributeSchema();

    vector<MockAttributeWriter*> mockAttrWriters;
    for (size_t i = 0; i < attrSchema->GetAttributeCount(); i++)
    {
        MockAttributeWriter* mockAttrWriter = new MockAttributeWriter(
                AttributeConfigPtr());
        mockAttrWriters.push_back(mockAttrWriter);
    }
    assert(mockAttrWriters.size() == 2);
    EXPECT_CALL(*mockWriter, CreateAttributeWriter(_,_,_))
        .WillOnce(Return(MockAttributeWriterPtr(mockAttrWriters[0])))
        .WillOnce(Return(MockAttributeWriterPtr(mockAttrWriters[1])));
    mockWriter->Init(mSchema, options);

    MockAttributeWriterPtr mockPriceWriter = DYNAMIC_POINTER_CAST(
            MockAttributeWriter, mockWriter->GetAttributeWriter("price"));
    MockAttributeWriterPtr mockBiz30dayWriter = DYNAMIC_POINTER_CAST(
            MockAttributeWriter, mockWriter->GetAttributeWriter("biz30day"));
    ASSERT_TRUE(mockPriceWriter);
    ASSERT_TRUE(mockBiz30dayWriter);

    InMemorySegmentModifier modifier;
    modifier.Init(DeletionMapSegmentWriterPtr(), mockWriter);
    string docString = "cmd=update_field,string1=hello,price=2;"
                       "cmd=update_field,string1=hello,price=3,biz30day=10;"
                       "cmd=update_field,string1=hello;";
    vector<NormalDocumentPtr> docs = 
        DocumentCreator::CreateDocuments(mSchema, docString);

    // update one field
    EXPECT_CALL(*mockPriceWriter, UpdateField(0, _))
        .WillOnce(Return(true));
    ASSERT_TRUE(modifier.UpdateDocument(0, docs[0]));

    // update two field
    EXPECT_CALL(*mockPriceWriter, UpdateField(0, _))
        .WillOnce(Return(true));
    EXPECT_CALL(*mockBiz30dayWriter, UpdateField(0, _))
        .WillOnce(Return(true));
    ASSERT_TRUE(modifier.UpdateDocument(0, docs[1]));

    // ignore updateField false
    EXPECT_CALL(*mockPriceWriter, UpdateField(0, _))
        .WillOnce(Return(false));
    EXPECT_CALL(*mockBiz30dayWriter, UpdateField(0, _))
        .WillOnce(Return(true));
    ASSERT_TRUE(modifier.UpdateDocument(0, docs[1]));

    EXPECT_CALL(*mockPriceWriter, UpdateField(0, _))
        .WillOnce(Return(true));
    EXPECT_CALL(*mockBiz30dayWriter, UpdateField(0, _))
        .WillOnce(Return(false));
    ASSERT_TRUE(modifier.UpdateDocument(0, docs[1]));

    // no attribute updated
    ASSERT_FALSE(modifier.UpdateDocument(0, docs[2]));
}

void InMemorySegmentModifierTest::TestUpdateEncodedFieldValue()
{
    IndexPartitionOptions options;
    MockInMemoryAttributeSegmentWriterPtr mockWriter(
            new MockInMemoryAttributeSegmentWriter);
    const AttributeSchemaPtr& attrSchema = mSchema->GetAttributeSchema();

    vector<MockAttributeWriter*> mockAttrWriters;
    for (size_t i = 0; i < attrSchema->GetAttributeCount(); i++)
    {
        MockAttributeWriter* mockAttrWriter = new MockAttributeWriter(
                AttributeConfigPtr());
        mockAttrWriters.push_back(mockAttrWriter);
    }
    assert(mockAttrWriters.size() == 2);
    EXPECT_CALL(*mockWriter, CreateAttributeWriter(_,_,_))
        .WillOnce(Return(MockAttributeWriterPtr(mockAttrWriters[0])))
        .WillOnce(Return(MockAttributeWriterPtr(mockAttrWriters[1])));
    mockWriter->Init(mSchema, options);

    MockAttributeWriterPtr mockPriceWriter = DYNAMIC_POINTER_CAST(
            MockAttributeWriter, mockWriter->GetAttributeWriter("price"));
    MockAttributeWriterPtr mockBiz30dayWriter = DYNAMIC_POINTER_CAST(
            MockAttributeWriter, mockWriter->GetAttributeWriter("biz30day"));
    ASSERT_TRUE(mockPriceWriter);
    ASSERT_TRUE(mockBiz30dayWriter);

    InMemorySegmentModifier modifier;
    modifier.Init(DeletionMapSegmentWriterPtr(), mockWriter);

    ConstString value;
    // update price, fieldId = 2
    EXPECT_CALL(*mockPriceWriter, UpdateField(0, _))
        .WillOnce(Return(true));
    ASSERT_TRUE(modifier.UpdateEncodedFieldValue(0, 2, value));

    // update biz30day, fieldId = 1
    EXPECT_CALL(*mockBiz30dayWriter, UpdateField(0, _))
        .WillOnce(Return(true));
    ASSERT_TRUE(modifier.UpdateEncodedFieldValue(0, 1, value));

    // update not attribute
    ASSERT_FALSE(modifier.UpdateEncodedFieldValue(0, 0, value));
}

void InMemorySegmentModifierTest::TestRemoveDocument()
{
    DeletionMapSegmentWriterPtr deletionmapSegWriter(
            new DeletionMapSegmentWriter());
    deletionmapSegWriter->Init(10);
    
    InMemorySegmentModifier modifier;
    modifier.Init(deletionmapSegWriter, InMemoryAttributeSegmentWriterPtr());

    ASSERT_FALSE(deletionmapSegWriter->IsDirty());
    modifier.RemoveDocument(0);

    ASSERT_TRUE(deletionmapSegWriter->IsDirty());
    ASSERT_EQ((uint32_t)1, deletionmapSegWriter->GetDeletedCount());
    modifier.RemoveDocument(1);
    ASSERT_TRUE(deletionmapSegWriter->IsDirty());
    ASSERT_EQ((uint32_t)2, deletionmapSegWriter->GetDeletedCount());
}

IE_NAMESPACE_END(partition);

