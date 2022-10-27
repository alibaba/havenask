#include <autil/StringTokenizer.h>
#include <autil/StringUtil.h>
#include "indexlib/common_define.h"
#include "indexlib/test/unittest.h"
#include "indexlib/index/normal/attribute/accessor/patch_attribute_modifier.h"
#include "indexlib/index/normal/attribute/accessor/attribute_segment_modifier.h"
#include "indexlib/index/normal/attribute/accessor/built_attribute_segment_modifier.h"
#include "indexlib/index/test/partition_schema_maker.h"
#include "indexlib/test/partition_data_maker.h"
#include "indexlib/test/document_creator.h"

using namespace std;
using namespace std::tr1;
using namespace autil;
using namespace autil::mem_pool;

IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(test);
IE_NAMESPACE_USE(document);
IE_NAMESPACE_USE(partition);
IE_NAMESPACE_USE(index_base);

IE_NAMESPACE_BEGIN(index);

namespace {
class MockPatchAttributeModifier : public PatchAttributeModifier
{
public:
    MockPatchAttributeModifier(const config::IndexPartitionSchemaPtr& schema)
        : PatchAttributeModifier(schema, NULL)
    {}

    MOCK_METHOD1(CreateBuiltSegmentModifier,
                 BuiltAttributeSegmentModifierPtr(segmentid_t segmentId));

};

class MockAttributeSegmentModifier : public BuiltAttributeSegmentModifier
{
public:
    MockAttributeSegmentModifier(const config::AttributeSchemaPtr& attrSchema)
        : BuiltAttributeSegmentModifier(0, attrSchema, NULL)
    {}

    MOCK_METHOD3(Update, void(docid_t docId, attrid_t attrId, 
                              const autil::ConstString& attrValue));
    MOCK_METHOD2(Dump, void(const file_system::DirectoryPtr& dir, 
                            segmentid_t srcSegment));
};

};

class PatchAttributeModifierTest : public INDEXLIB_TESTBASE
{
public:
    void CaseSetUp() override
    {
        mSchema.reset(new IndexPartitionSchema);
        PartitionSchemaMaker::MakeSchema(mSchema,
                "price:int32;title:text;sale:int32;nid:int64;count:int32",
                "nid:primarykey64:nid;title:text:title",
                "price;sale;count", "" );
    }

    void CaseTearDown() override
    {
    }

    void TestSimpleProcess()
    {         
        PartitionDataMaker::MakePartitionDataFiles(0, 0,
                GET_PARTITION_DIRECTORY(), "0,100,0;1,50,1;2,60,2");

        IndexPartitionOptions options;
        PartitionDataPtr partitionData = 
            PartitionDataMaker::CreatePartitionData(GET_FILE_SYSTEM(), options, mSchema);

        MockPatchAttributeModifier mockAttrModifier(mSchema);
        mockAttrModifier.Init(partitionData);

        MockAttributeSegmentModifier *mockSegModifier = 
            new MockAttributeSegmentModifier(mSchema->GetAttributeSchema());
        BuiltAttributeSegmentModifierPtr segModifier(mockSegModifier);

        NormalDocumentPtr document = DocumentCreator::CreateDocument(mSchema,  
                "cmd=update_field,sale=288,count=90");

        AttributeDocumentPtr attrDoc =
            DYNAMIC_POINTER_CAST(NormalDocument, document)->GetAttributeDocument();
        EXPECT_CALL(mockAttrModifier, 
                    CreateBuiltSegmentModifier(1))
            .WillOnce(Return(segModifier));
        EXPECT_CALL(*mockSegModifier, Update(1, 1, _))
            .Times(1);
        EXPECT_CALL(*mockSegModifier, Update(1, 2, _))
            .Times(1);
        mockAttrModifier.Update(101, attrDoc);

        // test dump
        EXPECT_CALL(*mockSegModifier, Dump(_,_))
            .Times(1);
        mockAttrModifier.Dump(GET_PARTITION_DIRECTORY());
    }

    void TestGetSegmentModifier()
    {
        PartitionDataMaker::MakePartitionDataFiles(0, 0,
                GET_PARTITION_DIRECTORY(), "0,100,0;1,50,1;2,60,2");

        IndexPartitionOptions options;
        PartitionDataPtr partitionData = 
            PartitionDataMaker::CreatePartitionData(GET_FILE_SYSTEM(), options, mSchema);

        PatchAttributeModifier attrModifier(mSchema, NULL);
        attrModifier.Init(partitionData);

        docid_t localDocid = INVALID_DOCID;
        AttributeSegmentModifierPtr segmentModifier0 =
            attrModifier.GetSegmentModifier(99, localDocid);
        ASSERT_EQ((docid_t)99, localDocid);

        AttributeSegmentModifierPtr segmentModifier1 = 
            attrModifier.GetSegmentModifier(100, localDocid);
        ASSERT_EQ((docid_t)0, localDocid);

        AttributeSegmentModifierPtr segmentModifier2 = 
            attrModifier.GetSegmentModifier(130, localDocid);
        ASSERT_EQ((docid_t)30, localDocid);
         
        ASSERT_TRUE(segmentModifier0 != segmentModifier1);
        ASSERT_TRUE(segmentModifier1 == segmentModifier2);
    }

    void TestCaseForGetSegmentBaseInfo()
    {
        PartitionDataMaker::MakePartitionDataFiles(0, 0,
                GET_PARTITION_DIRECTORY(), "1,200,0;3,100,3;4,60,4");
        
        IndexPartitionOptions options;
        PartitionDataPtr partitionData = 
            PartitionDataMaker::CreatePartitionData(
                    GET_FILE_SYSTEM(), options, mSchema);

        PatchAttributeModifier attrModifier(mSchema, NULL);
        attrModifier.Init(partitionData);

        docid_t globalId = 299;
        const PatchAttributeModifier::SegmentBaseDocIdInfo& info = 
            attrModifier.GetSegmentBaseInfo(globalId);
        INDEXLIB_TEST_EQUAL(3, info.segId);
        INDEXLIB_TEST_EQUAL(200, info.baseDocId);
    }

private:
    config::IndexPartitionSchemaPtr mSchema;
    autil::mem_pool::Pool mPool;
};

INDEXLIB_UNIT_TEST_CASE(PatchAttributeModifierTest, TestSimpleProcess);
INDEXLIB_UNIT_TEST_CASE(PatchAttributeModifierTest, TestGetSegmentModifier);
INDEXLIB_UNIT_TEST_CASE(PatchAttributeModifierTest, TestCaseForGetSegmentBaseInfo);

IE_NAMESPACE_END(index);
