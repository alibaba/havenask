#include <autil/StringTokenizer.h>
#include <autil/StringUtil.h>
#include "indexlib/common_define.h"
#include "indexlib/test/unittest.h"
#include "indexlib/index/normal/attribute/accessor/built_attribute_segment_modifier.h"
#include "indexlib/file_system/directory.h"
#include "indexlib/index/test/partition_schema_maker.h"

using namespace std;
using namespace std::tr1;
using namespace autil;
using namespace autil::mem_pool;

IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(file_system);

IE_NAMESPACE_BEGIN(index);

namespace {
class MockBuiltAttributeSegmentModifier : public BuiltAttributeSegmentModifier
{
public:
    MockBuiltAttributeSegmentModifier(const AttributeSchemaPtr& attrSchema)
        : BuiltAttributeSegmentModifier(0, attrSchema, NULL)
    {}

    MOCK_METHOD1(CreateUpdater, AttributeUpdaterPtr(uint32_t idx));
};

class MockAttributeUpdater : public AttributeUpdater
{
public:
    MockAttributeUpdater(const AttributeConfigPtr& attrConfig)
        : AttributeUpdater(NULL, 0, attrConfig)
    {}

    MOCK_METHOD2(Update, void(docid_t docId, const autil::ConstString& attrValue));
    MOCK_METHOD2(Dump, void(const file_system::DirectoryPtr& dir, 
                            segmentid_t srcSegment));
};

};


class BuiltAttributeSegmentModifierTest : public INDEXLIB_TESTBASE
{
public:
    void CaseSetUp() override
    {
        mSchema.reset(new IndexPartitionSchema);
        PartitionSchemaMaker::MakeSchema(mSchema,
                "price0:int32;price1:int32;price2:int32;"
                "price3:int32;price4:int32;nid:int64",
                "nid:primarykey64:nid;",
                "price0;price1;price2;price3;price4", "" );
    }

    void CaseTearDown() override
    {
    }

    void TestCaseGetUpdater()
    {
        AttributeSchemaPtr attrSchema = mSchema->GetAttributeSchema();
        BuiltAttributeSegmentModifier modifier(0, attrSchema, NULL);

        ASSERT_EQ((size_t)0, modifier.mUpdaters.size());
        modifier.GetUpdater(0);
        ASSERT_EQ((size_t)1, modifier.mUpdaters.size());
        ASSERT_TRUE(modifier.mUpdaters[0] != NULL);

        modifier.GetUpdater(4);
        ASSERT_EQ((size_t)5, modifier.mUpdaters.size());
        ASSERT_TRUE(modifier.mUpdaters[4] != NULL);

        ASSERT_TRUE(modifier.mUpdaters[3] == NULL);
        modifier.GetUpdater(3);
        ASSERT_EQ((size_t)5, modifier.mUpdaters.size());
        AttributeUpdaterPtr updater = modifier.mUpdaters[3];
        ASSERT_TRUE(updater != NULL);
        ASSERT_EQ(updater, modifier.GetUpdater(3));
    }

    void TestCaseForUpdate()
    {
        AttributeSchemaPtr attrSchema = mSchema->GetAttributeSchema();
        MockBuiltAttributeSegmentModifier mockModifier(attrSchema);
        ConstString value;
        
        MockAttributeUpdater* mockUpdater = 
            new MockAttributeUpdater(attrSchema->GetAttributeConfig(0));
        AttributeUpdaterPtr updater(mockUpdater);
        EXPECT_CALL(mockModifier, CreateUpdater(0))
            .WillOnce(Return(updater));
        EXPECT_CALL(*mockUpdater, Update(15, value))
            .Times(1);
        mockModifier.Update(15, 0, value);

        EXPECT_CALL(*mockUpdater, Dump(_,_))
            .Times(1);
        mockModifier.Dump(GET_PARTITION_DIRECTORY(), INVALID_SEGMENTID);
    }

private:
    IndexPartitionSchemaPtr mSchema;
};

INDEXLIB_UNIT_TEST_CASE(BuiltAttributeSegmentModifierTest, TestCaseForUpdate);
INDEXLIB_UNIT_TEST_CASE(BuiltAttributeSegmentModifierTest, TestCaseGetUpdater);

IE_NAMESPACE_END(index);
