#include "indexlib/index/normal/attribute/test/attribute_update_info_unittest.h"

using namespace std;

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, AttributeUpdateInfoTest);

AttributeUpdateInfoTest::AttributeUpdateInfoTest()
{
}

AttributeUpdateInfoTest::~AttributeUpdateInfoTest()
{
}

void AttributeUpdateInfoTest::CaseSetUp()
{
}

void AttributeUpdateInfoTest::CaseTearDown()
{
}

void AttributeUpdateInfoTest::TestJsonize()
{
    string jsonStr =
        "{                                        \
            \"attribute_update_info\" :        \
            [                                       \
            {                                       \
                \"update_segment_id\" : 1,          \
                \"update_doc_count\" : 1000         \
            },                                      \
            {                                       \
                \"update_segment_id\" : 3,          \
                \"update_doc_count\" : 2000         \
            }                                       \
            ]                                       \
         }";
    
    AttributeUpdateInfo attrUpdateInfo;
    FromJsonString(attrUpdateInfo, jsonStr);

    AttributeUpdateInfo expectUpdateInfo;
    ASSERT_TRUE(expectUpdateInfo.Add(SegmentUpdateInfo(1, 1000)));
    ASSERT_TRUE(expectUpdateInfo.Add(SegmentUpdateInfo(3, 2000)));
    ASSERT_FALSE(expectUpdateInfo.Add(SegmentUpdateInfo(3, 2000)));

    ASSERT_EQ(expectUpdateInfo, attrUpdateInfo);

    AttributeUpdateInfo::Iterator iter = attrUpdateInfo.CreateIterator();
    ASSERT_TRUE(iter.HasNext());
    SegmentUpdateInfo info = iter.Next();
    ASSERT_EQ((segmentid_t)1, info.updateSegId);
    ASSERT_EQ((uint32_t)1000, info.updateDocCount);

    ASSERT_TRUE(iter.HasNext());
    info = iter.Next();
    ASSERT_EQ((segmentid_t)3, info.updateSegId);
    ASSERT_EQ((uint32_t)2000, info.updateDocCount);
    
    ASSERT_FALSE(iter.HasNext());
}

IE_NAMESPACE_END(index);

