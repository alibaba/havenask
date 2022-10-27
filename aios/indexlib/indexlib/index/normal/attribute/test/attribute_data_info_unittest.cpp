#include "indexlib/index/normal/attribute/test/attribute_data_info_unittest.h"

using namespace std;

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, AttributeDataInfoTest);

AttributeDataInfoTest::AttributeDataInfoTest()
{
}

AttributeDataInfoTest::~AttributeDataInfoTest()
{
}

void AttributeDataInfoTest::CaseSetUp()
{
}

void AttributeDataInfoTest::CaseTearDown()
{
}

void AttributeDataInfoTest::TestSimpleProcess()
{
    string jsonStr = "{        \
       \"uniq_item_count\" : 1000, \
       \"max_item_length\" : 200   \
    }";

    AttributeDataInfo dataInfo;
    dataInfo.FromString(jsonStr);
    ASSERT_EQ((uint32_t)1000, dataInfo.uniqItemCount);
    ASSERT_EQ((uint32_t)200, dataInfo.maxItemLen);

    dataInfo.uniqItemCount = 2000;
    dataInfo.maxItemLen = 400;
    string str = dataInfo.ToString();

    AttributeDataInfo otherDataInfo;
    otherDataInfo.FromString(str);
    ASSERT_EQ(dataInfo.uniqItemCount, otherDataInfo.uniqItemCount);
    ASSERT_EQ(dataInfo.maxItemLen, otherDataInfo.maxItemLen);
}

IE_NAMESPACE_END(index);

