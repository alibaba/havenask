#include "indexlib/index/attribute/AttributeDataInfo.h"

#include <string>

#include "unittest/unittest.h"

namespace indexlibv2::index {

class AttributeDataInfoTest : public TESTBASE
{
    AttributeDataInfoTest() = default;
    ~AttributeDataInfoTest() = default;
    void setUp() override {}
    void tearDown() override {}
};

TEST_F(AttributeDataInfoTest, TestSimpleProcess)
{
    std::string jsonStr = "{        \
       \"uniq_item_count\" : 1000, \
       \"max_item_length\" : 200   \
    }";

    AttributeDataInfo dataInfo(/*count*/ 0, /*len*/ 0);
    dataInfo.FromString(jsonStr);
    ASSERT_EQ((uint32_t)1000, dataInfo.uniqItemCount);
    ASSERT_EQ((uint32_t)200, dataInfo.maxItemLen);

    dataInfo.uniqItemCount = 2000;
    dataInfo.maxItemLen = 400;
    std::string str = dataInfo.ToString();

    AttributeDataInfo otherDataInfo(/*count*/ 0, /*len*/ 0);
    otherDataInfo.FromString(str);
    ASSERT_EQ(dataInfo.uniqItemCount, otherDataInfo.uniqItemCount);
    ASSERT_EQ(dataInfo.maxItemLen, otherDataInfo.maxItemLen);
}

TEST_F(AttributeDataInfoTest, TestLoadAndStore)
{
    std::string path = GET_TEMP_DATA_PATH();
    auto dir = indexlib::file_system::IDirectory::GetPhysicalDirectory(path);

    std::string jsonStr = R"({
        "uniq_item_count" : 1000,
        "max_item_length" : 200,
        "slice_count" : 4
    })";
    AttributeDataInfo dataInfo;
    FromJsonString(dataInfo, jsonStr);
    ASSERT_EQ(1000, dataInfo.uniqItemCount);
    ASSERT_EQ(200, dataInfo.maxItemLen);
    ASSERT_EQ(4, dataInfo.sliceCount);

    ASSERT_TRUE(dataInfo.Store(dir).IsOK());
    AttributeDataInfo fromFile;
    bool isExist;
    ASSERT_TRUE(fromFile.Load(dir, isExist).IsOK());
    ASSERT_EQ(1000, fromFile.uniqItemCount);
    ASSERT_EQ(200, fromFile.maxItemLen);
    ASSERT_EQ(4, fromFile.sliceCount);

    AttributeDataInfo fromStr;
    FromJsonString(fromStr, ToJsonString(dataInfo));
    ASSERT_EQ(1000, fromStr.uniqItemCount);
    ASSERT_EQ(200, fromStr.maxItemLen);
    ASSERT_EQ(4, fromStr.sliceCount);
}
} // namespace indexlibv2::index
