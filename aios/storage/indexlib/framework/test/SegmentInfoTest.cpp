#include "indexlib/framework/SegmentInfo.h"

#include "fslib/util/FileUtil.h"
#include "indexlib/base/Constant.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/JsonUtil.h"
#include "unittest/unittest.h"

namespace indexlibv2::framework {

class SegmentInfoTest : public TESTBASE
{
public:
    void setUp() override {}
    void tearDown() override {}

private:
    void MakeSegInfoAndCheckResult(const std::string& jsonStr, bool expectResult)
    {
        auto indexRoot = GET_TEMP_DATA_PATH() + "/index";
        ASSERT_TRUE(fslib::util::FileUtil::mkDir(indexRoot, true));
        auto seginfoPath = fslib::util::FileUtil::joinPath(indexRoot, SEGMENT_INFO_FILE_NAME);
        ASSERT_TRUE(fslib::util::FileUtil::writeFile(seginfoPath, jsonStr));

        auto indexDir = indexlib::file_system::Directory::GetPhysicalDirectory(indexRoot);
        SegmentInfo segInfo;
        auto readerOption = indexlib::file_system::ReaderOption::PutIntoCache(indexlib::file_system::FSOT_MEM);
        ASSERT_EQ(expectResult, segInfo.Load(indexDir->GetIDirectory(), readerOption).IsOK());
    }
};

TEST_F(SegmentInfoTest, testSimple)
{
    std::string jsonStr = R"(
        {
            "IsMergedSegment": false,
            "ShardingColumnCount": 1,
            "Timestamp": 1694115001276661,
            "hostname": "11.229.175.35",
            "ShardingColumnId": 4294967295,
            "Locator": "0100000000000080f618dce2c90406000100000000000000f618dce2c904060000000000ff030000",
            "MaxTTL": 0,
            "Descriptions": {},
            "DocCount": 5198
       }
    )";
    MakeSegInfoAndCheckResult(jsonStr, /*expectResult*/ true);
}

TEST_F(SegmentInfoTest, testInvalidLocator)
{
    {
        std::string jsonStr = R"(
        {
            "IsMergedSegment": false,
            "ShardingColumnCount": 1,
            "Timestamp": 1694115001276661,
            "hostname": "11.229.175.35",
            "ShardingColumnId": 4294967295,
            "Locator": "invalid-",
            "MaxTTL": 0,
            "Descriptions": {},
            "DocCount": 5198
        })";
        MakeSegInfoAndCheckResult(jsonStr, /*expectResult*/ false);
    }

    // for v1 locator, invalid with negative offset (i.e., offset < -1)
    {
        std::string jsonStr = R"(
        {
            "IsMergedSegment": false,
            "ShardingColumnCount": 1,
            "Timestamp": 1694115001276661,
            "hostname": "11.229.175.35",
            "ShardingColumnId": 4294967295,
            "Locator": "0000000000000000c8d50ee201000000f6ffffffffffffff0a00000001000000000000000100000000000000f6ffffffffffffff0a00000000000000ffff0000",
            "MaxTTL": 0,
            "Descriptions": {},
            "DocCount": 5198
        })";
        MakeSegInfoAndCheckResult(jsonStr, /*expectResult*/ false);
    }
}

} // namespace indexlibv2::framework
