#include <autil/StringUtil.h>
#include "indexlib/index_base/test/offline_recover_strategy_unittest.h"
#include "indexlib/test/partition_data_maker.h"
#include "indexlib/partition/partition_data_creator.h"
#include "indexlib/index/partition_info.h"
#include "indexlib/index_base/index_meta/segment_info.h"
#include "indexlib/config/index_partition_schema_maker.h"

using namespace std;
using namespace autil;
IE_NAMESPACE_USE(index);
IE_NAMESPACE_USE(test);
IE_NAMESPACE_USE(file_system);
IE_NAMESPACE_USE(common);
IE_NAMESPACE_USE(document);
IE_NAMESPACE_USE(storage);
IE_NAMESPACE_USE(config);

IE_NAMESPACE_BEGIN(index_base);
IE_LOG_SETUP(index_base, OfflineRecoverStrategyTest);

OfflineRecoverStrategyTest::OfflineRecoverStrategyTest()
{
}

OfflineRecoverStrategyTest::~OfflineRecoverStrategyTest()
{
}

void OfflineRecoverStrategyTest::CaseSetUp()
{
    mRootDir = GET_TEST_DATA_PATH();

    mSchema.reset(new IndexPartitionSchema);
    IndexPartitionSchemaMaker::MakeSchema(mSchema,
            "string0:string;", 
            "string0:string:string0", "", "");

    mOptions = IndexPartitionOptions();
    mOptions.SetIsOnline(false);
}

void OfflineRecoverStrategyTest::CaseTearDown()
{
}

void OfflineRecoverStrategyTest::TestCaseForRecoverEmptyIndex()
{
    {
        //test empty partition data, empty dir
        Version expectVersion;
        InnerTestRecover("", "", true, expectVersion);
    }

    TearDown();
    SetUp();

    {
        //test partition data empty, dir not empty
        Version expectVersion(-1, 0);
        expectVersion.AddSegment(0);
        InnerTestRecover("", "normal|0,1,2", true, expectVersion);
    }
}

void OfflineRecoverStrategyTest::TestCaseForRecoverNormalIndex()
{
    //test partition data not empty, lost segment not empty
    Version expectVersion(0, 0);
    expectVersion.AddSegment(0);
    expectVersion.AddSegment(1);
    expectVersion.AddSegment(2);
    InnerTestRecover("0,1,2;1,2,3", "normal|2,1,2", true, expectVersion);
}

void OfflineRecoverStrategyTest::TestCaseForRecoverAfterMerge()
{
    //recover merged segment
    {
        Version expectVersion(0, 0);
        expectVersion.AddSegment(0);
        expectVersion.AddSegment(1);
        InnerTestRecover("0,1,2;1,2,3", "merge|2,1,2#normal|3,3,3", true, expectVersion);
        ASSERT_FALSE(FileSystemWrapper::IsExist(mRootDir + "segment_2_level_0"));
        ASSERT_FALSE(FileSystemWrapper::IsExist(mRootDir + "segment_3_level_0"));
    }

    TearDown();
    SetUp();

    {
        Version expectVersion(0, 0);
        expectVersion.AddSegment(0);
        expectVersion.AddSegment(1);
        expectVersion.AddSegment(2);
        InnerTestRecover("0,1,2;1,2,3", "normal|2,1,2#merge|3,3,3", true, expectVersion);
        ASSERT_FALSE(FileSystemWrapper::IsExist(mRootDir + "segment_3_level_0"));
    }
}

void OfflineRecoverStrategyTest::TestCaseForRecoverBrokenSegment()
{
    //recover broken segment
    Version expectVersion(0, 0);
    expectVersion.AddSegment(0);
    expectVersion.AddSegment(1);
    expectVersion.AddSegment(2);
    InnerTestRecover("0,1,2;1,2,3", "normal|2,2,2#broken|3,1,2#normal|4,3,3", true, expectVersion);
    ASSERT_FALSE(FileSystemWrapper::IsExist(mRootDir + "segment_3_level_0"));
    ASSERT_FALSE(FileSystemWrapper::IsExist(mRootDir + "segment_4_level_0"));
}

void OfflineRecoverStrategyTest::TestCaseForRecoverWithInvalidPrefixSegment()
{
    //test partition data not empty, lost segment_2, invalid backup_segment_3
    Version expectVersion(0, 0);
    expectVersion.AddSegment(0);
    expectVersion.AddSegment(1);
    expectVersion.AddSegment(2);

    // create backup_segment_3
    PartitionDataMaker::MakeSegmentData(
            GET_PARTITION_DIRECTORY(), "3,0,0", false);
    expectVersion.SetLocator(Locator("0"));
    string srcSegPath = mRootDir + "segment_3_level_0";
    string dstSegPath = mRootDir + "backup_segment_3_level_0";
    FileSystemWrapper::Rename(srcSegPath, dstSegPath);

    InnerTestRecover("0,1,2;1,2,3", "normal|2,1,2", true, expectVersion);
}

void OfflineRecoverStrategyTest::TestCaseForRecoverWithInvalidTempDir()
{
    //test partition data not empty, lost segment_2, invalid backup_segment_3
    Version expectVersion(0, 0);
    expectVersion.AddSegment(0);
    expectVersion.AddSegment(1);
    expectVersion.AddSegment(2);

    // create backup_segment_3
    PartitionDataMaker::MakeSegmentData(
            GET_PARTITION_DIRECTORY(), "3,0,0", false);
    expectVersion.SetLocator(Locator("0"));

    string srcSegPath = mRootDir + "segment_3_level_0";
    string dstSegPath = mRootDir + "backup_segment_3_tmp";
    FileSystemWrapper::Rename(srcSegPath, dstSegPath);

    InnerTestRecover("0,1,2;1,2,3", "normal|2,1,2", true, expectVersion);
    ASSERT_TRUE(FileSystemWrapper::IsExist(dstSegPath));
}

void OfflineRecoverStrategyTest::TestCaseForVersionLevelRecover()
{
    Version expectVersion(0, 0);
    expectVersion.AddSegment(0);
    expectVersion.AddSegment(1);
    expectVersion.SetLocator(document::Locator("3"));
    
    mOptions.GetOfflineConfig().recoverType = RecoverStrategyType::RST_VERSION_LEVEL;    
    InnerTestRecover("0,1,2;1,2,3", "", false, expectVersion);
    TearDown();
    SetUp();
    mOptions.GetOfflineConfig().recoverType = RecoverStrategyType::RST_VERSION_LEVEL;
    InnerTestRecover("0,1,2;1,2,3", "normal|2,1,2", false, expectVersion);
    TearDown();
    SetUp();
    mOptions.GetOfflineConfig().recoverType = RecoverStrategyType::RST_VERSION_LEVEL;
    InnerTestRecover("0,1,2;1,2,3", "normal|2,1,2#broken|3,2,3#merge|4,2,2", false, expectVersion);
}

// currentSegInfos = segId,docCount,timestamp;...
// lostSegInfos = [broken|normal|merge]|segId,docCount,timestamp#...
void OfflineRecoverStrategyTest::InnerTestRecover(
        const string& currentSegInfos, const string& lostSegInfos,
        bool generateLocator,
        Version& expectVersion)
{
    if (!currentSegInfos.empty())
    {
        PartitionDataMaker::MakePartitionDataFiles(
                0, 0, GET_PARTITION_DIRECTORY(), currentSegInfos);
    }

    vector<vector<string> > lostSegInfoVec;
    StringUtil::fromString(lostSegInfos, lostSegInfoVec, "|", "#");

    for (size_t i = 0; i < lostSegInfoVec.size(); i++)
    {
        assert(lostSegInfoVec[i].size() == 2);
        bool isMerge = (lostSegInfoVec[i][0] == "merge");
        bool isBroken = (lostSegInfoVec[i][0] == "broken");

        segmentid_t segId = PartitionDataMaker::MakeSegmentData(
                    GET_PARTITION_DIRECTORY(), lostSegInfoVec[i][1], isMerge);
        if (isBroken)
        {
            DirectoryPtr directory = GET_PARTITION_DIRECTORY();
            stringstream ss;
            ss << "segment_" << segId << "_level_0" << "/segment_info";

            if (directory->IsExist(ss.str()))
            {
                directory->RemoveFile(ss.str());
            }
            break;
        }
        else if (!isMerge && generateLocator)
        {
            vector<int64_t> segInfos;
            autil::StringUtil::fromString(lostSegInfoVec[i][1], segInfos, ",");
            if (segInfos.size() > 2)
            {
                expectVersion.SetLocator(Locator(autil::StringUtil::toString(segInfos[2])));
            }
        }

    }

    PartitionDataPtr partitionData = PartitionDataMaker::CreatePartitionData(
            GET_FILE_SYSTEM(), mOptions, mSchema);
    Version version = partitionData->GetVersion();
    ASSERT_EQ(expectVersion, version);
}

IE_NAMESPACE_END(index_base);

