#include "indexlib/index_base/test/offline_recover_strategy_unittest.h"

#include "autil/StringUtil.h"
#include "indexlib/config/index_partition_schema_maker.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/index/partition_info.h"
#include "indexlib/index_base/index_meta/segment_info.h"
#include "indexlib/partition/partition_data_creator.h"
#include "indexlib/test/partition_data_maker.h"

using namespace std;
using namespace autil;
using namespace indexlib::index;
using namespace indexlib::test;
using namespace indexlib::file_system;
using namespace indexlib::common;
using namespace indexlib::document;
using namespace indexlib::file_system;
using namespace indexlib::config;

namespace indexlib { namespace index_base {
IE_LOG_SETUP(index_base, OfflineRecoverStrategyTest);

OfflineRecoverStrategyTest::OfflineRecoverStrategyTest() {}

OfflineRecoverStrategyTest::~OfflineRecoverStrategyTest() {}

void OfflineRecoverStrategyTest::CaseSetUp()
{
    mRootDir = GET_TEMP_DATA_PATH();

    mSchema.reset(new IndexPartitionSchema);
    IndexPartitionSchemaMaker::MakeSchema(mSchema, "string0:string;", "string0:string:string0", "", "");

    mOptions = IndexPartitionOptions();
    mOptions.SetIsOnline(false);
}

void OfflineRecoverStrategyTest::CaseTearDown() {}

void OfflineRecoverStrategyTest::TestCaseForRecoverEmptyIndex()
{
    {
        // test empty partition data, empty dir
        Version expectVersion;
        InnerTestRecover("", "", true, expectVersion);
    }

    tearDown();
    setUp();

    {
        // test partition data empty, dir not empty
        Version expectVersion(-1, 0);
        expectVersion.AddSegment(0);
        InnerTestRecover("", "normal|0,1,2", true, expectVersion);
    }
}

void OfflineRecoverStrategyTest::TestCaseForRecoverNormalIndex()
{
    // test partition data not empty, lost segment not empty
    Version expectVersion(0, 0);
    expectVersion.AddSegment(0);
    expectVersion.AddSegment(1);
    expectVersion.AddSegment(2);
    InnerTestRecover("0,1,2;1,2,3", "normal|2,1,2", true, expectVersion);
}

void OfflineRecoverStrategyTest::TestCaseForRecoverAfterMerge()
{
    // recover merged segment
    {
        Version expectVersion(0, 0);
        expectVersion.AddSegment(0);
        expectVersion.AddSegment(1);
        InnerTestRecover("0,1,2;1,2,3", "merge|2,1,2#normal|3,3,3", true, expectVersion);
        ASSERT_FALSE(FslibWrapper::IsExist(mRootDir + "segment_2_level_0").GetOrThrow());
        ASSERT_FALSE(FslibWrapper::IsExist(mRootDir + "segment_3_level_0").GetOrThrow());
    }

    tearDown();
    setUp();

    {
        Version expectVersion(0, 0);
        expectVersion.AddSegment(0);
        expectVersion.AddSegment(1);
        expectVersion.AddSegment(2);
        InnerTestRecover("0,1,2;1,2,3", "normal|2,1,2#merge|3,3,3", true, expectVersion);
        ASSERT_FALSE(FslibWrapper::IsExist(mRootDir + "segment_3_level_0").GetOrThrow());
    }
}

void OfflineRecoverStrategyTest::TestCaseForRecoverBrokenSegment()
{
    // recover broken segment
    Version expectVersion(0, 0);
    expectVersion.AddSegment(0);
    expectVersion.AddSegment(1);
    expectVersion.AddSegment(2);
    InnerTestRecover("0,1,2;1,2,3", "normal|2,2,2#broken|3,1,2#normal|4,3,3", true, expectVersion);
    ASSERT_FALSE(FslibWrapper::IsExist(mRootDir + "segment_3_level_0").GetOrThrow());
    ASSERT_FALSE(FslibWrapper::IsExist(mRootDir + "segment_4_level_0").GetOrThrow());
}

void OfflineRecoverStrategyTest::TestCaseForRecoverWithInvalidPrefixSegment()
{
    // test partition data not empty, lost segment_2, invalid backup_segment_3
    Version expectVersion(0, 0);
    expectVersion.AddSegment(0);
    expectVersion.AddSegment(1);
    expectVersion.AddSegment(2);

    // create backup_segment_3
    PartitionDataMaker::MakeSegmentData(GET_PARTITION_DIRECTORY(), "3,0,0", false);
    indexlibv2::framework::Locator locator(0, 0);
    expectVersion.SetLocator(Locator(locator.Serialize()));

    auto partDir = GET_PARTITION_DIRECTORY();
    partDir->MakeDirectory("backup_segment_3_tmp");
    auto segDir = partDir->GetDirectory("segment_3_level_0", false);
    segDir->RemoveFile(SEGMENT_INFO_FILE_NAME);
    indexlib::file_system::RemoveOption removeOption = indexlib::file_system::RemoveOption::MayNonExist();
    segDir->RemoveDirectory(DELETION_MAP_DIR_NAME, removeOption /* may not exist */);
    partDir->RemoveDirectory("segment_3_level_0", removeOption /* may non exist */);

    InnerTestRecover("0,1,2;1,2,3", "normal|2,1,2", true, expectVersion);
}

void OfflineRecoverStrategyTest::TestCaseForRecoverWithInvalidTempDir()
{
    // test partition data not empty, lost segment_2, invalid backup_segment_3
    Version expectVersion(0, 0);
    expectVersion.AddSegment(0);
    expectVersion.AddSegment(1);
    expectVersion.AddSegment(2);

    // create backup_segment_3
    PartitionDataMaker::MakeSegmentData(GET_PARTITION_DIRECTORY(), "3,0,0", false);
    indexlibv2::framework::Locator locator(0, 0);
    expectVersion.SetLocator(Locator(locator.Serialize()));

    auto partDir = GET_PARTITION_DIRECTORY();
    partDir->MakeDirectory("backup_segment_3_tmp");
    auto segDir = partDir->GetDirectory("segment_3_level_0", false);
    segDir->RemoveFile(SEGMENT_INFO_FILE_NAME);
    indexlib::file_system::RemoveOption removeOption = indexlib::file_system::RemoveOption::MayNonExist();
    segDir->RemoveDirectory(DELETION_MAP_DIR_NAME, removeOption);
    partDir->RemoveDirectory("segment_3_level_0", removeOption /* may non exist */);

    InnerTestRecover("0,1,2;1,2,3", "normal|2,1,2", true, expectVersion);
    ASSERT_TRUE(GET_PARTITION_DIRECTORY()->IsExist("backup_segment_3_tmp"));
}

void OfflineRecoverStrategyTest::TestCaseForVersionLevelRecover()
{
    Version expectVersion(0, 0);
    expectVersion.AddSegment(0);
    expectVersion.AddSegment(1);
    indexlibv2::framework::Locator locator(0, 3);
    expectVersion.SetLocator(Locator(locator.Serialize()));

    mOptions.GetOfflineConfig().recoverType = RecoverStrategyType::RST_VERSION_LEVEL;
    InnerTestRecover("0,1,2;1,2,3", "", false, expectVersion);
    tearDown();
    setUp();
    mOptions.GetOfflineConfig().recoverType = RecoverStrategyType::RST_VERSION_LEVEL;
    InnerTestRecover("0,1,2;1,2,3", "normal|2,1,2", false, expectVersion);
    tearDown();
    setUp();
    mOptions.GetOfflineConfig().recoverType = RecoverStrategyType::RST_VERSION_LEVEL;
    InnerTestRecover("0,1,2;1,2,3", "normal|2,1,2#broken|3,2,3#merge|4,2,2", false, expectVersion);
}

// currentSegInfos = segId,docCount,timestamp;...
// lostSegInfos = [broken|normal|merge]|segId,docCount,timestamp#...
void OfflineRecoverStrategyTest::InnerTestRecover(const string& currentSegInfos, const string& lostSegInfos,
                                                  bool generateLocator, Version& expectVersion)
{
    if (!currentSegInfos.empty()) {
        PartitionDataMaker::MakePartitionDataFiles(0, 0, GET_PARTITION_DIRECTORY(), currentSegInfos);
    }

    vector<vector<string>> lostSegInfoVec;
    StringUtil::fromString(lostSegInfos, lostSegInfoVec, "|", "#");

    for (size_t i = 0; i < lostSegInfoVec.size(); i++) {
        assert(lostSegInfoVec[i].size() == 2);
        bool isMerge = (lostSegInfoVec[i][0] == "merge");
        bool isBroken = (lostSegInfoVec[i][0] == "broken");

        segmentid_t segId =
            PartitionDataMaker::MakeSegmentData(GET_PARTITION_DIRECTORY(), lostSegInfoVec[i][1], isMerge);
        if (isBroken) {
            DirectoryPtr directory = GET_PARTITION_DIRECTORY();
            stringstream ss;
            ss << "segment_" << segId << "_level_0"
               << "/segment_info";

            if (directory->IsExist(ss.str())) {
                directory->RemoveFile(ss.str());
            }
            break;
        } else if (!isMerge && generateLocator) {
            vector<int64_t> segInfos;
            autil::StringUtil::fromString(lostSegInfoVec[i][1], segInfos, ",");
            if (segInfos.size() > 2) {
                indexlibv2::framework::Locator newLocator(0, segInfos[2]);

                expectVersion.SetLocator(Locator(newLocator.Serialize()));
            }
        }
    }

    PartitionDataPtr partitionData = PartitionDataMaker::CreatePartitionData(GET_FILE_SYSTEM(), mOptions, mSchema);
    Version version = partitionData->GetVersion();
    ASSERT_EQ(expectVersion, version) << ToJsonString(expectVersion) << "||" << ToJsonString(version);
}
}} // namespace indexlib::index_base
