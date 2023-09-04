#include "indexlib/partition/test/online_offline_join_intetest.h"

#include "autil/EnvUtil.h"
#include "indexlib/index_base/index_meta/version_loader.h"
#include "indexlib/merger/partition_merger_creator.h"
#include "indexlib/test/schema_maker.h"
#include "indexlib/util/test/build_test_util.h"

using namespace std;
using namespace autil;

using namespace indexlib::config;
using namespace indexlib::test;
using namespace indexlib::util;
using namespace indexlib::index_base;

namespace indexlib {
namespace partition {
IE_LOG_SETUP(partition, OnlineOfflineJoinTest);

OnlineOfflineJoinTest::OnlineOfflineJoinTest() {}

OnlineOfflineJoinTest::~OnlineOfflineJoinTest() {}

void OnlineOfflineJoinTest::CaseSetUp()
{
    mRootDir = GET_TEMP_DATA_PATH();
    MakeSchema(false);
    mOptions = IndexPartitionOptions();
    util::BuildTestUtil::SetupIndexPartitionOptionsForBuildMode(/*buildMode=*/GetParam(), &mOptions);
    mOptions.SetEnablePackageFile(false);
    mOptions.GetBuildConfig(false).maxDocCount = 10;
    mOptions.GetOnlineConfig().SetEnableRedoSpeedup(true);
    mOptions.GetOnlineConfig().SetVersionTsAlignment(1);
}

void OnlineOfflineJoinTest::CaseTearDown() {}

void OnlineOfflineJoinTest::MakeSchema(bool needSub)
{
    string field = "pk:uint64:pk;string1:string;text1:text;"
                   "long1:uint32;multi_long:uint64:true;"
                   "updatable_multi_long:uint64:true:true;";
    string index = "pk:primarykey64:pk;index1:string:string1;pack1:pack:text1;";
    string attr = "long1;multi_long;updatable_multi_long;pk;";
    string summary = "string1;";
    mSchema = SchemaMaker::MakeSchema(field, index, attr, summary);
    if (needSub) {
        string subfield = "substr1:string;subpkstr:string;sub_long:uint32;";
        string subindex = "subindex1:string:substr1;sub_pk:primarykey64:subpkstr;";
        string subattr = "substr1;subpkstr;sub_long;";
        string subsummary = "";

        IndexPartitionSchemaPtr subSchema = SchemaMaker::MakeSchema(subfield, subindex, subattr, subsummary);
        mSchema->SetSubIndexPartitionSchema(subSchema);
    }
}

void OnlineOfflineJoinTest::TestOfflineEndIndexWithStopTs()
{
    IndexPartitionOptions options;
    options.GetOnlineConfig().buildConfig.maxDocCount = 10;
    options.GetOnlineConfig().SetVersionTsAlignment(1);
    options.GetOfflineConfig().buildConfig.maxDocCount = 10;
    {
        tearDown();
        setUp();
        PartitionStateMachine psm;
        string fullDocs = "cmd=add,pk=1,string1=hello,long1=0,ts=2;"
                          "##stopTs=5;";
        ASSERT_TRUE(psm.Init(mSchema, options, mRootDir));
        ASSERT_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, fullDocs, "", ""));
        Version version;
        VersionLoader::GetVersion(GET_CHECK_DIRECTORY(), version, INVALID_VERSION);
        EXPECT_EQ((int64_t)5, version.GetTimestamp());
        EXPECT_EQ((int64_t)1, version.GetVersionId());
        string incDocs1 = "cmd=add,pk=2,string1=hello,long1=1,ts=7;"
                          "cmd=add,pk=3,string1=hello,long1=2,ts=10;"
                          "##stopTs=25;";
        ASSERT_TRUE(psm.Transfer(BUILD_INC_NO_MERGE, incDocs1, "index1:hello", "long1=0;long1=1;long1=2"));
        VersionLoader::GetVersion(GET_CHECK_DIRECTORY(), version, INVALID_VERSION);
        EXPECT_EQ((int64_t)25, version.GetTimestamp());
        EXPECT_EQ((int64_t)3, version.GetVersionId());

        string incDocs2 = "##stopTs=35;";
        ASSERT_TRUE(psm.Transfer(BUILD_INC_NO_MERGE, incDocs2, "index1:hello", "long1=0;long1=1;long1=2"));
        VersionLoader::GetVersion(GET_CHECK_DIRECTORY(), version, INVALID_VERSION);
        EXPECT_EQ((int64_t)35, version.GetTimestamp());
        EXPECT_EQ((int64_t)5, version.GetVersionId());

        string incDocs3 = "##stopTs=45;";
        ASSERT_TRUE(psm.Transfer(BUILD_INC, incDocs3, "index1:hello", "long1=0;long1=1;long1=2"));
        VersionLoader::GetVersion(GET_CHECK_DIRECTORY(), version, INVALID_VERSION);
        EXPECT_EQ((int64_t)45, version.GetTimestamp());
        EXPECT_EQ((int64_t)7, version.GetVersionId());
    }
    {
        tearDown();
        setUp();
        PartitionStateMachine psm;
        string fullDocs = "cmd=add,pk=1,string1=hello,long1=0,ts=200;"
                          "##stopTs=5;";
        ASSERT_TRUE(psm.Init(mSchema, options, mRootDir));
        ASSERT_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, fullDocs, "", ""));
        Version version;
        VersionLoader::GetVersion(GET_CHECK_DIRECTORY(), version, INVALID_VERSION);
        EXPECT_EQ((int64_t)5, version.GetTimestamp());
        EXPECT_EQ((int64_t)1, version.GetVersionId());

        string incDocs1 = "cmd=add,pk=2,string1=hello,long1=1,ts=700;"
                          "cmd=add,pk=3,string1=hello,long1=2,ts=1000;"
                          "##stopTs=25;";
        ASSERT_TRUE(psm.Transfer(BUILD_INC_NO_MERGE, incDocs1, "index1:hello", "long1=0;long1=1;long1=2"));
        VersionLoader::GetVersion(GET_CHECK_DIRECTORY(), version, INVALID_VERSION);
        EXPECT_EQ((int64_t)25, version.GetTimestamp());
        EXPECT_EQ((int64_t)3, version.GetVersionId());

        string incDocs2 = "##stopTs=35;";
        ASSERT_TRUE(psm.Transfer(BUILD_INC_NO_MERGE, incDocs2, "index1:hello", "long1=0;long1=1;long1=2"));
        VersionLoader::GetVersion(GET_CHECK_DIRECTORY(), version, INVALID_VERSION);
        EXPECT_EQ((int64_t)35, version.GetTimestamp());
        EXPECT_EQ((int64_t)5, version.GetVersionId());

        string incDocs3 = "##stopTs=45;";
        ASSERT_TRUE(psm.Transfer(BUILD_INC, incDocs3, "index1:hello", "long1=0;long1=1;long1=2"));
        VersionLoader::GetVersion(GET_CHECK_DIRECTORY(), version, INVALID_VERSION);
        EXPECT_EQ((int64_t)45, version.GetTimestamp());
        EXPECT_EQ((int64_t)7, version.GetVersionId());
    }
    {
        tearDown();
        setUp();
        PartitionStateMachine psm;
        string fullDocs = "cmd=add,pk=1,string1=hello,long1=0,ts=200;"
                          "##stopTs=200;";
        ASSERT_TRUE(psm.Init(mSchema, options, mRootDir));
        ASSERT_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, fullDocs, "", ""));
        Version version;
        VersionLoader::GetVersion(GET_CHECK_DIRECTORY(), version, INVALID_VERSION);
        EXPECT_EQ((int64_t)200, version.GetTimestamp());
        EXPECT_EQ((int64_t)0, version.GetVersionId());

        string incDocs1 = "cmd=add,pk=2,string1=hello,long1=1,ts=700;"
                          "cmd=add,pk=3,string1=hello,long1=2,ts=1000;"
                          "##stopTs=1200;";
        ASSERT_TRUE(psm.Transfer(BUILD_INC_NO_MERGE, incDocs1, "index1:hello", "long1=0;long1=1;long1=2"));
        VersionLoader::GetVersion(GET_CHECK_DIRECTORY(), version, INVALID_VERSION);
        EXPECT_EQ((int64_t)1200, version.GetTimestamp());
        EXPECT_EQ((int64_t)2, version.GetVersionId());

        string incDocs2 = "cmd=add,pk=4,string1=hello,long1=3,ts=1250;"
                          "##stopTs=1250;";
        ASSERT_TRUE(psm.Transfer(BUILD_INC_NO_MERGE, incDocs2, "index1:hello", "long1=0;long1=1;long1=2;long1=3"));
        VersionLoader::GetVersion(GET_CHECK_DIRECTORY(), version, INVALID_VERSION);
        EXPECT_EQ((int64_t)1250, version.GetTimestamp());
        EXPECT_EQ((int64_t)3, version.GetVersionId());
        string incDocs3 = "cmd=add,pk=4,string1=hello,long1=3,ts=1250;"
                          "cmd=add,pk=5,string1=hello,long1=4,ts=1330;"
                          "##stopTs=1350;";
        ASSERT_TRUE(
            psm.Transfer(BUILD_INC_NO_MERGE, incDocs3, "index1:hello", "long1=0;long1=1;long1=2;long1=3;long1=4"));
        VersionLoader::GetVersion(GET_CHECK_DIRECTORY(), version, INVALID_VERSION);
        EXPECT_EQ((int64_t)1350, version.GetTimestamp());
        string incDocs4 = "##stopTs=1350;";
        ASSERT_TRUE(psm.Transfer(BUILD_INC, incDocs4, "index1:hello", "long1=0;long1=1;long1=2;long1=3;long1=4"));
        VersionLoader::GetVersion(GET_CHECK_DIRECTORY(), version, INVALID_VERSION);
        EXPECT_EQ((int64_t)1350, version.GetTimestamp());
    }
    {
        tearDown();
        setUp();
        PartitionStateMachine psm;
        string fullDocs = "##stopTs=200;";
        ASSERT_TRUE(psm.Init(mSchema, options, mRootDir));
        ASSERT_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, fullDocs, "", ""));
        Version version;
        VersionLoader::GetVersion(GET_CHECK_DIRECTORY(), version, INVALID_VERSION);
        EXPECT_EQ((int64_t)200, version.GetTimestamp());
        EXPECT_EQ((int64_t)1, version.GetVersionId());

        string incDocs1 = "##stopTs=1200;";
        ASSERT_TRUE(psm.Transfer(BUILD_INC, incDocs1, "index1:hello", ""));
        VersionLoader::GetVersion(GET_CHECK_DIRECTORY(), version, INVALID_VERSION);
        EXPECT_EQ((int64_t)1200, version.GetTimestamp());
        Version::SetDefaultVersionFormatNumForTest(Version::INVALID_VERSION_FORMAT_NUM);
        EnvUtil::unsetEnv("VERSION_FORMAT_NUM");
    }
}

void OnlineOfflineJoinTest::TestMultiPartitionOfflineEndIndexWithStopTs()
{
    auto CheckMultiPartMerge = [this](const string& part1Doc, const string& part2Doc, const string& part3Doc,
                                      int64_t expectTs, uint32_t expectFormatVersion) {
        MakeOnePartitionData(mSchema, mOptions, "part1", part1Doc);
        MakeOnePartitionData(mSchema, mOptions, "part2", part2Doc);
        MakeOnePartitionData(mSchema, mOptions, "part3", part3Doc);
        vector<string> mergeSrcs;
        mergeSrcs.push_back(GET_TEMP_DATA_PATH() + "part1");
        mergeSrcs.push_back(GET_TEMP_DATA_PATH() + "part2");
        mergeSrcs.push_back(GET_TEMP_DATA_PATH() + "part3");
        string mergedPartPath = GET_TEMP_DATA_PATH() + "mergePart";
        merger::PartitionMergerPtr multiPartMerger(
            merger::PartitionMergerCreator::TEST_CreateFullParallelPartitionMerger(mergeSrcs, mergedPartPath, mOptions,
                                                                                   NULL, ""));
        multiPartMerger->Merge(true);
        auto mergedPartDir = GET_CHECK_DIRECTORY()->GetDirectory("mergePart", false);
        Version version;
        VersionLoader::GetVersion(mergedPartDir, version, INVALID_VERSION);
        EXPECT_EQ((int64_t)expectTs, version.GetTimestamp());
        EXPECT_EQ((int64_t)expectFormatVersion, version.GetFormatVersion());
    };
    {
        tearDown();
        setUp();
        string part1Doc = "cmd=add,pk=0,string1=hello,long1=0,ts=9;"
                          "cmd=add,pk=1,string1=hello,long1=1,ts=10;##stopTs=15;";
        string part2Doc = "cmd=add,pk=2,string1=hello,long1=2,ts=25;"
                          "cmd=add,pk=3,string1=hello,long1=3,ts=25;##stopTs=15";
        string part3Doc = "cmd=add,pk=4,string1=hello,long1=4,ts=13;"
                          "cmd=add,pk=5,string1=hello,long1=5,ts=16;##stopTs=15;";
        CheckMultiPartMerge(part1Doc, part2Doc, part3Doc, 15, 2u);
        string mergedPartPath = GET_TEMP_DATA_PATH() + "mergePart";
        PartitionStateMachine psm;
        psm.Init(mSchema, mOptions, mergedPartPath);
        string incDocStr = "cmd=add,pk=6,string1=hello,long1=6,ts=20;##stopTs=19;";
        ASSERT_TRUE(psm.Transfer(BUILD_INC, incDocStr, "index1:hello",
                                 "long1=0;long1=1;long1=2;long1=3;long1=4;long1=5;long1=6"));
        auto mergedPartDir = GET_CHECK_DIRECTORY()->GetDirectory("mergePart", false);
        Version version;
        VersionLoader::GetVersion(mergedPartDir, version, INVALID_VERSION);
        EXPECT_EQ((int64_t)19, version.GetTimestamp());
        EXPECT_EQ(2u, version.GetFormatVersion());
    }
    {
        tearDown();
        setUp();
        string part1Doc = "";
        string part2Doc = "cmd=add,pk=2,string1=hello,long1=2,ts=25;"
                          "cmd=add,pk=3,string1=hello,long1=3,ts=25;##stopTs=15";
        string part3Doc = "cmd=add,pk=4,string1=hello,long1=4,ts=13;"
                          "cmd=add,pk=5,string1=hello,long1=5,ts=16;##stopTs=15;";
        CheckMultiPartMerge(part1Doc, part2Doc, part3Doc, 15, 2u);
        string mergedPartPath = GET_TEMP_DATA_PATH() + "mergePart";
        PartitionStateMachine psm;
        psm.Init(mSchema, mOptions, mergedPartPath);
        string incDocStr = "cmd=add,pk=6,string1=hello,long1=6,ts=20;##stopTs=19;";
        ASSERT_TRUE(psm.Transfer(BUILD_INC, incDocStr, "index1:hello", "long1=2;long1=3;long1=4;long1=5;long1=6"));
        auto mergedPartDir = GET_CHECK_DIRECTORY()->GetDirectory("mergePart", false);

        Version version;
        VersionLoader::GetVersion(mergedPartDir, version, INVALID_VERSION);
        EXPECT_EQ((int64_t)19, version.GetTimestamp());
        EXPECT_EQ(2u, version.GetFormatVersion());
    }
    {
        tearDown();
        setUp();
        string part1Doc = "##stopTs=15;";
        string part2Doc = "##stopTs=15;";
        string part3Doc = "##stopTs=15;";
        CheckMultiPartMerge(part1Doc, part2Doc, part3Doc, 15, 2u);
        string mergedPartPath = GET_TEMP_DATA_PATH() + "mergePart";
        PartitionStateMachine psm;
        psm.Init(mSchema, mOptions, mergedPartPath);
        string incDocStr = "cmd=add,pk=6,string1=hello,long1=6,ts=20;##stopTs=19;";
        ASSERT_TRUE(psm.Transfer(BUILD_INC, incDocStr, "index1:hello", "long1=6"));
        auto mergedPartDir = GET_CHECK_DIRECTORY()->GetDirectory("mergePart", false);
        Version version;
        VersionLoader::GetVersion(mergedPartDir, version, INVALID_VERSION);
        EXPECT_EQ((int64_t)19, version.GetTimestamp());
        EXPECT_EQ(2u, version.GetFormatVersion());
    }
}

void OnlineOfflineJoinTest::TestCompatibleWithOldVersion()
{
    {{Version::SetDefaultVersionFormatNumForTest(1);
    tearDown();
    setUp();
    string part1Doc = "cmd=add,pk=0,string1=hello,long1=0,ts=9;"
                      "cmd=add,pk=1,string1=hello,long1=1,ts=10;##stopTs=15;";
    string part2Doc = "cmd=add,pk=2,string1=hello,long1=2,ts=25;"
                      "cmd=add,pk=3,string1=hello,long1=3,ts=25;##stopTs=15";
    string part3Doc = "cmd=add,pk=4,string1=hello,long1=4,ts=13;"
                      "cmd=add,pk=5,string1=hello,long1=5,ts=16;##stopTs=15;";

    MakeOnePartitionData(mSchema, mOptions, "part1", part1Doc);
    MakeOnePartitionData(mSchema, mOptions, "part2", part2Doc);
    MakeOnePartitionData(mSchema, mOptions, "part3", part3Doc);
    Version::SetDefaultVersionFormatNumForTest(Version::INVALID_VERSION_FORMAT_NUM);
}
{
    string mergedPartPath = GET_TEMP_DATA_PATH() + "mergePart";
    vector<string> mergeSrcs;
    mergeSrcs.push_back(GET_TEMP_DATA_PATH() + "part1");
    mergeSrcs.push_back(GET_TEMP_DATA_PATH() + "part2");
    mergeSrcs.push_back(GET_TEMP_DATA_PATH() + "part3");
    merger::PartitionMergerPtr multiPartMerger(merger::PartitionMergerCreator::TEST_CreateFullParallelPartitionMerger(
        mergeSrcs, mergedPartPath, mOptions, NULL, ""));
    multiPartMerger->Merge(true);
    auto mergedPartDir = GET_CHECK_DIRECTORY()->GetDirectory("mergePart", false);
    ASSERT_NE(mergedPartDir, nullptr);
    Version version;
    VersionLoader::GetVersion(mergedPartDir, version, INVALID_VERSION);
    EXPECT_EQ((int64_t)15, version.GetTimestamp());
    EXPECT_EQ((int64_t)1u, version.GetFormatVersion());
}
} // namespace partition
{
    tearDown();
    setUp();
    {
        Version::SetDefaultVersionFormatNumForTest(1);
        tearDown();
        setUp();
        string part1Doc = "cmd=add,pk=0,string1=hello,long1=0,ts=9;"
                          "cmd=add,pk=1,string1=hello,long1=1,ts=10;##stopTs=15;";
        string part2Doc = "cmd=add,pk=2,string1=hello,long1=2,ts=25;"
                          "cmd=add,pk=3,string1=hello,long1=3,ts=25;##stopTs=15";

        MakeOnePartitionData(mSchema, mOptions, "part1", part1Doc);
        MakeOnePartitionData(mSchema, mOptions, "part2", part2Doc);
        Version::SetDefaultVersionFormatNumForTest(Version::INVALID_VERSION_FORMAT_NUM);
    }
    {
        string part3Doc = "cmd=add,pk=4,string1=hello,long1=4,ts=13;"
                          "cmd=add,pk=5,string1=hello,long1=5,ts=16;##stopTs=15;";
        MakeOnePartitionData(mSchema, mOptions, "part3", part3Doc);
        string mergedPartPath = GET_TEMP_DATA_PATH() + "mergePart";
        vector<string> mergeSrcs;
        mergeSrcs.push_back(GET_TEMP_DATA_PATH() + "part1");
        mergeSrcs.push_back(GET_TEMP_DATA_PATH() + "part2");
        mergeSrcs.push_back(GET_TEMP_DATA_PATH() + "part3");
        merger::PartitionMergerPtr multiPartMerger(
            merger::PartitionMergerCreator::TEST_CreateFullParallelPartitionMerger(mergeSrcs, mergedPartPath, mOptions,
                                                                                   NULL, ""));
        multiPartMerger->Merge(true);
        auto mergedPartDir = GET_CHECK_DIRECTORY()->GetDirectory("mergePart", false);
        ASSERT_NE(mergedPartDir, nullptr);
        Version version;
        VersionLoader::GetVersion(mergedPartDir, version, INVALID_VERSION);
        EXPECT_EQ((int64_t)15, version.GetTimestamp());
        EXPECT_EQ((int64_t)2u, version.GetFormatVersion());
    }
}
{
    tearDown();
    setUp();
    {
        Version::SetDefaultVersionFormatNumForTest(1);
        PartitionStateMachine psm;
        string fullDocs = "cmd=add,pk=1,string1=hello,long1=0,ts=2;";
        ASSERT_TRUE(psm.Init(mSchema, mOptions, mRootDir));
        ASSERT_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, fullDocs, "", ""));
        string incDoc = "cmd=add,pk=2,string1=hello,long1=1,ts=3;";
        ASSERT_TRUE(psm.Transfer(BUILD_INC_NO_MERGE, incDoc, "", ""));
        Version version;
        VersionLoader::GetVersion(GET_CHECK_DIRECTORY(), version, INVALID_VERSION);
        EXPECT_EQ(1u, version.GetFormatVersion());
        EXPECT_EQ(3, version.GetTimestamp());
        Version::SetDefaultVersionFormatNumForTest(Version::INVALID_VERSION_FORMAT_NUM);
    }
    {
        PartitionStateMachine psm;
        ASSERT_TRUE(psm.Init(mSchema, mOptions, mRootDir));
        string incDoc2 = "##stopTs=100;";
        ASSERT_TRUE(psm.Transfer(BUILD_INC_NO_MERGE, incDoc2, "index1:hello", "long1=0;long1=1"));
        Version version;
        VersionLoader::GetVersion(GET_CHECK_DIRECTORY(), version, INVALID_VERSION);
        EXPECT_EQ(2u, version.GetFormatVersion());
        EXPECT_EQ(100, version.GetTimestamp());
    }
}
} // namespace indexlib

void OnlineOfflineJoinTest::MakeOnePartitionData(const IndexPartitionSchemaPtr& schema,
                                                 const IndexPartitionOptions& options, const string& partRootDir,
                                                 const string& docStrs)
{
    GET_PARTITION_DIRECTORY()->MakeDirectory(partRootDir);
    string rootDir = GET_TEMP_DATA_PATH() + partRootDir;
    PartitionStateMachine psm;
    psm.Init(schema, options, rootDir);
    ASSERT_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, docStrs, "", ""));
}

void OnlineOfflineJoinTest::TestPatchLoaderIncConsistentWithRt()
{
    {
        Version::SetDefaultVersionFormatNumForTest(2);
        IndexPartitionOptions options = mOptions;
        options.GetOnlineConfig().SetEnableRedoSpeedup(false);
        options.GetOnlineConfig().isIncConsistentWithRealtime = true;
        PartitionStateMachine psm;
        string fullDocs = "cmd=add,pk=1,string1=hello,long1=1,ts=2;"
                          "cmd=add,pk=2,string1=hello,long1=2,ts=2;"
                          "cmd=add,pk=3,string1=hello,long1=3,ts=2;";
        ASSERT_TRUE(psm.Init(mSchema, options, mRootDir));
        ASSERT_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, fullDocs, "index1:hello", "long1=1;long1=2;long1=3"));

        string rtDoc1 = "cmd=add,pk=4,string1=hello,long1=4,ts=50,locator=0:35;"
                        "cmd=add,pk=5,string1=hello,long1=5,ts=60,locator=0:35;"
                        "cmd=update_field,pk=2,string1=hello,long1=20,ts=50,locator=0:60;";
        ASSERT_TRUE(psm.Transfer(BUILD_RT, rtDoc1, "index1:hello", "long1=1;long1=20;long1=3;long1=4;long1=5"));

        string incDoc1 = "cmd=update_field,pk=2,string1=hello,long1=20,ts=30,locator=0:35;"
                         "cmd=update_field,pk=1,string1=hello,long1=10,ts=40,locator=0:35;";
        // check patch to lastVersion will not be lost
        EXPECT_TRUE(psm.Transfer(BUILD_INC_NO_MERGE_FORCE_REOPEN, incDoc1, "index1:hello",
                                 "long1=10;long1=20;long1=3;long1=4;long1=5"));
        Version::SetDefaultVersionFormatNumForTest(Version::INVALID_VERSION_FORMAT_NUM);
    }
    {
        Version::SetDefaultVersionFormatNumForTest(2);
        IndexPartitionOptions options = mOptions;
        options.GetOnlineConfig().isIncConsistentWithRealtime = true;
        PartitionStateMachine psm;
        string fullDocs = "cmd=add,pk=1,string1=hello,long1=1,ts=2;"
                          "cmd=add,pk=2,string1=hello,long1=2,ts=2;"
                          "cmd=add,pk=3,string1=hello,long1=3,ts=2;";
        ASSERT_TRUE(psm.Init(mSchema, options, mRootDir));
        ASSERT_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, fullDocs, "index1:hello", "long1=1;long1=2;long1=3"));

        string rtDoc1 = "cmd=add,pk=4,string1=hello,long1=4,ts=50,locator=0:35;"
                        "cmd=add,pk=5,string1=hello,long1=5,ts=60,locator=0:35;"
                        "cmd=update_field,pk=2,string1=hello,long1=20,ts=50,locator=0:60;";
        ASSERT_TRUE(psm.Transfer(BUILD_RT, rtDoc1, "index1:hello", "long1=1;long1=20;long1=3;long1=4;long1=5"));

        string incDoc1 = "cmd=update_field,pk=2,string1=hello,long1=20,ts=30,locator=0:35;"
                         "cmd=update_field,pk=1,string1=hello,long1=10,ts=40,locator=0:35;";
        // check patch to lastVersion will not be lost
        EXPECT_TRUE(psm.Transfer(BUILD_INC_NO_MERGE_FORCE_REOPEN, incDoc1, "index1:hello",
                                 "long1=10;long1=20;long1=3;long1=4;long1=5"));
        Version::SetDefaultVersionFormatNumForTest(Version::INVALID_VERSION_FORMAT_NUM);
    }
    {
        tearDown();
        setUp();
        // verify format_version = 1 will lost patch in newVersion
        Version::SetDefaultVersionFormatNumForTest(1);
        IndexPartitionOptions options = mOptions;
        options.GetOnlineConfig().isIncConsistentWithRealtime = true;
        PartitionStateMachine psm;
        string fullDocs = "cmd=add,pk=1,string1=hello,long1=1,ts=2;"
                          "cmd=add,pk=2,string1=hello,long1=2,ts=2;"
                          "cmd=add,pk=3,string1=hello,long1=3,ts=2;";
        ASSERT_TRUE(psm.Init(mSchema, options, mRootDir));
        ASSERT_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, fullDocs, "index1:hello", "long1=1;long1=2;long1=3"));

        string rtDoc1 = "cmd=update_field,pk=2,string1=hello,long1=20,ts=30,locator=0:35;"
                        "cmd=add,pk=4,string1=hello,long1=4,ts=50,locator=0:35;"
                        "cmd=add,pk=5,string1=hello,long1=5,ts=60,locator=0:35;";
        ASSERT_TRUE(psm.Transfer(BUILD_RT, rtDoc1, "index1:hello", "long1=1;long1=20;long1=3;long1=4;long1=5"));

        string incDoc1 = "cmd=update_field,pk=2,string1=hello,long1=20,ts=30,locator=0:35;"
                         "cmd=update_field,pk=1,string1=hello,long1=10,ts=40,locator=0:35;"
                         "cmd=add,pk=4,string1=hello,long1=4,ts=50,locator=0:35;";
        // check update pk=1,long1=10  will be lost
        EXPECT_TRUE(
            psm.Transfer(BUILD_INC_NO_MERGE, incDoc1, "index1:hello", "long1=10;long1=20;long1=3;long1=4;long1=5"));

        string rtDoc2 = "cmd=update_field,pk=1,string1=hello,long1=10,ts=40,locator=0:35;";
        // check update pk=1,long1=10  will be lost
        EXPECT_TRUE(psm.Transfer(BUILD_RT, rtDoc2, "index1:hello", "long1=10;long1=20;long1=3;long1=4;long1=5"));

        string incDoc2 = "cmd=add,pk=5,string1=hello,long1=5,ts=60,locator=0:35;";
        // check update pk=1,long1=10  will be lost
        EXPECT_TRUE(
            psm.Transfer(BUILD_INC_NO_MERGE, incDoc2, "index1:hello", "long1=10;long1=20;long1=3;long1=4;long1=5"));
        Version::SetDefaultVersionFormatNumForTest(Version::INVALID_VERSION_FORMAT_NUM);
    }
}

void OnlineOfflineJoinTest::TestRedoStrategyForIncConsistentWithRt()
{
    { // test op's target doc in lastVersion's replay zone
        tearDown();
        setUp();
        IndexPartitionOptions options = mOptions;
        options.GetOnlineConfig().onDiskFlushRealtimeIndex = false;
        options.GetOnlineConfig().isIncConsistentWithRealtime = true;
        string fullDocString = "cmd=add,pk=1,string1=hello,long1=1,ts=0;"
                               "cmd=add,pk=2,string1=hello,long1=2,ts=0;"
                               "cmd=add,pk=3,string1=hello,long1=3,ts=0;"
                               "cmd=add,pk=4,string1=hello,long1=4,ts=110;"
                               "cmd=add,pk=5,string1=hello,long1=5,ts=120;"
                               "##stopTs=100;";

        string rtDoc = "cmd=add,pk=4,string1=hello,long1=4,ts=110;"
                       "cmd=add,pk=5,string1=hello,long1=5,ts=120;"
                       "cmd=add,pk=6,string1=hello,long1=6,ts=160;"
                       "cmd=add,pk=7,string1=hello,long1=7,ts=170;"
                       "cmd=add,pk=8,string1=hello,long1=8,ts=180;"
                       "cmd=add,pk=9,string1=hello,long1=9,ts=190;"
                       "cmd=update_field,pk=4,long1=40,ts=200;"
                       "cmd=update_field,pk=5,long1=50,ts=200;";

        string incDoc1 = "cmd=add,pk=4,string1=hello,long1=4,ts=120;"
                         "cmd=add,pk=5,string1=hello,long1=5,ts=150;"
                         "cmd=add,pk=6,string1=hello,long1=6,ts=160;"
                         "cmd=add,pk=7,string1=hello,long1=7,ts=170;"
                         "cmd=add,pk=8,string1=hello,long1=8,ts=180;"
                         "cmd=add,pk=9,string1=hello,long1=9,ts=190;"
                         "##stopTs=170;";

        PartitionStateMachine psm;
        INDEXLIB_TEST_TRUE(psm.Init(mSchema, options, mRootDir));
        INDEXLIB_TEST_TRUE(
            psm.Transfer(BUILD_FULL, fullDocString, "index1:hello", "long1=1;long1=2;long1=3;long1=4;long1=5;"));
        INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_RT, rtDoc, "index1:hello",
                                        "long1=1;long1=2;long1=3;long1=40;long1=50;long1=6;long1=7;long1=8;long1=9;"));
        INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_INC_NO_MERGE, incDoc1, "index1:hello",
                                        "long1=1;long1=2;long1=3;long1=40;long1=50;long1=6;long1=7;long1=8;long1=9;"));
    }
    { // test op's target doc in lastVersion's replay zone. if rt not consistent with inc, redo log will be lost
        tearDown();
        setUp();
        IndexPartitionOptions options = mOptions;
        options.GetOnlineConfig().onDiskFlushRealtimeIndex = false;
        options.GetOnlineConfig().isIncConsistentWithRealtime = true;
        string fullDocString = "cmd=add,pk=1,string1=hello,long1=1,ts=0;"
                               "cmd=add,pk=2,string1=hello,long1=2,ts=0;"
                               "cmd=add,pk=3,string1=hello,long1=3,ts=0;"
                               "cmd=add,pk=4,string1=hello,long1=4,ts=110;"
                               "cmd=add,pk=5,string1=hello,long1=5,ts=120;"
                               "##stopTs=100;";

        string rtDoc = "cmd=add,pk=6,string1=hello,long1=6,ts=160;"
                       "cmd=add,pk=7,string1=hello,long1=7,ts=170;"
                       "cmd=add,pk=8,string1=hello,long1=8,ts=180;"
                       "cmd=add,pk=9,string1=hello,long1=9,ts=190;"
                       "cmd=update_field,pk=4,long1=40,ts=200;"
                       "cmd=update_field,pk=5,long1=50,ts=200;";

        string incDoc1 = "cmd=add,pk=4,string1=hello,long1=4,ts=120;"
                         "cmd=add,pk=5,string1=hello,long1=5,ts=150;"
                         "cmd=add,pk=6,string1=hello,long1=6,ts=160;"
                         "cmd=add,pk=7,string1=hello,long1=7,ts=170;"
                         "cmd=add,pk=8,string1=hello,long1=8,ts=180;"
                         "cmd=add,pk=9,string1=hello,long1=9,ts=190;"
                         "##stopTs=170;";

        PartitionStateMachine psm;
        INDEXLIB_TEST_TRUE(psm.Init(mSchema, options, mRootDir));
        INDEXLIB_TEST_TRUE(
            psm.Transfer(BUILD_FULL, fullDocString, "index1:hello", "long1=1;long1=2;long1=3;long1=4;long1=5;"));
        INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_RT, rtDoc, "index1:hello",
                                        "long1=1;long1=2;long1=3;long1=40;long1=50;long1=6;long1=7;long1=8;long1=9;"));
        INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_INC_NO_MERGE, incDoc1, "index1:hello",
                                        "long1=1;long1=2;long1=3;long1=4;long1=5;long1=6;long1=7;long1=8;long1=9;"));
    }
    {
        // test op.ts in newVersion's replay zone
        tearDown();
        setUp();
        IndexPartitionOptions options = mOptions;
        options.GetOnlineConfig().onDiskFlushRealtimeIndex = false;
        options.GetOnlineConfig().isIncConsistentWithRealtime = true;
        string fullDocString = "cmd=add,pk=1,string1=hello,long1=1,ts=0;"
                               "cmd=add,pk=2,string1=hello,long1=2,ts=0;"
                               "cmd=add,pk=3,string1=hello,long1=3,ts=0;"
                               "cmd=add,pk=4,string1=hello,long1=4,ts=110;"
                               "cmd=add,pk=5,string1=hello,long1=5,ts=120;"
                               "##stopTs=100;";

        string rtDoc = "cmd=add,pk=4,string1=hello,long1=4,ts=110;"
                       "cmd=add,pk=5,string1=hello,long1=5,ts=120;"
                       "cmd=add,pk=6,string1=hello,long1=6,ts=160;"
                       "cmd=add,pk=7,string1=hello,long1=7,ts=170;"
                       "cmd=add,pk=8,string1=hello,long1=8,ts=180;"
                       "cmd=add,pk=9,string1=hello,long1=9,ts=190;"
                       "cmd=update_field,pk=2,long1=22,ts=220;"
                       "cmd=update_field,pk=2,long1=23,ts=230,locator=0:180;";

        string incDoc1 = "cmd=add,pk=4,string1=hello,long1=4,ts=120;"
                         "cmd=add,pk=5,string1=hello,long1=5,ts=150;"
                         "cmd=add,pk=6,string1=hello,long1=6,ts=160;"
                         "cmd=add,pk=7,string1=hello,long1=7,ts=170;"
                         "cmd=add,pk=8,string1=hello,long1=8,ts=180;"
                         "cmd=add,pk=9,string1=hello,long1=9,ts=190;"
                         "cmd=add,pk=9,string1=hello,long1=9,ts=240;"
                         "cmd=update_field,pk=2,long1=22,ts=220;"
                         "##stopTs=200;";

        PartitionStateMachine psm;
        INDEXLIB_TEST_TRUE(psm.Init(mSchema, options, mRootDir));
        INDEXLIB_TEST_TRUE(
            psm.Transfer(BUILD_FULL, fullDocString, "index1:hello", "long1=1;long1=2;long1=3;long1=4;long1=5;"));
        INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_RT, rtDoc, "index1:hello",
                                        "long1=1;long1=23;long1=3;long1=4;long1=5;long1=6;long1=7;long1=8;long1=9;"));
        INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_INC_NO_MERGE, incDoc1, "index1:hello",
                                        "long1=1;long1=23;long1=3;long1=4;long1=5;long1=6;long1=7;long1=8;long1=9;"));
    }
    { // test delOp's segId belongs to lastVersion
        tearDown();
        setUp();
        IndexPartitionOptions options = mOptions;
        options.GetBuildConfig(false).enablePackageFile = false;
        options.GetBuildConfig(true).enablePackageFile = false;
        options.GetOnlineConfig().onDiskFlushRealtimeIndex = false;
        options.GetOnlineConfig().isIncConsistentWithRealtime = true;
        string fullDocString = "cmd=add,pk=1,string1=hello,long1=1,ts=0;"
                               "cmd=add,pk=2,string1=hello,long1=2,ts=0;"
                               "cmd=add,pk=3,string1=hello,long1=3,ts=0;"
                               "cmd=add,pk=4,string1=hello,long1=4,ts=110;"
                               "cmd=add,pk=5,string1=hello,long1=5,ts=120;"
                               "cmd=delete,pk=1,ts=120;"
                               "##stopTs=130;";

        string rtDoc = "cmd=add,pk=6,string1=hello,long1=6,ts=160;"
                       "cmd=add,pk=7,string1=hello,long1=7,ts=170;"
                       "cmd=add,pk=8,string1=hello,long1=8,ts=180;"
                       "cmd=add,pk=9,string1=hello,long1=9,ts=190;"
                       "cmd=delete,pk=2,ts=200;"
                       "cmd=delete,pk=3,ts=201;";

        string incDoc1 = "cmd=add,pk=6,string1=hello,long1=6,ts=160;"
                         "cmd=add,pk=7,string1=hello,long1=7,ts=170;"
                         "cmd=add,pk=8,string1=hello,long1=8,ts=180;"
                         "cmd=add,pk=9,string1=hello,long1=9,ts=190;"
                         "cmd=delete,pk=2,ts=200;"
                         "##stopTs=190;";

        PartitionStateMachine psm;
        INDEXLIB_TEST_TRUE(psm.Init(mSchema, options, mRootDir));
        INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_FULL, fullDocString, "index1:hello", "long1=2;long1=3;long1=4;long1=5;"));
        INDEXLIB_TEST_TRUE(
            psm.Transfer(BUILD_RT, rtDoc, "index1:hello", "long1=4;long1=5;long1=6;long1=7;long1=8;long1=9;"));
        INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_INC_NO_MERGE, incDoc1, "index1:hello",
                                        "long1=4;long1=5;long1=6;long1=7;long1=8;long1=9;"));
    }
    { // test op's target segment in lastVersion, not in newVersion
        tearDown();
        setUp();
        IndexPartitionOptions options = mOptions;
        options.GetOnlineConfig().onDiskFlushRealtimeIndex = false;
        options.GetOnlineConfig().isIncConsistentWithRealtime = true;
        options.GetBuildConfig(false).maxDocCount = 2;
        // verison.0: s0, s1, s2, s3
        string fullDocString = "cmd=add,pk=1,string1=hello,long1=1,ts=0;"
                               "cmd=add,pk=2,string1=hello,long1=2,ts=0;"
                               "cmd=add,pk=3,string1=hello,long1=3,ts=0;"
                               "cmd=add,pk=4,string1=hello,long1=4,ts=110;"
                               "cmd=add,pk=5,string1=hello,long1=5,ts=120;"
                               "cmd=add,pk=6,string1=hello,long1=6,ts=120;"
                               "cmd=add,pk=7,string1=hello,long1=7,ts=120;"
                               "cmd=add,pk=8,string1=hello,long1=8,ts=120;"
                               "##stopTs=100;";

        string rtDoc = "cmd=add,pk=4,string1=hello,long1=4,ts=110;"
                       "cmd=add,pk=5,string1=hello,long1=5,ts=120;"
                       "cmd=add,pk=6,string1=hello,long1=6,ts=160;"
                       "cmd=add,pk=7,string1=hello,long1=7,ts=170;"
                       "cmd=add,pk=8,string1=hello,long1=8,ts=180;"
                       "cmd=add,pk=9,string1=hello,long1=9,ts=190;"
                       "cmd=update_field,pk=2,long1=20,ts=200;"
                       "cmd=update_field,pk=4,long1=40,ts=200;"
                       "cmd=update_field,pk=5,long1=50,ts=200;"
                       "cmd=update_field,pk=7,long1=70,ts=200;";

        // build version.1 : s0, s1, s2, s3, s4, s5
        // merge s2,s3,s4,s5->s6
        // merge version.2 : s0, s1, s6
        string incDoc1 = "cmd=add,pk=4,string1=hello,long1=4,ts=110;"
                         "cmd=add,pk=5,string1=hello,long1=5,ts=120;"
                         "cmd=add,pk=6,string1=hello,long1=6,ts=160;"
                         "##stopTs=110;";

        options.GetMergeConfig().mergeStrategyStr = "specific_segments";
        options.GetMergeConfig().mergeStrategyParameter.SetLegacyString("merge_segments=2,3,4,5");

        PartitionStateMachine psm;
        INDEXLIB_TEST_TRUE(psm.Init(mSchema, options, mRootDir));
        INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, fullDocString, "index1:hello",
                                        "long1=1;long1=2;long1=3;long1=4;long1=5;long1=6;long1=7;long1=8;"));
        INDEXLIB_TEST_TRUE(
            psm.Transfer(BUILD_RT, rtDoc, "index1:hello",
                         "long1=1;long1=20;long1=3;long1=40;long1=50;long1=6;long1=70;long1=8;long1=9;"));
        INDEXLIB_TEST_TRUE(
            psm.Transfer(BUILD_INC, incDoc1, "index1:hello",
                         "long1=1;long1=20;long1=3;long1=40;long1=50;long1=6;long1=70;long1=8;long1=9;"));

        string incDoc2 = "cmd=add,pk=4,string1=hello,long1=4,ts=110;"
                         "cmd=add,pk=5,string1=hello,long1=5,ts=120;"
                         "cmd=add,pk=6,string1=hello,long1=6,ts=160;"
                         "cmd=add,pk=7,string1=hello,long1=7,ts=170;"
                         "cmd=add,pk=8,string1=hello,long1=8,ts=180;"
                         "cmd=add,pk=9,string1=hello,long1=9,ts=190;"
                         "cmd=update_field,pk=2,long1=20,ts=200;"
                         "##stopTs=199;";

        INDEXLIB_TEST_TRUE(
            psm.Transfer(BUILD_INC_NO_MERGE, incDoc2, "index1:hello",
                         "long1=1;long1=20;long1=3;long1=40;long1=50;long1=6;long1=70;long1=8;long1=9;"));
    }
}

void OnlineOfflineJoinTest::TestRedoStrategyForIncConsistentWithRtWhenForceReopen()
{
    string field = "pk:string;long1:int32;long2:int32;multi_int:int32:true:true;string1:string;";
    string index = "pk:primarykey64:pk;index1:string:string1";
    string attr = "long1;packAttr1:long2,multi_int";
    IndexPartitionOptions options = mOptions;
    options.GetOnlineConfig().onDiskFlushRealtimeIndex = false;
    options.GetOnlineConfig().isIncConsistentWithRealtime = true;
    string fullDocString = "cmd=add,pk=1,string1=hello,long1=1,updatable_multi_long=1 2,ts=0;"
                           "cmd=add,pk=2,string1=hello,long1=2,updatable_multi_long=2 3,ts=0;"
                           "cmd=add,pk=3,string1=hello,long1=3,updatable_multi_long=3 4,ts=0;"
                           "cmd=add,pk=4,string1=hello,long1=4,updatable_multi_long=4 5,ts=110;"
                           "cmd=add,pk=5,string1=hello,long1=5,updatable_multi_long=5 6,ts=120;"
                           "##stopTs=100;";

    string rtDoc = "cmd=add,pk=8,string1=hello,long1=8,updatable_multi_long=8 9,ts=140;"
                   "cmd=add,pk=9,string1=hello,long1=9,updatable_multi_long=9 10,ts=150;"
                   "cmd=update_field,pk=2,long1=20,updatable_multi_long=20 3,ts=151;"
                   "cmd=update_field,pk=3,long1=30,updatable_multi_long=30 4,ts=160;"
                   "cmd=update_field,pk=4,long1=40,updatable_multi_long=40 5,ts=170;"
                   "cmd=update_field,pk=5,long1=50,updatable_multi_long=50 6,ts=180;";

    string incDoc1 = "cmd=add,pk=8,string1=hello,long1=8,updatable_multi_long=8 9,ts=140;"
                     "cmd=add,pk=9,string1=hello,long1=9,updatable_multi_long=9 10,ts=150;"
                     "##stopTs=150;";

    InnerTestReOpen(
        mSchema, options, fullDocString,
        {{"RT", rtDoc, "index1:hello", "long1=1;long1=20;long1=30;long1=40;long1=50;long1=8;long1=9;"},
         {"QUERY", "", "index1:hello",
          "updatable_multi_long=1 2;updatable_multi_long=20 3;updatable_multi_long=30 4;updatable_multi_long=40 "
          "5;updatable_multi_long=50 6;updatable_multi_long=8 9;updatable_multi_long=9 10;"},
         {"INC_NO_MERGE_FORCE_REOPEN", incDoc1, "index1:hello",
          "long1=1;long1=20;long1=30;long1=40;long1=50;long1=8;long1=9;"},
         {"QUERY", "", "index1:hello",
          "updatable_multi_long=1 2;updatable_multi_long=20 3;updatable_multi_long=30 4;updatable_multi_long=40 "
          "5;updatable_multi_long=50 6;updatable_multi_long=8 9;updatable_multi_long=9 10;"}});
}

void OnlineOfflineJoinTest::TestDelOpRedoOptimize()
{
    IndexPartitionOptions options = mOptions;
    options.GetOnlineConfig().onDiskFlushRealtimeIndex = false;
    options.GetOnlineConfig().isIncConsistentWithRealtime = true;
    options.GetBuildConfig(false).enablePackageFile = false;
    options.GetBuildConfig(true).enablePackageFile = false;

    string fullDocString = "cmd=add,pk=1,string1=hello,long1=1,ts=0;"
                           "cmd=delete,pk=1,ts=0;"
                           "cmd=add,pk=2,string1=hello,long1=2,ts=0;"
                           "cmd=add,pk=3,string1=hello,long1=3,ts=0;"
                           "cmd=add,pk=4,string1=hello,long1=4,ts=110;"
                           "cmd=add,pk=5,string1=hello,long1=5,ts=120;"
                           "cmd=add,pk=6,string1=hello,long1=6,ts=120;"
                           "##stopTs=100;";

    string rtDoc1 = "cmd=add,pk=7,string1=hello,long1=7,ts=110;"
                    "cmd=add,pk=8,string1=hello,long1=8,ts=120;"
                    "cmd=delete,pk=2,ts=130;"
                    "cmd=delete,pk=3,ts=140;";

    string incDoc1 = "cmd=add,pk=7,string1=hello,long1=7,ts=110;"
                     "cmd=add,pk=8,string1=hello,long1=8,ts=120;"
                     "cmd=delete,pk=2,ts=130;##stopTs=130;";

    // rt ahead of inc
    InnerTestReOpen(mSchema, options, fullDocString,
                    {{"RT", rtDoc1, "index1:hello", "long1=4;long1=5;long1=6;long1=7;long1=8;"},
                     {"INC_NO_MERGE", incDoc1, "index1:hello", "long1=4;long1=5;long1=6;long1=7;long1=8;"}});

    // inc partial cover rt
    string incDoc2 = "cmd=add,pk=7,string1=hello,long1=7,ts=110;"
                     "cmd=add,pk=8,string1=hello,long1=8,ts=120;"
                     "cmd=delete,pk=2,ts=130;"
                     "cmd=delete,pk=3,ts=130;"
                     "cmd=add,pk=2,string1=hello,long1=20,ts=140;"
                     "##stopTs=130;";
    InnerTestReOpen(mSchema, options, fullDocString,
                    {{"RT", rtDoc1, "index1:hello", "long1=4;long1=5;long1=6;long1=7;long1=8;"},
                     {"INC_NO_MERGE", incDoc2, "index1:hello", "long1=4;long1=5;long1=6;long1=7;long1=8;"}});

    // inc completely cover rt
    string incDoc3 = "cmd=add,pk=7,string1=hello,long1=7,ts=110;"
                     "cmd=add,pk=8,string1=hello,long1=8,ts=120;"
                     "cmd=delete,pk=2,ts=130;"
                     "cmd=delete,pk=3,ts=140;"
                     "cmd=add,pk=2,string1=hello,long1=20,ts=140;"
                     "##stopTs=141;";
    InnerTestReOpen(mSchema, options, fullDocString,
                    {{"RT", rtDoc1, "index1:hello", "long1=4;long1=5;long1=6;long1=7;long1=8;"},
                     {"INC_NO_MERGE", incDoc3, "index1:hello", "long1=4;long1=5;long1=6;long1=7;long1=8;long1=20;"}});

    // test delOp only in rt
    string incDoc4 = "cmd=add,pk=7,string1=hello,long1=7,ts=110;"
                     "##stopTs=110;";
    InnerTestReOpen(mSchema, options, fullDocString,
                    {{"RT", rtDoc1, "index1:hello", "long1=4;long1=5;long1=6;long1=7;long1=8;"},
                     {"INC_NO_MERGE", incDoc4, "index1:hello", "long1=4;long1=5;long1=6;long1=7;long1=8;"}});

    // test index state rollback
    string incDoc5 = "cmd=add,pk=7,string1=hello,long1=7,ts=110;"
                     "cmd=add,pk=8,string1=hello,long1=8,ts=120;"
                     "cmd=delete,pk=2,ts=130;"
                     "cmd=delete,pk=3,ts=140;"
                     "cmd=delete,pk=4,ts=142;"
                     "cmd=add,pk=4,string1=hello,long1=40,ts=150;"
                     "##stopTs=130;";
    InnerTestReOpen(mSchema, options, fullDocString,
                    {{"RT", rtDoc1, "index1:hello", "long1=4;long1=5;long1=6;long1=7;long1=8;"},
                     {"INC_NO_MERGE", incDoc5, "index1:hello", "long1=5;long1=6;long1=7;long1=8;long1=40;"},
                     {"RT", "cmd=delete,pk=4,ts=142;", "index1:hello", "long1=5;long1=6;long1=7;long1=8;"},
                     {"RT", "cmd=add,pk=4,string1=hello,long1=40,ts=150;", "index1:hello",
                      "long1=5;long1=6;long1=7;long1=8;long1=40;"}});

    // test reopen multiple times
    string rtDoc2 = "cmd=add,pk=7,string1=hello,long1=7,ts=110;"
                    "cmd=add,pk=8,string1=hello,long1=8,ts=120;"
                    "cmd=delete,pk=2,ts=130;"
                    "cmd=delete,pk=3,ts=140;"
                    "cmd=add,pk=9,string1=hello,long1=9,ts=150;"
                    "cmd=add,pk=10,string1=hello,long1=10,ts=160;"
                    "cmd=delete,pk=7,ts=170;";

    string incDoc6 = "cmd=add,pk=7,string1=hello,long1=7,ts=110;"
                     "cmd=add,pk=8,string1=hello,long1=8,ts=120;"
                     "##stopTs=120;";

    string incDoc7 = "cmd=add,pk=8,string1=hello,long1=8,ts=120;"
                     "cmd=delete,pk=2,ts=130;"
                     "cmd=delete,pk=3,ts=140;"
                     "cmd=add,pk=9,string1=hello,long1=9,ts=150;"
                     "##stopTs=150;";
    InnerTestReOpen(mSchema, options, fullDocString,
                    {{"RT", rtDoc2, "index1:hello", "long1=4;long1=5;long1=6;long1=8;long1=9;long1=10"},
                     {"INC_NO_MERGE", incDoc6, "index1:hello", "long1=4;long1=5;long1=6;long1=8;long1=9;long1=10"},
                     {"INC_NO_MERGE", incDoc7, "index1:hello", "long1=4;long1=5;long1=6;long1=8;long1=9;long1=10"}});
}

void OnlineOfflineJoinTest::TestDelOpRedoOptimizeWithIncNotConsistentWithRt()
{
    {
        tearDown();
        setUp();
        IndexPartitionOptions options = mOptions;
        options.GetOnlineConfig().onDiskFlushRealtimeIndex = false;
        options.GetOnlineConfig().isIncConsistentWithRealtime = false;
        options.GetBuildConfig(false).enablePackageFile = false;
        options.GetBuildConfig(true).enablePackageFile = false;

        string fullDocString = "cmd=add,pk=1,string1=hello,long1=1,ts=0;"
                               "cmd=delete,pk=1,ts=0;"
                               "cmd=add,pk=2,string1=hello,long1=2,ts=0;"
                               "cmd=add,pk=3,string1=hello,long1=3,ts=0;"
                               "cmd=add,pk=4,string1=hello,long1=4,ts=110;"
                               "cmd=add,pk=5,string1=hello,long1=5,ts=120;"
                               "cmd=add,pk=6,string1=hello,long1=6,ts=120;"
                               "##stopTs=100;";

        string rtDoc1 = "cmd=add,pk=7,string1=hello,long1=7,ts=110;"
                        "cmd=add,pk=8,string1=hello,long1=8,ts=120;"
                        "cmd=delete,pk=7,ts=130;"
                        "cmd=delete,pk=2,ts=130;"
                        "cmd=delete,pk=3,ts=140;";

        string incDoc1 = "cmd=add,pk=3,string1=hello,long1=7,ts=110;"
                         "cmd=add,pk=7,string1=hello,long1=7,ts=110;"
                         "cmd=add,pk=8,string1=hello,long1=8,ts=120;"
                         "cmd=delete,pk=2,ts=130;##stopTs=130;";

        // rt ahead of inc
        InnerTestReOpen(mSchema, options, fullDocString,
                        {{"RT", rtDoc1, "index1:hello", "long1=4;long1=5;long1=6;long1=8;"},
                         {"INC_NO_MERGE", incDoc1, "index1:hello", "long1=4;long1=5;long1=6;long1=8;"}});
    }
    {
        // test redo stop bug aone_id:20681011
        tearDown();
        setUp();
        IndexPartitionOptions options = mOptions;
        options.GetOnlineConfig().buildConfig.maxDocCount = 2;
        options.GetOnlineConfig().onDiskFlushRealtimeIndex = false;
        options.GetOnlineConfig().isIncConsistentWithRealtime = false;
        options.GetBuildConfig(false).enablePackageFile = false;
        options.GetBuildConfig(true).enablePackageFile = false;

        string fullDocString = "cmd=add,pk=1,string1=hello,long1=1,ts=0;"
                               "cmd=add,pk=2,string1=hello,long1=2,ts=0;"
                               "cmd=add,pk=3,string1=hello,long1=3,ts=0;"
                               "cmd=add,pk=4,string1=hello,long1=4,ts=0;"
                               "##stopTs=100;";

        string rtDoc1 = "cmd=add,pk=8,string1=hello,long1=8,ts=120;"
                        "cmd=add,pk=4,string1=hello,long1=44,ts=105;"
                        "cmd=add,pk=1,string1=hello,long1=11,ts=105;"
                        "cmd=add,pk=2,string1=hello,long1=22,ts=105;"
                        "cmd=add,pk=3,string1=hello,long1=33,ts=105;"
                        "cmd=add,pk=4,string1=hello,long1=44,ts=105;"
                        "cmd=add,pk=7,string1=hello,long1=77,ts=125;"
                        "cmd=add,pk=5,string1=hello,long1=55,ts=130;";

        string incDoc1 = "cmd=add,pk=4,string1=hello,long1=44,ts=105;"
                         "cmd=add,pk=1,string1=hello,long1=11,ts=105;"
                         "cmd=add,pk=2,string1=hello,long1=22,ts=105;"
                         "cmd=add,pk=3,string1=hello,long1=33,ts=105;"
                         "cmd=add,pk=4,string1=hello,long1=44,ts=105;"
                         "cmd=add,pk=7,string1=hello,long1=7,ts=105;"
                         "##stopTs=110;";

        InnerTestReOpen(
            mSchema, options, fullDocString,
            {{"RT", rtDoc1, "index1:hello", "long1=8;long1=11;long1=22;long1=33;long1=44;long1=77;long1=55;"},
             {"INC_NO_MERGE", incDoc1, "index1:hello",
              "long1=11;long1=22;long1=33;long1=44;long1=55;long1=77;long1=8;"}});
    }
    {
        // test diffSegReader has delMap
        tearDown();
        setUp();
        IndexPartitionOptions options = mOptions;
        options.GetOfflineConfig().buildConfig.maxDocCount = 2;
        options.GetOnlineConfig().onDiskFlushRealtimeIndex = false;
        options.GetOnlineConfig().isIncConsistentWithRealtime = false;
        options.GetBuildConfig(false).enablePackageFile = false;
        options.GetBuildConfig(true).enablePackageFile = false;

        string fullDocString = "cmd=add,pk=1,string1=hello,long1=1,ts=0;"
                               "cmd=add,pk=2,string1=hello,long1=2,ts=0;"
                               "cmd=add,pk=3,string1=hello,long1=3,ts=0;"
                               "cmd=add,pk=4,string1=hello,long1=4,ts=0;"
                               "##stopTs=100;";

        string rtDoc1 = "cmd=add,pk=5,string1=hello,long1=5,ts=105;"
                        "cmd=add,pk=7,string1=hello,long1=7,ts=105;"
                        "cmd=add,pk=1,string1=hello,long1=11111,ts=175;"
                        "cmd=add,pk=8,string1=hello,long1=8,ts=120;"
                        "cmd=add,pk=5,string1=hello,long1=55,ts=130;";

        string incDoc1 = "cmd=add,pk=5,string1=hello,long1=5,ts=105;"
                         "cmd=add,pk=1,string1=hello,long1=11,ts=105;"
                         "cmd=add,pk=7,string1=hello,long1=7,ts=105;"
                         "cmd=add,pk=1,string1=hello,long1=111,ts=115;"
                         "cmd=delete,pk=8,ts=120;"
                         "cmd=add,pk=1,string1=hello,long1=1111,ts=116;"
                         "##stopTs=110;";

        // for online, needLookupReverse must be false
        InnerTestReOpen(
            mSchema, options, fullDocString,
            {{"RT", rtDoc1, "index1:hello", "long1=11111;long1=2;long1=3;long1=4;long1=55;long1=7;long1=8;"},
             {"INC_NO_MERGE", incDoc1, "index1:hello",
              "long1=11111;long1=2;long1=3;long1=4;long1=55;long1=7;long1=8;"}});
    }
    { // test op's target segment in lastVersion, not in newVersion
        tearDown();
        setUp();
        IndexPartitionOptions options = mOptions;
        options.GetOnlineConfig().onDiskFlushRealtimeIndex = false;
        options.GetOnlineConfig().isIncConsistentWithRealtime = false;
        options.GetBuildConfig(false).maxDocCount = 2;
        // verison.0: s0, s1, s2, s3
        string fullDocString = "cmd=add,pk=1,string1=hello,long1=1,ts=0;"
                               "cmd=add,pk=2,string1=hello,long1=2,ts=0;"
                               "cmd=add,pk=3,string1=hello,long1=3,ts=0;"
                               "cmd=add,pk=4,string1=hello,long1=4,ts=110;"
                               "cmd=add,pk=5,string1=hello,long1=5,ts=120;"
                               "cmd=add,pk=6,string1=hello,long1=6,ts=120;"
                               "cmd=add,pk=7,string1=hello,long1=7,ts=120;"
                               "cmd=add,pk=8,string1=hello,long1=8,ts=120;"
                               "##stopTs=100;";

        string rtDoc = "cmd=add,pk=4,string1=hello,long1=4,ts=110;"
                       "cmd=add,pk=5,string1=hello,long1=5,ts=120;"
                       "cmd=add,pk=6,string1=hello,long1=6,ts=160;"
                       "cmd=add,pk=7,string1=hello,long1=7,ts=170;"
                       "cmd=add,pk=8,string1=hello,long1=8,ts=180;"
                       "cmd=add,pk=9,string1=hello,long1=9,ts=190;"
                       "cmd=update_field,pk=2,long1=20,ts=200;"
                       "cmd=update_field,pk=4,long1=40,ts=200;"
                       "cmd=update_field,pk=5,long1=50,ts=200;"
                       "cmd=update_field,pk=7,long1=70,ts=200;";

        // build version.1 : s0, s1, s2, s3, s4, s5, s6(delay dedup)
        // merge s2,s3,s4,s5->s7
        // merge version.2 : s0, s1, s6, s7
        string incDoc1 = "cmd=add,pk=4,string1=hello,long1=4,ts=110;"
                         "cmd=add,pk=5,string1=hello,long1=5,ts=120;"
                         "cmd=add,pk=6,string1=hello,long1=6,ts=160;"
                         "##stopTs=110;";

        options.GetMergeConfig().mergeStrategyStr = "specific_segments";
        options.GetMergeConfig().mergeStrategyParameter.SetLegacyString("merge_segments=2,3,4,5");

        PartitionStateMachine psm;
        INDEXLIB_TEST_TRUE(psm.Init(mSchema, options, mRootDir));
        INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, fullDocString, "index1:hello",
                                        "long1=1;long1=2;long1=3;long1=4;long1=5;long1=6;long1=7;long1=8;"));
        INDEXLIB_TEST_TRUE(
            psm.Transfer(BUILD_RT, rtDoc, "index1:hello",
                         "long1=1;long1=20;long1=3;long1=40;long1=50;long1=6;long1=70;long1=8;long1=9;"));
        INDEXLIB_TEST_TRUE(
            psm.Transfer(BUILD_INC, incDoc1, "index1:hello",
                         "long1=1;long1=20;long1=3;long1=40;long1=50;long1=6;long1=70;long1=8;long1=9;"));

        string incDoc2 = "cmd=add,pk=4,string1=hello,long1=4,ts=110;"
                         "cmd=add,pk=5,string1=hello,long1=5,ts=120;"
                         "cmd=add,pk=6,string1=hello,long1=6,ts=160;"
                         "cmd=add,pk=7,string1=hello,long1=7,ts=170;"
                         "cmd=add,pk=8,string1=hello,long1=8,ts=180;"
                         "cmd=add,pk=9,string1=hello,long1=9,ts=190;"
                         "cmd=update_field,pk=2,long1=20,ts=200;";

        INDEXLIB_TEST_TRUE(
            psm.Transfer(BUILD_INC_NO_MERGE, incDoc2, "index1:hello",
                         "long1=1;long1=20;long1=3;long1=40;long1=50;long1=6;long1=70;long1=8;long1=9;"));
    }
}

void OnlineOfflineJoinTest::TestDeletionMapMergeWithIncNotConsistentWithRt()
{
    {
        // test deletionMap merge when inc not consistent with rt
        tearDown();
        setUp();
        IndexPartitionOptions options = mOptions;
        options.GetOfflineConfig().buildConfig.maxDocCount = 2;
        options.GetOnlineConfig().onDiskFlushRealtimeIndex = false;
        options.GetOnlineConfig().isIncConsistentWithRealtime = false;
        options.GetBuildConfig(false).enablePackageFile = false;
        options.GetBuildConfig(true).enablePackageFile = false;

        string fullDocString = "cmd=add,pk=1,string1=hello,long1=1,ts=0;"
                               "cmd=add,pk=2,string1=hello,long1=2,ts=0;"
                               "cmd=add,pk=3,string1=hello,long1=3,ts=0;"
                               "cmd=add,pk=4,string1=hello,long1=4,ts=0;"
                               "##stopTs=100;";

        string rtDoc1 = "cmd=add,pk=5,string1=hello,long1=5,ts=105;"
                        "cmd=add,pk=6,string1=hello,long1=6,ts=105;"
                        "cmd=add,pk=7,string1=hello,long1=7,ts=105;"
                        "cmd=add,pk=2,string1=hello,long1=222,ts=130;";

        string incDoc1 = "cmd=add,pk=5,string1=hello,long1=5,ts=105;"
                         "cmd=add,pk=6,string1=hello,long1=6,ts=105;"
                         "cmd=add,pk=1,string1=hello,long1=11,ts=105;"
                         "cmd=add,pk=7,string1=hello,long1=7,ts=105;"
                         "##stopTs=110;";

        // rt ahead of inc
        InnerTestReOpen(
            mSchema, options, fullDocString,
            {{"RT", rtDoc1, "index1:hello", "long1=1;long1=222;long1=3;long1=4;long1=5;long1=6;long1=7;"},
             {"INC_NO_MERGE", incDoc1, "index1:hello", "long1=11;long1=222;long1=3;long1=4;long1=5;long1=6;long1=7;"}});
    }
}

void OnlineOfflineJoinTest::InnerTestReOpen(const IndexPartitionSchemaPtr& schema, const IndexPartitionOptions& options,
                                            const std::string& fullDoc,
                                            const std::vector<std::vector<string>>& buildCmds)
{
    tearDown();
    setUp();
    PartitionStateMachine psm;
    psm.SetPsmOption("resultCheckType", "UNORDERED");
    INDEXLIB_TEST_TRUE(psm.Init(schema, options, mRootDir));
    ASSERT_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, fullDoc, "", ""));

    for (const auto& buildCmd : buildCmds) {
        ASSERT_EQ(4u, buildCmd.size());
        if (buildCmd[0] == string("RT")) {
            ASSERT_TRUE(psm.Transfer(BUILD_RT, buildCmd[1], buildCmd[2], buildCmd[3]));
            continue;
        }
        if (buildCmd[0] == string("INC_NO_MERGE")) {
            ASSERT_TRUE(psm.Transfer(BUILD_INC_NO_MERGE, buildCmd[1], buildCmd[2], buildCmd[3]));
            continue;
        }
        if (buildCmd[0] == string("INC_NO_MERGE_FORCE_REOPEN")) {
            ASSERT_TRUE(psm.Transfer(BUILD_INC_NO_MERGE_FORCE_REOPEN, buildCmd[1], buildCmd[2], buildCmd[3]));
            continue;
        }
        if (buildCmd[0] == string("INC")) {
            ASSERT_TRUE(psm.Transfer(BUILD_INC, buildCmd[1], buildCmd[2], buildCmd[3]));
            continue;
        }
        if (buildCmd[0] == string("QUERY")) {
            ASSERT_TRUE(psm.Transfer(QUERY, buildCmd[1], buildCmd[2], buildCmd[3]));
            continue;
        }
        ASSERT_TRUE(false);
    }
}

void OnlineOfflineJoinTest::TestUpdateDocInSubDocScene()
{
    string field = "pk:string;long1:int32;long2:int32;multi_int:int32:true:true;string1:string;";
    string index = "pk:primarykey64:pk;index1:string:string1";
    string attr = "long1;packAttr1:long2,multi_int";
    string summary = "";
    auto schema = SchemaMaker::MakeSchema(field, index, attr, summary);
    IndexPartitionSchemaPtr subSchema = SchemaMaker::MakeSchema(
        "sub_pk:string;sub_int:int32;sub_multi_int:int32:true:true;substring1:string",
        "sub_pk:primarykey64:sub_pk;sub_index1:string:substring1;", "sub_pk;sub_int;sub_multi_int;", "");
    schema->SetSubIndexPartitionSchema(subSchema);
    IndexPartitionOptions options = mOptions;
    options.GetOnlineConfig().onlineKeepVersionCount = 100;
    options.GetBuildConfig(true).keepVersionCount = 100;
    options.GetBuildConfig(false).enablePackageFile = false;
    options.GetBuildConfig(false).keepVersionCount = 100;

    string fullDocs = "cmd=add,string1=hello,pk=pk1,long1=1,multi_int=1 2 3,long2=1,"
                      "sub_pk=sub11^sub12,substring1=hello^hello,sub_int=1^2,sub_multi_int=11 12^12 13,ts=0;"
                      "cmd=add,string1=hello,pk=pk2,long1=2,multi_int=2 2 3,long2=2,"
                      "sub_pk=sub21^sub22,substring1=hello^hello,sub_int=2^3,sub_multi_int=21 22^22 23,ts=0;"
                      "cmd=add,string1=hello,pk=pk3,long1=3,multi_int=3 2 3,long2=3,"
                      "sub_pk=sub31^sub32,substring1=hello^hello,sub_int=3^4,sub_multi_int=31 32^32 33,ts=10;"
                      "cmd=add,string1=hello,pk=pk4,long1=4,multi_int=4 2 3,long2=4,"
                      "sub_pk=sub41^sub42,substring1=hello^hello,sub_int=4^5,sub_multi_int=41 42^42 43,ts=10;";

    string rtDoc1 = "cmd=add,string1=hello,pk=pk5,long1=5,multi_int=5 2 3,long2=5,"
                    "sub_pk=sub51^sub52,substring1=hello^hello,sub_int=5^6,sub_multi_int=51 52^52 53,ts=23;"
                    "cmd=add,string1=hello,pk=pk6,long1=6,multi_int=6 2 3,long2=6,"
                    "sub_pk=sub61^sub62,substring1=hello^hello,sub_int=6^7,sub_multi_int=61 62^62 63,ts=24;"
                    "cmd=delete,pk=pk1,ts=25;"
                    "cmd=delete_sub,pk=pk2,sub_pk=sub21,ts=26;"
                    "cmd=update_field,pk=pk3,sub_pk=sub31,sub_int=30,sub_multi_int=310 320,ts=27;";

    string incDoc1 = "cmd=add,string1=hello,pk=pk5,long1=5,multi_int=5 2 3,long2=5,"
                     "sub_pk=sub51^sub52,substring1=hello^hello,sub_int=5^6,sub_multi_int=51 52^52 53,ts=23;"
                     "cmd=add,string1=hello,pk=pk6,long1=6,multi_int=6 2 3,long2=6,"
                     "sub_pk=sub61^sub62,substring1=hello^hello,sub_int=6^7,sub_multi_int=61 62^62 63,ts=24;"
                     "##stopTs=24;";

    string rtDoc2 = "cmd=add,string1=hello,pk=pk5,long1=50,multi_int=50 2 3,long2=50,"
                    "sub_pk=sub51^sub52,substring1=hello^hello,sub_int=50^60,sub_multi_int=510 520^520 530,ts=29;"
                    "cmd=update_field,pk=pk6,sub_pk=sub62,sub_int=70,sub_multi_int=620 630,ts=30;";

    string incDoc2 = "cmd=add,string1=hello,pk=pk6,long1=6,multi_int=6 2 3,long2=6,"
                     "sub_pk=sub61^sub62,substring1=hello^hello,sub_int=6^7,sub_multi_int=61 62^62 63,ts=24;"
                     "cmd=delete,pk=pk1,ts=25;"
                     "cmd=delete_sub,pk=pk2,sub_pk=sub21,ts=26;"
                     "cmd=update_field,pk=pk3,sub_pk=sub31,sub_int=30,sub_multi_int=310 320,ts=27;"
                     "##stopTs=28";

    InnerTestReOpen(
        schema, options, fullDocs,
        {{"RT", rtDoc1, "index1:hello", "long1=2;long1=3;long1=4;long1=5;long1=6"},
         {"QUERY", "", "sub_index1:hello",
          "sub_int=3;sub_int=30;sub_int=4;sub_int=4;sub_int=5;sub_int=5;sub_int=6;sub_int=6;sub_int=7"},
         {"INC_NO_MERGE", incDoc1, "index1:hello", "long1=2;long1=3;long1=4;long1=5;long1=6"},
         {"QUERY", "", "sub_index1:hello",
          "sub_int=3;sub_int=30;sub_int=4;sub_int=4;sub_int=5;sub_int=5;sub_int=6;sub_int=6;sub_int=7"},
         {"RT", rtDoc2, "index1:hello", "long1=2;long1=3;long1=4;long1=50;long1=6"},
         {"QUERY", "", "sub_index1:hello",
          "sub_int=3;sub_int=30;sub_int=4;sub_int=4;sub_int=5;sub_int=50;sub_int=60;sub_int=6;sub_int=70"},
         {"INC_NO_MERGE", incDoc2, "index1:hello", "long1=2;long1=3;long1=4;long1=50;long1=6"},
         {"QUERY", "", "sub_index1:hello",
          "sub_int=3;sub_int=30;sub_int=4;sub_int=4;sub_int=5;sub_int=50;sub_int=60;sub_int=6;sub_int=70"}});
}
}
}
