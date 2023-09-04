#include "indexlib/common_define.h"
#include "indexlib/config/disable_fields_config.h"
#include "indexlib/file_system/IFileSystem.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/index_base/schema_adapter.h"
#include "indexlib/test/partition_state_machine.h"
#include "indexlib/test/schema_maker.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/util/PathUtil.h"
#include "indexlib/util/test/build_test_util.h"

using namespace std;
using namespace indexlib::test;
using namespace indexlib::config;
using namespace indexlib::file_system;
using namespace indexlib::util;

namespace indexlib { namespace partition {

class RecoverRtIndexTest : public INDEXLIB_TESTBASE_WITH_PARAM<int>
{
public:
    RecoverRtIndexTest();
    ~RecoverRtIndexTest();

    DECLARE_CLASS_NAME(RecoverRtIndexTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSimpleProcess();
    void TestRecoverWithSubSchema();
    void TestAddAndDelete();
    void TestAddAndUpdate();
    void TestAddUpdateDelete();
    void TestRtIndexNotComplete();
    void TestKV();
    void TestKKV();
    void TestNoEnoughMemory();
    void TestOneSegment();
    void TestRecoverRtWithDisable();
    void TestRecoverWhenReopen();
    void TestRecoverWithReopenTwice();
    // void TestRecoverSrcIndex();
private:
    IndexPartitionSchemaPtr MakeSchema(bool needSub);

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(RecoverRtIndexTest, TestSimpleProcess);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(RecoverRtIndexTest, TestRecoverWithSubSchema);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(RecoverRtIndexTest, TestAddAndDelete);
// online realtime flush on disk not support update
// INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(RecoverRtIndexTest, TestAddAndUpdate);
// INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(RecoverRtIndexTest, TestAddUpdateDelete);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(RecoverRtIndexTest, TestRtIndexNotComplete);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(RecoverRtIndexTest, TestKV);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(RecoverRtIndexTest, TestKKV);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(RecoverRtIndexTest, TestNoEnoughMemory);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(RecoverRtIndexTest, TestOneSegment);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(RecoverRtIndexTest, TestRecoverRtWithDisable);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(RecoverRtIndexTest, TestRecoverWhenReopen);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(RecoverRtIndexTest, TestRecoverWithReopenTwice);
// INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(RecoverRtIndexTest, TestRecoverSrcIndex);

INSTANTIATE_TEST_CASE_P(BuildMode, RecoverRtIndexTest, testing::Values(0, 1, 2));

IE_LOG_SETUP(partition, RecoverRtIndexTest);

RecoverRtIndexTest::RecoverRtIndexTest() {}

RecoverRtIndexTest::~RecoverRtIndexTest() {}

void RecoverRtIndexTest::CaseSetUp() {}

void RecoverRtIndexTest::CaseTearDown() {}

// void RecoverRtIndexTest::TestRecoverSrcIndex()
// {
//     std::string root = "/home/yijie.zhang/error_index/04_07/partition_0_65535";
//     string baseRootPath = GET_TEMP_DATA_PATH();
//     storage::FileSystemWrapper::Copy(root, baseRootPath);
//     string rootPath = baseRootPath + "/partition_0_65535/";
//     IndexPartitionSchemaPtr schema = index_base::SchemaAdapter::LoadSchema(rootPath);
//     IndexPartitionOptions options =
//     util::BuildTestUtil::GetIndexPartitionOptionsForBuildMode(/*buildMode=*/GetParam());
//     options.GetOnlineConfig().onDiskFlushRealtimeIndex = true;
//     options.GetOnlineConfig().loadRemainFlushRealtimeIndex = true;
//     PartitionStateMachine psm;
//     ASSERT_TRUE(psm.Init(schema, options, rootPath));
//     ASSERT_TRUE(psm.Transfer(QUERY, "", "pk:120631911_4168542187", "docid=1"));
// }

void RecoverRtIndexTest::TestSimpleProcess()
{
    auto schema = MakeSchema(false);
    IndexPartitionOptions options = util::BuildTestUtil::GetIndexPartitionOptionsForBuildMode(/*buildMode=*/GetParam());
    options.GetOnlineConfig().buildConfig.speedUpPrimaryKeyReader = true;
    options.GetOnlineConfig().buildConfig.speedUpPrimaryKeyReaderParam = "combine_segments=true,max_doc_count=10";
    options.GetOnlineConfig().onDiskFlushRealtimeIndex = true;
    options.GetOnlineConfig().loadRemainFlushRealtimeIndex = true;
    string rootPath = GET_TEMP_DATA_PATH();
    {
        PartitionStateMachine psm;
        ASSERT_TRUE(psm.Init(schema, options, rootPath));

        ASSERT_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, "", "", ""));

        string rtDocs = "cmd=add,pk=3,string1=hello,long1=1,ts=4;"
                        "cmd=add,pk=3,string1=hello,long1=4,ts=4;"
                        "cmd=add,pk=4,string1=hello,long1=2,ts=5;";
        ASSERT_TRUE(psm.Transfer(BUILD_RT_SEGMENT, rtDocs, "index1:hello", "docid=1;docid=2"));

        // delete doc in building  segment
        rtDocs = "cmd=add,pk=3,string1=hello,long1=3,ts=4;"
                 "cmd=delete,pk=3,ts=6;";
        ASSERT_TRUE(psm.Transfer(BUILD_RT_SEGMENT, rtDocs, "index1:hello", "docid=2;"));
        IndexPartitionPtr indexPart = psm.GetIndexPartition();
        DirectoryPtr rootDir = indexPart->GetRootDirectory();
        IFileSystemPtr fileSystem = rootDir->GetFileSystem();
        fileSystem->Sync(true).GetOrThrow();
    }
    PartitionStateMachine psm1;
    ASSERT_TRUE(psm1.Init(schema, options, rootPath));
    ASSERT_TRUE(psm1.Transfer(QUERY, "", "index1:hello", "docid=2;"));
}

void RecoverRtIndexTest::TestRecoverWhenReopen()
{
    auto schema = MakeSchema(false);
    IndexPartitionOptions options = util::BuildTestUtil::GetIndexPartitionOptionsForBuildMode(/*buildMode=*/GetParam());
    options.GetOnlineConfig().buildConfig.speedUpPrimaryKeyReader = true;
    options.GetOnlineConfig().buildConfig.speedUpPrimaryKeyReaderParam = "combine_segments=true,max_doc_count=10";
    options.GetOnlineConfig().onDiskFlushRealtimeIndex = true;
    options.GetOnlineConfig().loadRemainFlushRealtimeIndex = true;
    string rootPath = GET_TEMP_DATA_PATH();
    {
        PartitionStateMachine psm;
        ASSERT_TRUE(psm.Init(schema, options, rootPath));

        ASSERT_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, "", "", ""));

        string rtDocs = "cmd=add,pk=3,string1=hello,long1=1,ts=4;"
                        "cmd=add,pk=4,string1=hello,long1=2,ts=5;";
        ASSERT_TRUE(psm.Transfer(BUILD_RT_SEGMENT, rtDocs, "index1:hello", "docid=0,long1=1;docid=1,long1=2"));

        string rtDocs1 = "cmd=add,pk=3,string1=hello,long1=3,ts=6;"
                         "cmd=add,pk=4,string1=hello,long1=4,ts=7;";
        ASSERT_TRUE(psm.Transfer(BUILD_RT, rtDocs1, "index1:hello", "docid=2,long1=3;docid=3,long1=4"));

        string incDocs = "cmd=add,pk=3,string1=hello,long1=5,ts=1;"
                         "cmd=add,pk=4,string1=hello,long1=6,ts=2;";
        ASSERT_TRUE(psm.Transfer(BUILD_INC, incDocs, "index1:hello", "docid=4,long1=3;docid=5,long1=4"));

        string rtDocs2 = "cmd=add,pk=5,string1=hello1,long1=3,ts=8;";
        ASSERT_TRUE(psm.Transfer(BUILD_RT_SEGMENT, rtDocs2, "index1:hello", "docid=4,long1=3;docid=5,long1=4"));

        IndexPartitionPtr indexPart = psm.GetIndexPartition();
        DirectoryPtr rootDir = indexPart->GetRootDirectory();
        IFileSystemPtr fileSystem = rootDir->GetFileSystem();
        fileSystem->Sync(true).GetOrThrow();
    }

    PartitionStateMachine psm;
    ASSERT_TRUE(psm.Init(schema, options, rootPath));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "index1:hello", "docid=4,long1=3;docid=5,long1=4"));
}

void RecoverRtIndexTest::TestRecoverWithReopenTwice()
{
    auto schema = MakeSchema(false);
    IndexPartitionOptions options = util::BuildTestUtil::GetIndexPartitionOptionsForBuildMode(/*buildMode=*/GetParam());
    options.GetOnlineConfig().buildConfig.speedUpPrimaryKeyReader = true;
    options.GetOnlineConfig().buildConfig.speedUpPrimaryKeyReaderParam = "combine_segments=true,max_doc_count=10";
    options.GetOnlineConfig().onDiskFlushRealtimeIndex = true;
    options.GetOnlineConfig().loadRemainFlushRealtimeIndex = true;
    string rootPath = GET_TEMP_DATA_PATH();
    {
        PartitionStateMachine psm;
        ASSERT_TRUE(psm.Init(schema, options, rootPath));

        ASSERT_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, "", "", ""));

        string rtDocs = "cmd=add,pk=4,string1=hello,long1=1,ts=4;"
                        "cmd=add,pk=4,string1=hello,long1=2,ts=5;";
        ASSERT_TRUE(psm.Transfer(BUILD_RT_SEGMENT, rtDocs, "index1:hello", "docid=1,long1=2"));

        string rtDocs1 = "cmd=add,pk=4,string1=hello,long1=3,ts=6;"
                         "cmd=add,pk=5,string1=hello,long1=4,ts=7;";
        ASSERT_TRUE(psm.Transfer(BUILD_RT, rtDocs1, "index1:hello", "docid=2,long1=3;docid=3,long1=4"));

        string incDocs = "cmd=add,pk=6,string1=hello1,long1=6,ts=2;";
        ASSERT_TRUE(psm.Transfer(BUILD_INC, incDocs, "index1:hello", "docid=3,long1=3;docid=4,long1=4"));

        IndexPartitionPtr indexPart = psm.GetIndexPartition();
        DirectoryPtr rootDir = indexPart->GetRootDirectory();
        IFileSystemPtr fileSystem = rootDir->GetFileSystem();
        fileSystem->Sync(true).GetOrThrow();
    }

    {
        PartitionStateMachine psm;
        ASSERT_TRUE(psm.Init(schema, options, rootPath));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "index1:hello", "docid=3,long1=3;docid=4,long1=4"));
    }

    {
        PartitionStateMachine psm;
        ASSERT_TRUE(psm.Init(schema, options, rootPath));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "index1:hello", "docid=3,long1=3;docid=4,long1=4"));
    }
}

void RecoverRtIndexTest::TestRecoverRtWithDisable()
{
    auto schema = MakeSchema(false);
    IndexPartitionOptions options = util::BuildTestUtil::GetIndexPartitionOptionsForBuildMode(/*buildMode=*/GetParam());
    options.GetOnlineConfig().buildConfig.speedUpPrimaryKeyReader = true;
    options.GetOnlineConfig().buildConfig.speedUpPrimaryKeyReaderParam = "combine_segments=true,max_doc_count=10";
    options.GetOnlineConfig().onDiskFlushRealtimeIndex = true;
    options.GetOnlineConfig().loadRemainFlushRealtimeIndex = true;
    auto& disableFieldsConfig = options.GetOnlineConfig().GetDisableFieldsConfig();
    disableFieldsConfig.attributes.push_back("long1");
    string rootPath = GET_TEMP_DATA_PATH();
    index_base::Version rtVersion;
    {
        PartitionStateMachine psm;
        ASSERT_TRUE(psm.Init(schema, options, rootPath));

        ASSERT_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, "", "", ""));

        string rtDocs = "cmd=add,pk=3,string1=hello,long1=1,ts=4;"
                        "cmd=add,pk=3,string1=hello,long1=4,ts=4;"
                        "cmd=add,pk=4,string1=hello,long1=2,ts=5;";
        ASSERT_TRUE(psm.Transfer(BUILD_RT_SEGMENT, rtDocs, "index1:hello", "docid=1;docid=2"));

        // delete doc in building  segment
        rtDocs = "cmd=add,pk=3,string1=hello,long1=3,ts=4;"
                 "cmd=delete,pk=3,ts=6;";
        ASSERT_TRUE(psm.Transfer(BUILD_RT_SEGMENT, rtDocs, "index1:hello", "docid=2;"));
        IndexPartitionPtr indexPart = psm.GetIndexPartition();
        DirectoryPtr rootDir = indexPart->GetRootDirectory();
        IFileSystemPtr fileSystem = rootDir->GetFileSystem();
        fileSystem->Sync(true).GetOrThrow();
        rtVersion = indexPart->GetReader()->GetVersion();
        // version not valid, some segment not exist
        auto version = rtVersion;
        version.AddSegment(version.GetLastSegment() + 1);
        version.SetVersionId(5);
        string rtIndexPath = PathUtil::JoinPath(rootPath, RT_INDEX_PARTITION_DIR_NAME);
        version.TEST_Store(rtIndexPath, false);
    }

    {
        PartitionStateMachine psm1;
        ASSERT_TRUE(psm1.Init(schema, options, rootPath));
        ASSERT_TRUE(psm1.Transfer(QUERY, "", "index1:hello", "docid=2;"));
    }
}

void RecoverRtIndexTest::TestRtIndexNotComplete()
{
    auto schema = MakeSchema(false);
    IndexPartitionOptions options = util::BuildTestUtil::GetIndexPartitionOptionsForBuildMode(/*buildMode=*/GetParam());
    options.GetOnlineConfig().buildConfig.speedUpPrimaryKeyReader = true;
    options.GetOnlineConfig().buildConfig.speedUpPrimaryKeyReaderParam = "combine_segments=true,max_doc_count=10";
    options.GetOnlineConfig().onDiskFlushRealtimeIndex = true;
    options.GetOnlineConfig().loadRemainFlushRealtimeIndex = true;
    string rootPath = GET_TEMP_DATA_PATH();
    segmentid_t lastSegmentId;
    index_base::Version rtVersion;
    {
        PartitionStateMachine psm;
        ASSERT_TRUE(psm.Init(schema, options, rootPath));

        ASSERT_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, "", "", ""));

        string rtDocs = "cmd=add,pk=3,string1=hello,long1=1,ts=4;"
                        "cmd=add,pk=3,string1=hello,long1=4,ts=4;"
                        "cmd=add,pk=4,string1=hello,long1=2,ts=5;";
        ASSERT_TRUE(psm.Transfer(BUILD_RT_SEGMENT, rtDocs, "index1:hello", "docid=1;docid=2"));

        // delete doc in building  segment
        rtDocs = "cmd=add,pk=3,string1=hello,long1=3,ts=4;"
                 "cmd=delete,pk=3,ts=6;";
        ASSERT_TRUE(psm.Transfer(BUILD_RT_SEGMENT, rtDocs, "index1:hello", "docid=2;"));
        IndexPartitionPtr indexPart = psm.GetIndexPartition();
        DirectoryPtr rootDir = indexPart->GetRootDirectory();
        IFileSystemPtr fileSystem = rootDir->GetFileSystem();
        fileSystem->Sync(true).GetOrThrow();
        rtVersion = indexPart->GetReader()->GetVersion();
        // version not valid, some segment not exist
        auto version = rtVersion;
        version.AddSegment(version.GetLastSegment() + 1);
        version.SetVersionId(5);
        string rtIndexPath = PathUtil::JoinPath(rootPath, RT_INDEX_PARTITION_DIR_NAME);
        version.TEST_Store(rtIndexPath, false);
        lastSegmentId = rtVersion.GetLastSegment();
    }

    {
        PartitionStateMachine psm1;
        ASSERT_TRUE(psm1.Init(schema, options, rootPath));
        ASSERT_TRUE(psm1.Transfer(QUERY, "", "index1:hello", "docid=2;"));
    }

    {
        // some segment no segment info
        string rtIndexPath = PathUtil::JoinPath(rootPath, RT_INDEX_PARTITION_DIR_NAME);
        stringstream segmentInfoPath;
        segmentInfoPath << "segment_" << lastSegmentId << "_level_0/segment_info";
        string segmentInfo = PathUtil::JoinPath(rtIndexPath, segmentInfoPath.str());
        PartitionStateMachine psm1;
        ASSERT_TRUE(psm1.Init(schema, options, rootPath));
        FslibWrapper::DeleteFileE(segmentInfo, DeleteOption::NoFence(false));
        ASSERT_TRUE(psm1.Transfer(QUERY, "", "index1:hello", "docid=1;docid=2;"));
    }
}

void RecoverRtIndexTest::TestRecoverWithSubSchema()
{
    // todo add delete update
    auto schema = MakeSchema(true);
    IndexPartitionOptions options = util::BuildTestUtil::GetIndexPartitionOptionsForBuildMode(/*buildMode=*/GetParam());
    options.GetOnlineConfig().buildConfig.speedUpPrimaryKeyReader = true;
    options.GetOnlineConfig().buildConfig.speedUpPrimaryKeyReaderParam = "combine_segments=true,max_doc_count=10";
    options.GetOnlineConfig().onDiskFlushRealtimeIndex = true;
    options.GetOnlineConfig().loadRemainFlushRealtimeIndex = true;
    string rootPath = GET_TEMP_DATA_PATH();
    {
        PartitionStateMachine psm;

        ASSERT_TRUE(psm.Init(schema, options, rootPath));

        ASSERT_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, "", "", ""));

        string rtDocs = "cmd=add,pk=3,string1=hello,subpkstr=sub1^sub2,substr1=s1^s1;"
                        "cmd=add,pk=4,string1=hello,subpkstr=sub3^sub4,substr1=s1^s1;";
        ASSERT_TRUE(psm.Transfer(BUILD_RT_SEGMENT, rtDocs, "index1:hello", "docid=0;docid=1"));

        // delete doc in building  segment
        rtDocs = "cmd=delete,pk=3;"
                 "cmd=delete_sub,pk=4,subpkstr=sub4;";
        ASSERT_TRUE(psm.Transfer(BUILD_RT_SEGMENT, rtDocs, "index1:hello", "docid=1;"));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "subindex1:s1", "subpkstr=sub3;"));
        IndexPartitionPtr indexPart = psm.GetIndexPartition();
        DirectoryPtr rootDir = indexPart->GetRootDirectory();
        IFileSystemPtr fileSystem = rootDir->GetFileSystem();
        fileSystem->Sync(true).GetOrThrow();
    }
    PartitionStateMachine psm1;
    ASSERT_TRUE(psm1.Init(schema, options, rootPath));
    ASSERT_TRUE(psm1.Transfer(QUERY, "", "index1:hello", "docid=1;"));
    ASSERT_TRUE(psm1.Transfer(QUERY, "", "subindex1:s1", "subpkstr=sub3;"));
}

IndexPartitionSchemaPtr RecoverRtIndexTest::MakeSchema(bool needSub)
{
    string field = "pk:uint64:pk;string1:string;long1:uint32";
    string index = "pk:primarykey64:pk;index1:string:string1";
    string attr = "long1;pk;";
    string summary = "string1;";
    auto schema = SchemaMaker::MakeSchema(field, index, attr, summary);
    if (needSub) {
        string subfield = "substr1:string;subpkstr:string;sub_long:uint32;";
        string subindex = "subindex1:string:substr1;sub_pk:primarykey64:subpkstr;";
        string subattr = "substr1;subpkstr;sub_long;";
        string subsummary = "";

        IndexPartitionSchemaPtr subSchema = SchemaMaker::MakeSchema(subfield, subindex, subattr, subsummary);
        schema->SetSubIndexPartitionSchema(subSchema);
    }
    return schema;
}

void RecoverRtIndexTest::TestAddAndDelete()
{
    {
        // add & delete : A0 A1 A2 A3 A1 D3 A4 A5 | D2 A5 D0
        // expect 1, 4, 5 exist
        tearDown();
        setUp();
        auto schema = MakeSchema(false);
        IndexPartitionOptions options =
            util::BuildTestUtil::GetIndexPartitionOptionsForBuildMode(/*buildMode=*/GetParam());
        options.GetOnlineConfig().buildConfig.maxDocCount = 10;
        options.GetOfflineConfig().buildConfig.maxDocCount = 10;
        options.GetOnlineConfig().onDiskFlushRealtimeIndex = true;
        options.GetOnlineConfig().loadRemainFlushRealtimeIndex = true;
        string rootPath = GET_TEMP_DATA_PATH();
        {
            PartitionStateMachine psm;
            ASSERT_TRUE(psm.Init(schema, options, rootPath));
            ASSERT_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, "", "", ""));

            string rtDocs = "cmd=add,pk=0,string1=seg1,ts=0;"
                            "cmd=add,pk=1,string1=seg1,ts=1;"
                            "cmd=add,pk=2,string1=seg1,ts=2;"
                            "cmd=add,pk=3,string1=seg1,ts=3;"
                            "cmd=add,pk=1,string1=seg1v2,ts=4;"
                            "cmd=delete,pk=3,ts=5;"
                            "cmd=add,pk=4,string1=seg1,ts=6;"
                            "cnd=add,pk=5,string1=seg1,ts=7";
            ASSERT_TRUE(psm.Transfer(BUILD_RT_SEGMENT, rtDocs, "", ""));
            // 0,2,4,5 and 1 exist
            ASSERT_TRUE(psm.Transfer(QUERY, "", "index1:seg1", "docid=0,pk=0;docid=2,pk=2;docid=5,pk=4;docid=6,pk=5;"));
            ASSERT_TRUE(psm.Transfer(QUERY, "", "index1:seg1v2", "docid=4,pk=1;"));

            rtDocs = "cmd=delete,pk=2,ts=8;"
                     "cmd=add,pk=5,string1=seg2,ts=9;"
                     "cmd=delete,pk=0,ts=10";
            ASSERT_TRUE(psm.Transfer(BUILD_RT_SEGMENT, rtDocs, "", ""));
            // 1,4,5 exist
            ASSERT_TRUE(psm.Transfer(QUERY, "", "index1:seg1", "docid=5,pk=4;"));
            ASSERT_TRUE(psm.Transfer(QUERY, "", "index1:seg1v2", "docid=4,pk=1;"));
            ASSERT_TRUE(psm.Transfer(QUERY, "", "index1:seg2", "docid=7,pk=5;"));

            IndexPartitionPtr indexPart = psm.GetIndexPartition();
            DirectoryPtr rootDir = indexPart->GetRootDirectory();
            IFileSystemPtr fileSystem = rootDir->GetFileSystem();
            fileSystem->Sync(true).GetOrThrow();
        }
        PartitionStateMachine psm1;
        ASSERT_TRUE(psm1.Init(schema, options, rootPath));
        ASSERT_TRUE(psm1.Transfer(QUERY, "", "index1:seg1", "docid=5,pk=4;"));
        ASSERT_TRUE(psm1.Transfer(QUERY, "", "index1:seg1v2", "docid=4,pk=1;"));
        ASSERT_TRUE(psm1.Transfer(QUERY, "", "index1:seg2", "docid=7,pk=5;"));
        ASSERT_FALSE(psm1.Transfer(QUERY, "", "pk:0", "string1=seg1"));
        ASSERT_FALSE(psm1.Transfer(QUERY, "", "pk:2", "string1=seg1"));
    }

    {
        // add & delete : A0 A1 A2 A3 A1 D3 A4 A5 | D2 A2 A3 A5 D0
        // expect 1, 2, 3, 4, 5 exist
        tearDown();
        setUp();
        auto schema = MakeSchema(false);
        IndexPartitionOptions options =
            util::BuildTestUtil::GetIndexPartitionOptionsForBuildMode(/*buildMode=*/GetParam());
        options.GetOnlineConfig().buildConfig.maxDocCount = 10;
        options.GetOfflineConfig().buildConfig.maxDocCount = 10;
        options.GetOnlineConfig().onDiskFlushRealtimeIndex = true;
        options.GetOnlineConfig().loadRemainFlushRealtimeIndex = true;
        string rootPath = GET_TEMP_DATA_PATH();
        {
            PartitionStateMachine psm;
            ASSERT_TRUE(psm.Init(schema, options, rootPath));
            ASSERT_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, "", "", ""));

            string rtDocs = "cmd=add,pk=0,string1=seg1,ts=0;"
                            "cmd=add,pk=1,string1=seg1,ts=1;"
                            "cmd=add,pk=2,string1=seg1,ts=2;"
                            "cmd=add,pk=3,string1=seg1,ts=3;"
                            "cmd=add,pk=1,string1=seg1v2,ts=4;"
                            "cmd=delete,pk=3,ts=5;"
                            "cmd=add,pk=4,string1=seg1,ts=6;"
                            "cnd=add,pk=5,string1=seg1,ts=7";
            ASSERT_TRUE(psm.Transfer(BUILD_RT_SEGMENT, rtDocs, "", ""));
            // 0,2,4,5 & 1 exist
            ASSERT_TRUE(psm.Transfer(QUERY, "", "index1:seg1", "docid=0,pk=0;docid=2,pk=2;docid=5,pk=4;docid=6,pk=5;"));
            ASSERT_TRUE(psm.Transfer(QUERY, "", "index1:seg1v2", "docid=4,pk=1;"));

            rtDocs = "cmd=delete,pk=2,ts=8;"
                     "cmd=add,pk=2,string1=seg2,ts=9;"
                     "cmd=add,pk=3,string1=seg2,ts=10;"
                     "cmd=add,pk=5,string1=seg2,ts=11;"
                     "cmd=delete,pk=0,ts=12";
            // 4 & 1 & 2,3,5 exist
            ASSERT_TRUE(psm.Transfer(BUILD_RT_SEGMENT, rtDocs, "", ""));
            ASSERT_TRUE(psm.Transfer(QUERY, "", "index1:seg1", "docid=5,pk=4;"));
            ASSERT_TRUE(psm.Transfer(QUERY, "", "index1:seg1v2", "docid=4,pk=1;"));
            ASSERT_TRUE(psm.Transfer(QUERY, "", "index1:seg2", "docid=7,pk=2;docid=8,pk=3;docid=9,pk=5;"));

            IndexPartitionPtr indexPart = psm.GetIndexPartition();
            DirectoryPtr rootDir = indexPart->GetRootDirectory();
            IFileSystemPtr fileSystem = rootDir->GetFileSystem();
            fileSystem->Sync(true).GetOrThrow();
        }
        PartitionStateMachine psm1;
        ASSERT_TRUE(psm1.Init(schema, options, rootPath));
        ASSERT_TRUE(psm1.Transfer(QUERY, "", "index1:seg1", "docid=5,pk=4;"));
        ASSERT_TRUE(psm1.Transfer(QUERY, "", "index1:seg1v2", "docid=4,pk=1;"));
        ASSERT_TRUE(psm1.Transfer(QUERY, "", "index1:seg2", "docid=7,pk=2;docid=8,pk=3;docid=9,pk=5;"));
    }
    {
        // add & delete : A0 A1 A2 A3 D1 D1 A1 A4 | D2 D2 A2 D3 D3 D0
        // duplicate delete will be unique, redo will not have continue delete cmd with same pk
        // expect 1, 2, 4 exist
        tearDown();
        setUp();
        auto schema = MakeSchema(false);
        IndexPartitionOptions options =
            util::BuildTestUtil::GetIndexPartitionOptionsForBuildMode(/*buildMode=*/GetParam());
        options.GetOnlineConfig().buildConfig.maxDocCount = 10;
        options.GetOfflineConfig().buildConfig.maxDocCount = 10;
        options.GetOnlineConfig().onDiskFlushRealtimeIndex = true;
        options.GetOnlineConfig().loadRemainFlushRealtimeIndex = true;
        string rootPath = GET_TEMP_DATA_PATH();
        {
            PartitionStateMachine psm;
            ASSERT_TRUE(psm.Init(schema, options, rootPath));
            ASSERT_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, "", "", ""));

            string rtDocs = "cmd=add,pk=0,string1=seg1,ts=0;"
                            "cmd=add,pk=1,string1=seg1,ts=1;"
                            "cmd=add,pk=2,string1=seg1,ts=2;"
                            "cmd=add,pk=3,string1=seg1,ts=3;"
                            "cmd=delete,pk=1,ts=4;"
                            "cmd=delete,pk=1,ts=5;"
                            "cmd=add,pk=1,string1=seg1v2,ts=6;"
                            "cnd=add,pk=4,string1=seg1,ts=7";
            ASSERT_TRUE(psm.Transfer(BUILD_RT_SEGMENT, rtDocs, "", ""));
            // 0,2,3,4 & 1
            ASSERT_TRUE(psm.Transfer(QUERY, "", "index1:seg1", "docid=0,pk=0;docid=2,pk=2;docid=3,pk=3;docid=5,pk=4;"));
            ASSERT_TRUE(psm.Transfer(QUERY, "", "index1:seg1v2", "docid=4,pk=1;"));

            rtDocs = "cmd=delete,pk=2,ts=8;"
                     "cmd=delete,pk=2,ts=9;"
                     "cmd=add,pk=2,string1=seg2,ts=9;"
                     "cmd=delete,pk=3,ts=10;"
                     "cmd=delete,pk=3,ts=11;"
                     "cmd=delete,pk=0,ts=12";
            ASSERT_TRUE(psm.Transfer(BUILD_RT_SEGMENT, rtDocs, "", ""));
            // 4 & 1 & 2 exist
            ASSERT_TRUE(psm.Transfer(QUERY, "", "index1:seg1", "docid=5,pk=4;"));
            ASSERT_TRUE(psm.Transfer(QUERY, "", "index1:seg1v2", "docid=4,pk=1;"));
            ASSERT_TRUE(psm.Transfer(QUERY, "", "index1:seg2", "docid=6,pk=2;"));

            IndexPartitionPtr indexPart = psm.GetIndexPartition();
            DirectoryPtr rootDir = indexPart->GetRootDirectory();
            IFileSystemPtr fileSystem = rootDir->GetFileSystem();
            fileSystem->Sync(true).GetOrThrow();
        }
        PartitionStateMachine psm1;
        ASSERT_TRUE(psm1.Init(schema, options, rootPath));
        ASSERT_TRUE(psm1.Transfer(QUERY, "", "index1:seg1", "docid=5,pk=4;"));
        ASSERT_TRUE(psm1.Transfer(QUERY, "", "index1:seg1v2", "docid=4,pk=1;"));
        ASSERT_TRUE(psm1.Transfer(QUERY, "", "index1:seg2", "docid=6,pk=2;"));
    }
}

void RecoverRtIndexTest::TestAddAndUpdate()
{
    {
        // add & update : A0 A1 U1 A2 A3 | U2 A2 U2 U3 U3 | U3
        // expect 0,1,2,3
    } {
        // add & update : U0 U1 | A0 A1 U0 U1 | U0 U1
        // expect 0, 1
        tearDown();
        setUp();
        auto schema = MakeSchema(false);
        IndexPartitionOptions options =
            util::BuildTestUtil::GetIndexPartitionOptionsForBuildMode(/*buildMode=*/GetParam());
        options.GetOnlineConfig().buildConfig.maxDocCount = 10;
        options.GetOfflineConfig().buildConfig.maxDocCount = 10;
        options.GetOnlineConfig().onDiskFlushRealtimeIndex = true;
        options.GetOnlineConfig().loadRemainFlushRealtimeIndex = true;
        string rootPath = GET_TEMP_DATA_PATH();
        {
            PartitionStateMachine psm;
            ASSERT_TRUE(psm.Init(schema, options, rootPath));
            ASSERT_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, "", "", ""));

            string rtDocs = "cmd=update_field,pk=0,string1=seg1,string2=hello,ts=0;"
                            "cmd=update_field,pk=1,string1=seg1,string2=hello,ts=1";
            ASSERT_TRUE(psm.Transfer(BUILD_RT_SEGMENT, rtDocs, "", ""));
            // none exist
            ASSERT_FALSE(psm.Transfer(QUERY, "", "index1:seg1", "docid=0,pk=0;docid=1;pk=1"));

            rtDocs = "cmd=add,pk=0,string1=seg2,string2=hello,ts=2;"
                     "cmd=add,pk=1,string1=seg2,string2=hello,ts=3;"
                     "cmd=update_field,pk=0,string1=seg2v2,string2=hello,ts=4;"
                     "cmd=update_field,pk=1,string1=seg2v3,string2=hello,ts=5";
            ASSERT_TRUE(psm.Transfer(BUILD_RT_SEGMENT, rtDocs, "", ""));
            // 0 1 exist
            ASSERT_TRUE(psm.Transfer(QUERY, "", "index1:seg2v2", "docid=2,pk=0;docid=3,pk=1;"));

            rtDocs = "cmd=update_field,pk=0,string1=seg3,string2=hello,ts=6;"
                     "cmd=update_field,pk=1,string1=seg3,string2=hello,ts=7";
            ASSERT_TRUE(psm.Transfer(BUILD_RT_SEGMENT, rtDocs, "", ""));
            // 0 1 exist
            ASSERT_TRUE(psm.Transfer(QUERY, "", "index1:seg3", "docid=4,pk=0;docid=5,pk=1;"));

            IndexPartitionPtr indexPart = psm.GetIndexPartition();
            DirectoryPtr rootDir = indexPart->GetRootDirectory();
            IFileSystemPtr fileSystem = rootDir->GetFileSystem();
            fileSystem->Sync(true).GetOrThrow();
        }
        PartitionStateMachine psm1;
        ASSERT_TRUE(psm1.Init(schema, options, rootPath));
        ASSERT_TRUE(psm1.Transfer(QUERY, "", "index1:seg3", "docid=4,pk=0;docid=5,pk=1;"));
        ASSERT_FALSE(psm1.Transfer(QUERY, "", "index1:seg2v2", "docid=2,pk=0;docid=3,pk=1;"));
    }
}

void RecoverRtIndexTest::TestAddUpdateDelete()
{
    {
        // add & update & delete : A0 U0 A1 A2 | D0 U0 U1| U2 D2
        // expect 1 exist
    }
}

void RecoverRtIndexTest::TestKV()
{
    string field = "key:string;value:int8";
    auto schema = SchemaMaker::MakeKVSchema(field, "key", "value");
    IndexPartitionOptions options = util::BuildTestUtil::GetIndexPartitionOptionsForBuildMode(/*buildMode=*/GetParam());
    options.GetBuildConfig(true).buildTotalMemory = 22;
    options.GetBuildConfig(false).buildTotalMemory = 22;
    options.GetBuildConfig(true).maxDocCount = 10;
    options.GetBuildConfig(false).maxDocCount = 10;
    options.GetOnlineConfig().onDiskFlushRealtimeIndex = true;
    options.GetOnlineConfig().loadRemainFlushRealtimeIndex = true;
    string rootPath = GET_TEMP_DATA_PATH();
    {
        PartitionStateMachine psm;
        ASSERT_TRUE(psm.Init(schema, options, rootPath));
        ASSERT_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, "", "", ""));

        string rtDocs = "cmd=delete,key=1,ts=1;"
                        "cmd=delete,key=2,ts=2;"
                        "cmd=delete,key=3,ts=3;"
                        "cmd=add,key=2,value=2,ts=4;"
                        "cmd=add,key=3,value=3,ts=5;"
                        "cmd=delete,key=3,ts=6;";
        ASSERT_TRUE(psm.Transfer(BUILD_RT_SEGMENT, rtDocs, "", ""));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "1", ""));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "2", "value=2"));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "3", ""));

        rtDocs = "cmd=delete,key=4,ts=7;"
                 "cmd=delete,key=5,ts=8;"
                 "cmd=delete,key=6,ts=9;"
                 "cmd=add,key=5,value=5,ts=10;"
                 "cmd=add,key=6,value=6,ts=11;"
                 "cmd=delete,key=6,ts=12;"
                 "cmd=add,key=3,value=33,ts=13;"
                 "cmd=delete,key=2,value=33,ts=14;";

        ASSERT_TRUE(psm.Transfer(BUILD_RT_SEGMENT, rtDocs, "", ""));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "4", ""));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "5", "value=5"));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "6", ""));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "2", ""));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "3", "value=33"));

        IndexPartitionPtr indexPart = psm.GetIndexPartition();
        DirectoryPtr rootDir = indexPart->GetRootDirectory();
        IFileSystemPtr fileSystem = rootDir->GetFileSystem();
        fileSystem->Sync(true).GetOrThrow();
    }
    PartitionStateMachine psm1;
    ASSERT_TRUE(psm1.Init(schema, options, rootPath));
    ASSERT_TRUE(psm1.Transfer(QUERY, "", "4", ""));
    ASSERT_TRUE(psm1.Transfer(QUERY, "", "5", "value=5"));
    ASSERT_TRUE(psm1.Transfer(QUERY, "", "6", ""));
    ASSERT_TRUE(psm1.Transfer(QUERY, "", "2", ""));
    ASSERT_TRUE(psm1.Transfer(QUERY, "", "3", "value=33"));
}

void RecoverRtIndexTest::TestKKV()
{
    string field = "pkey:string;skey:string;value:uint32";
    auto schema = SchemaMaker::MakeKKVSchema(field, "pkey", "skey", "value;skey;");
    IndexPartitionOptions options = util::BuildTestUtil::GetIndexPartitionOptionsForBuildMode(/*buildMode=*/GetParam());
    options.GetBuildConfig(true).buildTotalMemory = 22;
    options.GetBuildConfig(false).buildTotalMemory = 22;
    options.GetBuildConfig(true).maxDocCount = 10;
    options.GetBuildConfig(false).maxDocCount = 10;
    options.GetOnlineConfig().onDiskFlushRealtimeIndex = true;
    options.GetOnlineConfig().loadRemainFlushRealtimeIndex = true;
    string rootPath = GET_TEMP_DATA_PATH();
    {
        PartitionStateMachine psm;
        ASSERT_TRUE(psm.Init(schema, options, rootPath));
        ASSERT_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, "", "", ""));

        string docString = "cmd=add,pkey=pkey1,skey=1,value=1,ts=0;"
                           "cmd=add,pkey=pkey1,skey=2,value=2,ts=1;"
                           "cmd=add,pkey=pkey2,skey=3,value=3,ts=2;"
                           "cmd=add,pkey=pkey2,skey=4,value=4,ts=3;"
                           "cmd=delete,pkey=pkey2,skey=4,value=2,ts=4;"
                           "cmd=delete,pkey=pkey2,skey=5,value=2,ts=5;"
                           "cmd=delete,pkey=pkey1,skey=2,ts=6;"
                           "cmd=delete,pkey=pkey3,ts=7;"
                           "cmd=delete,pkey=pkey3,skey=5,ts=8;";

        ASSERT_TRUE(psm.Transfer(BUILD_RT_SEGMENT, docString, "", ""));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "pkey1", "skey=1,value=1"));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "pkey2", "skey=3,value=3"));

        docString = "cmd=delete,pkey=pkey1,ts=9;"
                    "cmd=add,pkey=pkey3,skey=5,value=5,ts=10;"
                    "cmd=add,pkey=pkey3,skey=6,value=6,ts=11;"
                    "cmd=add,pkey=pkey4,skey=7,value=7,ts=12;"
                    "cmd=add,pkey=pkey4,skey=8,value=8,ts=13;"
                    "cmd=delete,pkey=pkey4,ts=14;"
                    "cmd=delete,pkey=pkey3,skey=6,ts=15;"
                    "cmd=delete,pkey=pkey4,skey=7,ts=16;"
                    "cmd=add,pkey=pkey4,skey=9,value=9,ts=17;"
                    "cmd=delete,pkey=pkey5,skey=9,value=9,ts=18";
        ASSERT_TRUE(psm.Transfer(BUILD_RT_SEGMENT, docString, "", ""));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "pkey1", ""));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "pkey3", "skey=5,value=5"));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "pkey4", "skey=9,value=9"));

        IndexPartitionPtr indexPart = psm.GetIndexPartition();
        DirectoryPtr rootDir = indexPart->GetRootDirectory();
        IFileSystemPtr fileSystem = rootDir->GetFileSystem();
        fileSystem->Sync(true).GetOrThrow();
    }
    PartitionStateMachine psm1;
    ASSERT_TRUE(psm1.Init(schema, options, rootPath));
    ASSERT_TRUE(psm1.Transfer(QUERY, "", "pkey1", ""));
    ASSERT_TRUE(psm1.Transfer(QUERY, "", "pkey3", "skey=5,value=5"));
    ASSERT_TRUE(psm1.Transfer(QUERY, "", "pkey4", "skey=9,value=9"));
}

void RecoverRtIndexTest::TestNoEnoughMemory()
{
    {
        // test recover rt index without enough memory (quota)
        // quoat < 1024 (1GB)
        auto schema = MakeSchema(false);
        IndexPartitionOptions options =
            util::BuildTestUtil::GetIndexPartitionOptionsForBuildMode(/*buildMode=*/GetParam());
        options.GetOnlineConfig().buildConfig.speedUpPrimaryKeyReader = true;
        options.GetOnlineConfig().buildConfig.speedUpPrimaryKeyReaderParam = "combine_segments=true,max_doc_count=10";
        options.GetOnlineConfig().onDiskFlushRealtimeIndex = true;
        options.GetOnlineConfig().loadRemainFlushRealtimeIndex = true;
        options.GetOnlineConfig().maxRealtimeMemSize = 521;
        options.GetOnlineConfig().buildConfig.buildTotalMemory = 1024 * 1024;
        PartitionStateMachine psm;
        string rootPath = GET_TEMP_DATA_PATH();
        ASSERT_TRUE(psm.Init(schema, options, rootPath));
        // ASSERT_ANY_THROW(psm.Init(schema, options, rootPath));
        ASSERT_FALSE(psm.Transfer(BUILD_FULL_NO_MERGE, "", "", ""));
    }
}

void RecoverRtIndexTest::TestOneSegment()
{
    auto schema = MakeSchema(false);
    IndexPartitionOptions options = util::BuildTestUtil::GetIndexPartitionOptionsForBuildMode(/*buildMode=*/GetParam());
    options.GetOnlineConfig().buildConfig.speedUpPrimaryKeyReader = true;
    options.GetOnlineConfig().buildConfig.speedUpPrimaryKeyReaderParam = "combine_segments=true,max_doc_count=10";
    options.GetOnlineConfig().onDiskFlushRealtimeIndex = true;
    options.GetOnlineConfig().loadRemainFlushRealtimeIndex = true;
    string rootPath = GET_TEMP_DATA_PATH();
    {
        PartitionStateMachine psm;
        ASSERT_TRUE(psm.Init(schema, options, rootPath));
        ASSERT_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, "", "", ""));

        string rtDocs = "cmd=add,pk=3,string1=hello,long1=1,ts=4;"
                        "cmd=add,pk=3,string1=hello,long1=4,ts=4;"
                        "cmd=add,pk=4,string1=hello,long1=2,ts=5;";
        ASSERT_TRUE(psm.Transfer(BUILD_RT_SEGMENT, rtDocs, "index1:hello", "docid=1;docid=2"));
        IndexPartitionPtr indexPart = psm.GetIndexPartition();
        DirectoryPtr rootDir = indexPart->GetRootDirectory();
        IFileSystemPtr fileSystem = rootDir->GetFileSystem();
        fileSystem->Sync(true).GetOrThrow();
    }
    PartitionStateMachine psm1;
    ASSERT_TRUE(psm1.Init(schema, options, rootPath));
    ASSERT_TRUE(psm1.Transfer(QUERY, "", "index1:hello", "docid=1;docid=2"));
}
}} // namespace indexlib::partition
