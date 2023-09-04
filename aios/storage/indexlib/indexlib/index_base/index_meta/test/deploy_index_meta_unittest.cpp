#include "indexlib/index_base/index_meta/test/deploy_index_meta_unittest.h"

#include "indexlib/file_system/FileInfo.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/file_system/load_config/LoadConfig.h"
#include "indexlib/index_base/deploy_index_wrapper.h"
#include "indexlib/test/partition_state_machine.h"
#include "indexlib/test/schema_maker.h"
#include "indexlib/util/PathUtil.h"

using namespace std;
using namespace autil;
using namespace fslib;
using namespace fslib::fs;

namespace indexlib { namespace index_base {
using namespace indexlib::file_system;
using namespace indexlib::file_system;
using namespace indexlib::test;
using namespace indexlib::config;
using namespace indexlib::util;
IE_LOG_SETUP(index_base, DeployIndexMetaTest);

DeployIndexMetaTest::DeployIndexMetaTest() {}

DeployIndexMetaTest::~DeployIndexMetaTest() {}

void DeployIndexMetaTest::CaseSetUp() { mRootDir = GET_TEMP_DATA_PATH(); }

void DeployIndexMetaTest::CaseTearDown() {}
void DeployIndexMetaTest::StoreDefaultVersionContent(const file_system::DirectoryPtr& dir) const
{
    string content =
        R"(
    {
        "segments": [0],
        "versionid": 0,
        "format_version": 1,
        "level_info":
        {
             "level_num" : 2,
             "topology" : "sharding_mod",
             "level_metas" :
             [
                 {
                     "level_idx" : 0,
                     "cursor" : 0,
                     "topology" : "sequence",
                     "segments" :
                     [ 0 ]
                 }
            ]
        }
    })";
    dir->Store("version.0", content, WriterOption::AtomicDump());
    // FslibWrapper::AtomicStoreE(PathUtil::JoinPath(versionDir, "version.0"), content);
    return;
}

void DeployIndexMetaTest::TestDeployFileMeta()
{
    {
        FileInfo meta("file", 0, 0);
        ASSERT_EQ("file", meta.filePath);
        ASSERT_EQ(0, meta.fileLength);
        ASSERT_EQ(0, meta.modifyTime);
        ASSERT_TRUE(meta.isFile());
        ASSERT_FALSE(meta.isDirectory());
        ASSERT_TRUE(meta.isValid());
    }
    {
        FileInfo meta("dir/");
        ASSERT_EQ("dir/", meta.filePath);
        ASSERT_EQ(-1, meta.fileLength);
        ASSERT_EQ((uint64_t)-1, meta.modifyTime);
        ASSERT_FALSE(meta.isFile());
        ASSERT_TRUE(meta.isDirectory());
        ASSERT_TRUE(meta.isValid());
    }
    {
        FileInfo meta("file");
        ASSERT_EQ("file", meta.filePath);
        ASSERT_EQ(-1, meta.fileLength);
        ASSERT_EQ((uint64_t)-1, meta.modifyTime);
        meta.modifyTime = (uint64_t)100;
        ASSERT_TRUE(meta.isFile());
        ASSERT_FALSE(meta.isDirectory());
        ASSERT_FALSE(meta.isValid());
    }
    {
        FileInfo meta("file");
        ASSERT_EQ("file", meta.filePath);
        meta.fileLength = 1;
        ASSERT_EQ((uint64_t)-1, meta.modifyTime);
        ASSERT_TRUE(meta.isFile());
        ASSERT_FALSE(meta.isDirectory());
        ASSERT_TRUE(meta.isValid());
    }
}
void DeployIndexMetaTest::CreateFiles(const DirectoryPtr& dir, const string& filePaths)
{
    vector<string> fileNames;
    fileNames = StringUtil::split(filePaths, ";");
    for (size_t i = 0; i < fileNames.size(); ++i) {
        dir->Store(fileNames[i], "");
    }
}

void DeployIndexMetaTest::CreateFiles(const string& dir, const string& filePaths)
{
    vector<string> fileNames;
    fileNames = StringUtil::split(filePaths, ";");
    for (size_t i = 0; i < fileNames.size(); ++i) {
        string filePath = util::PathUtil::JoinPath(dir, fileNames[i]);
        FslibWrapper::AtomicStoreE(filePath, "");
    }
}

void DeployIndexMetaTest::TestDeployIndexMeta()
{
    const auto segDir = GET_PARTITION_DIRECTORY()->MakeDirectory("segment_0_level_0");
    CreateFiles(segDir, "file1;file2;file3");

    StoreDefaultVersionContent(GET_PARTITION_DIRECTORY());

    DeployIndexWrapper::GetDeployIndexMetaInputParams inputParams;
    inputParams.localPath = "";
    inputParams.rawPath = mRootDir;
    inputParams.baseOnlineConfig = nullptr;
    inputParams.targetOnlineConfig = nullptr;
    inputParams.baseVersionId = -1;
    inputParams.targetVersionId = 0;
    DeployIndexMetaVec localMetas;
    DeployIndexMetaVec remoteMetas;
    DeployIndexWrapper::GetDeployIndexMeta(inputParams, localMetas, remoteMetas);

    FileList tmpList;
    DeployIndexWrapper::DumpSegmentDeployIndex(segDir, "");

    ASSERT_TRUE(DeployIndexWrapper::GetDeployIndexMeta(inputParams, localMetas, remoteMetas));
    ASSERT_EQ(0, remoteMetas.size());
    ASSERT_EQ(1, localMetas.size());
    for (auto& fileInfo : localMetas[0]->deployFileMetas) {
        tmpList.push_back(fileInfo.filePath);
    }
    EXPECT_THAT(tmpList,
                UnorderedElementsAre(
                    // INDEX_FORMAT_VERSION_FILE_NAME,
                    // index::SCHEMA_FILE_NAME,
                    "segment_0_level_0/", "segment_0_level_0/deploy_index", "segment_0_level_0/segment_file_list",
                    "segment_0_level_0/file1", "segment_0_level_0/file2", "segment_0_level_0/file3"));

    Version v;
    string versionPath = util::PathUtil::JoinPath(mRootDir, "version.0");
    v.Load(versionPath);
    DeployIndexWrapper::DumpDeployMeta(GET_PARTITION_DIRECTORY(), v);
    localMetas.clear();
    ASSERT_TRUE(DeployIndexWrapper::GetDeployIndexMeta(inputParams, localMetas, remoteMetas));
    // ASSERT_TRUE(localMetas[0].isComplete);
    for (auto meta : localMetas[0]->deployFileMetas) {
        if (meta.filePath == "deploy_meta.0") {
            ASSERT_GT(meta.fileLength, 0);
        }
        // ASSERT_NE(meta.modifyTime, (uint64_t)-1) << meta.filePath;
    }

    // test no filter
    localMetas.clear();
    ASSERT_TRUE(DeployIndexWrapper::GetDeployIndexMeta(inputParams, localMetas, remoteMetas));
    ASSERT_EQ(7, localMetas[0]->deployFileMetas.size());
    tmpList.clear();
    for (const auto& fileInfo : localMetas[0]->deployFileMetas) {
        tmpList.push_back(fileInfo.filePath);
    }
    EXPECT_THAT(tmpList,
                UnorderedElementsAre("deploy_meta.0", string("segment_0_level_0/") + SEGMENT_FILE_LIST,
                                     string("segment_0_level_0/") + DEPLOY_INDEX_FILE_NAME, "segment_0_level_0/",
                                     "segment_0_level_0/file1", "segment_0_level_0/file2", "segment_0_level_0/file3"));

    // test filter
    config::OnlineConfig onlineConfig;
    onlineConfig.needReadRemoteIndex = true;
    onlineConfig.loadConfigList.SetLoadMode(file_system::LoadConfig::LoadMode::REMOTE_ONLY);
    inputParams.baseOnlineConfig = &onlineConfig;
    inputParams.targetOnlineConfig = &onlineConfig;
    localMetas.clear();
    ASSERT_TRUE(DeployIndexWrapper::GetDeployIndexMeta(inputParams, localMetas, remoteMetas));
    ASSERT_EQ(0, localMetas.size());
}

void DeployIndexMetaTest::TestLegacyDeployIndexMetaFile()
{
    string field = "pkstr:string;text1:text;long1:uint32;multi_long:uint32:true;"
                   "updatable_multi_long:uint32:true:true;";
    string index = "pk:primarykey64:pkstr;pack1:pack:text1;";
    string attr = "long1;multi_long;updatable_multi_long;";
    string summary = "pkstr;";

    auto schema = SchemaMaker::MakeSchema(field, index, attr, summary);

    IndexPartitionSchemaPtr subSchema = SchemaMaker::MakeSchema("substr1:string;subpkstr:string;sub_long:uint32;",
                                                                "subindex1:string:substr1;sub_pk:primarykey64:subpkstr",
                                                                "substr1;subpkstr;sub_long;", "");

    config::IndexPartitionOptions options;
    schema->SetSubIndexPartitionSchema(subSchema);
    mRootDir = GET_TEMP_DATA_PATH();
    auto originIndexPath = util::PathUtil::JoinPath(GET_PRIVATE_TEST_DATA_PATH(), "legacy_deploy_index");
    auto indexPath = util::PathUtil::JoinPath(mRootDir, "legacy_deploy_index");
    ASSERT_EQ(FSEC_OK, FslibWrapper::Copy(originIndexPath, indexPath).Code());
    options.SetEnablePackageFile(true);
    // string docString = "cmd=add,pkstr=hello,ts=0,subpkstr=sub1^sub2,substr1=s1^s2";
    IndexFileList meta;
    ASSERT_TRUE(meta.Load(util::PathUtil::JoinPath(indexPath, "segment_0_level_0/deploy_index")).GetOrThrow());
    string docString = "";
    PartitionStateMachine psm;
    ASSERT_TRUE(psm.Init(schema, options, indexPath));
    ASSERT_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, docString, "pk:hello", "docid=0,pkstr=hello,main_join=2;"));
    ASSERT_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, "", "subindex1:s2", "docid=1,subpkstr=sub2,sub_join=0;"));
    // string docString2 = "cmd=add,pkstr=hello2,ts=1,subpkstr=sub3^sub4,substr1=s3^s4";
    string docString2 = "";
    ASSERT_TRUE(psm.Transfer(QUERY, docString2, "pk:hello2", "docid=1,pkstr=hello2,main_join=4;"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "subindex1:s4", "docid=3,subpkstr=sub4,sub_join=1;"));
    string docString3 = "cmd=add,pkstr=hello3,ts=2,subpkstr=sub5^sub6,substr1=s4^s5";
    ASSERT_TRUE(psm.Transfer(BUILD_INC, docString3, "subindex1:s4",
                             "docid=3,subpkstr=sub4,sub_join=1;"
                             "docid=4,subpkstr=sub5,sub_join=2;"));
    ASSERT_TRUE(
        FslibWrapper::IsExist(util::PathUtil::JoinPath(indexPath, "segment_2_level_0/segment_file_list")).GetOrThrow());
    ASSERT_TRUE(
        FslibWrapper::IsExist(util::PathUtil::JoinPath(indexPath, "segment_2_level_0/deploy_index")).GetOrThrow());
}
}} // namespace indexlib::index_base
