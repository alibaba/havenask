#include <sys/types.h>
#include <utime.h>

#include "autil/NetUtil.h"
#include "autil/StringUtil.h"
#include "indexlib/common_define.h"
#include "indexlib/config/load_config_list.h"
#include "indexlib/config/online_config.h"
#include "indexlib/file_system/EntryTable.h"
#include "indexlib/file_system/FileInfo.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/file_system/test/LoadConfigListCreator.h"
#include "indexlib/file_system/test/PackageFileUtil.h"
#include "indexlib/index_base/deploy_index_wrapper.h"
#include "indexlib/index_base/index_meta/segment_file_list_wrapper.h"
#include "indexlib/index_base/index_meta/segment_info.h"
#include "indexlib/index_base/index_meta/version.h"
#include "indexlib/index_base/index_meta/version_loader.h"
#include "indexlib/index_define.h"
#include "indexlib/test/partition_data_maker.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/util/PathUtil.h"

using namespace std;
using namespace autil;
using namespace autil::legacy;
using namespace fslib;
using namespace fslib::fs;
using testing::UnorderedElementsAreArray;

using namespace indexlib::test;
using namespace indexlib::config;
using namespace indexlib::util;
using namespace indexlib::file_system;

namespace indexlib { namespace index_base {
class DeployIndexWrapperTest : public INDEXLIB_TESTBASE
{
public:
    DECLARE_CLASS_NAME(DeployIndexWrapperTest);

public:
    void CaseSetUp() override { mRootDir = GET_TEMP_DATA_PATH(); }

    void CaseTearDown() override {}

    void StoreDefaultVersionContent(const std::string& versionDir, const std::string& segIds = string("0"),
                                    size_t versionId = 0, const std::string& temperatures = "") const
    {
        string content =
            R"(
    {
        "segments": [ $segIds ],
        "versionid": $vid,
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
                     [ $segIds ]
                 }
            ]
        }
    })";
        static const std::string segIdsKey("$segIds");
        static const std::string vidKey("$vid");

        // replace segIds
        auto pos = content.find(segIdsKey);
        assert(pos != string::npos);
        content.replace(pos, segIdsKey.size(), segIds);

        pos = content.find(segIdsKey, pos);
        assert(pos != string::npos);
        content.replace(pos, segIdsKey.size(), segIds);

        // replace version id
        pos = content.find(vidKey);
        assert(pos != string::npos);
        content.replace(pos, vidKey.size(), std::to_string(versionId));
        if (!temperatures.empty()) {
            string temperatureContent = R"(
               ,"segment_temperature_metas":
               [)";
            vector<string> temperatureValues = StringUtil::split(temperatures, ",");
            vector<string> segments = StringUtil::split(segIds, ",");
            assert(segments.size() == temperatureValues.size());
            for (size_t i = 0; i < temperatureValues.size(); i++) {
                string segmentTemperaute;
                if (i != 0) {
                    segmentTemperaute = ",{";
                } else {
                    segmentTemperaute = "{";
                }
                segmentTemperaute += "\"segment_id\":" + segments[i] + ",";
                segmentTemperaute += "\"segment_temperature\":\"" + temperatureValues[i] + "\",";
                segmentTemperaute += "\"segment_temperature_detail\":" + string("\"HOT:1;WARM:1;COLD:1\"}");
                temperatureContent += segmentTemperaute;
            }
            temperatureContent += "]}";
            StringUtil::replaceLast(content, "}", "");
            content += temperatureContent;
        }

        ASSERT_EQ(FSEC_OK, FslibWrapper::AtomicStore(
                               PathUtil::JoinPath(versionDir, string("version.") + to_string(versionId)), content)
                               .Code());
        return;
    }
    // filePaths : file1;file2;file3
    void CreateFiles(const DirectoryPtr& dir, const string& filePaths)
    {
        vector<string> fileNames;
        fileNames = StringUtil::split(filePaths, ";");
        for (size_t i = 0; i < fileNames.size(); ++i) {
            dir->Store(fileNames[i], "");
        }
    }
    void CreateFiles(const string& dir, const string& filePaths)
    {
        vector<string> fileNames;
        fileNames = StringUtil::split(filePaths, ";");
        for (size_t i = 0; i < fileNames.size(); ++i) {
            string filePath = util::PathUtil::JoinPath(dir, fileNames[i]);
            ASSERT_EQ(FSEC_OK, FslibWrapper::AtomicStore(filePath, "").Code());
        }
    }

    bool GetDeployFiles(const std::string& remotePath, const std::string& localPath,
                        const config::OnlineConfig& onlineConfig, fslib::FileList& localFileList,
                        fslib::FileList& remoteFileList, const versionid_t targetVersionId,
                        const versionid_t baseVersionId = INVALID_VERSIONID)
    {
        SCOPED_TRACE("GetDeployFiles");
        DeployIndexWrapper::GetDeployIndexMetaInputParams params;
        params.rawPath = remotePath;
        params.localPath = localPath;
        params.baseOnlineConfig = &mOnlineConfig;
        params.targetOnlineConfig = &mOnlineConfig;
        params.targetVersionId = targetVersionId;
        params.baseVersionId = baseVersionId;
        DeployIndexMetaVec localDeployIndexMetaVec;
        DeployIndexMetaVec remoteDeployIndexMetaVec;
        bool ret = DeployIndexWrapper::GetDeployIndexMeta(params, localDeployIndexMetaVec, remoteDeployIndexMetaVec);
        if (!ret) {
            return false;
        }
        if (remoteDeployIndexMetaVec.size() == 1) {
            for (const auto& fileInfo : remoteDeployIndexMetaVec[0]->deployFileMetas) {
                remoteFileList.push_back(fileInfo.filePath);
            }
        } else {
            EXPECT_EQ(0, remoteDeployIndexMetaVec.size());
        }
        EXPECT_EQ(1, localDeployIndexMetaVec.size());
        for (const auto& fileInfo : localDeployIndexMetaVec[0]->deployFileMetas) {
            localFileList.push_back(fileInfo.filePath);
        }
        return ret;
    }

    bool GetDeployIndexMeta(DeployIndexMeta& remoteDeployIndexMeta, DeployIndexMeta& localDeployIndexMeta,
                            const std::string& sourcePath, const std::string& localPath, versionid_t targetVersionId,
                            versionid_t baseVersionId, const config::OnlineConfig& targetOnlineConfig,
                            const config::OnlineConfig& baseOnlineConfig)
    {
        SCOPED_TRACE("GetDeployIndexMeta");
        DeployIndexWrapper::GetDeployIndexMetaInputParams params;
        params.rawPath = sourcePath;
        params.localPath = localPath;
        params.baseOnlineConfig = &baseOnlineConfig;
        params.targetOnlineConfig = &targetOnlineConfig;
        params.targetVersionId = targetVersionId;
        params.baseVersionId = baseVersionId;
        DeployIndexMetaVec localDeployIndexMetaVec;
        DeployIndexMetaVec remoteDeployIndexMetaVec;
        bool ret = DeployIndexWrapper::GetDeployIndexMeta(params, localDeployIndexMetaVec, remoteDeployIndexMetaVec);
        if (remoteDeployIndexMetaVec.size() == 1) {
            remoteDeployIndexMeta = *remoteDeployIndexMetaVec[0];
        }
        if (localDeployIndexMetaVec.size() == 1) {
            localDeployIndexMeta = *localDeployIndexMetaVec[0];
        }
        return ret;
    }

    void CheckDeployMeta(const DeployIndexMeta& deployMeta, vector<string> expectFileNames,
                         vector<string> expectFinalFileNames, const char* file = "", int32_t line = -1)
    {
        string msg = string(file) + ":" + StringUtil::toString(line);
        // ASSERT_TRUE(deployMeta.isComplete);
        vector<string> actualFileNames, actualFinalFileNames;
        for (const FileInfo& fileInfo : deployMeta.deployFileMetas) {
            ASSERT_TRUE(fileInfo.isValid()) << msg << "\n";
            actualFileNames.push_back(fileInfo.filePath);
        }
        EXPECT_THAT(actualFileNames, UnorderedElementsAreArray(expectFileNames)) << msg << ": deployFileMetas\n";
        for (const FileInfo& fileInfo : deployMeta.finalDeployFileMetas) {
            ASSERT_TRUE(fileInfo.isValid()) << msg;
            actualFinalFileNames.push_back(fileInfo.filePath);
        }
        EXPECT_THAT(actualFinalFileNames, UnorderedElementsAreArray(expectFinalFileNames))
            << msg << ": finalDeployFileMetas\n";
    }

public:
    void TestDumpDeployIndex()
    {
        string testPath = util::PathUtil::JoinPath(mRootDir, "DumpDeployIndex");
        auto testDir = GET_PARTITION_DIRECTORY()->MakeDirectory("DumpDeployIndex");
        ASSERT_NE(testDir, nullptr);
        {
            // normal case
            CreateFiles(testDir, "file1;file2;file3");

            SegmentFileListWrapper::Dump(testDir, "");

            string deployIndexFile = util::PathUtil::JoinPath(testPath, SEGMENT_FILE_LIST);
            ASSERT_TRUE(FslibWrapper::IsExist(deployIndexFile).GetOrThrow());

            // assure list from SEGMENT_FILE_LIST
            FslibWrapper::DeleteFileE(PathUtil::JoinPath(testPath, "file1"), DeleteOption::NoFence(false));
        }
        {
            // path not exist
            auto tmpDir = testDir->GetDirectory("pathNotExist", false);
            ASSERT_EQ(tmpDir, nullptr);
            ASSERT_TRUE(!SegmentFileListWrapper::Dump(tmpDir, ""));
        }
        {
            // deploy index already exist
            // CreateFiles(testDir, SEGMENT_FILE_LIST);
            ASSERT_TRUE(SegmentFileListWrapper::Dump(testDir, ""));
        }
    }

    void TestDumpTruncateMetaIndex()
    {
        string testPath = util::PathUtil::JoinPath(mRootDir, TRUNCATE_META_DIR_NAME);
        GET_PARTITION_DIRECTORY()->MakeDirectory("segment_0_level_0", DirectoryOption::Package());
        auto testDir = GET_PARTITION_DIRECTORY()->MakeDirectory(TRUNCATE_META_DIR_NAME);

        CreateFiles(testDir, "file1;file2;file3");
        DeployIndexWrapper::DumpTruncateMetaIndex(GET_PARTITION_DIRECTORY());
        string deployIndexFile = util::PathUtil::JoinPath(testPath, SEGMENT_FILE_LIST);
        ASSERT_TRUE(FslibWrapper::IsExist(deployIndexFile).GetOrThrow());

        // truncate dir can't be parsed
        StoreDefaultVersionContent(mRootDir);
        FileList localFileList, remoteFileList;
        GetDeployFiles(mRootDir, "", mOnlineConfig, localFileList, remoteFileList, -1 /* lastest version */);
        ASSERT_THAT(localFileList,
                    UnorderedElementsAre("segment_0_level_0/", "truncate_meta/", "truncate_meta/file1",
                                         "truncate_meta/file2", "truncate_meta/file3", "truncate_meta/deploy_index",
                                         "truncate_meta/segment_file_list"));
        FslibWrapper::DeleteDirE(testPath, DeleteOption::NoFence(false));
    }

    void TestDumpSegmentDeployIndexWithDirectoryAndPackageFile()
    {
        SCOPED_TRACE("TestDumpSegmentDeployIndexWithDirectoryAndPackageFile");
        file_system::FileSystemOptions fsOptions;
        fsOptions.outputStorage = file_system::FSST_PACKAGE_MEM;
        RESET_FILE_SYSTEM("ut", true /* auto mount */, fsOptions);
        file_system::DirectoryPtr directory = GET_PARTITION_DIRECTORY();
        file_system::DirectoryPtr segDir = directory->MakeDirectory("segment_0_level_0", DirectoryOption::Package());
        file_system::IFileSystemPtr fileSystem = GET_FILE_SYSTEM();
        PackageFileUtil::CreatePackageFile(segDir, "file5:abc#index/file6:abc#attribute/file7:abc", "");
        segDir->FlushPackage();
        segDir->Sync(true /*wait finish*/);
        SegmentInfoPtr segInfo(new SegmentInfo);
        segInfo->docCount = 100;
        DeployIndexWrapper::DumpSegmentDeployIndex(segDir, segInfo);
        segInfo->Store(segDir, true);
        segDir->Sync(true /*wait finish*/);
        ASSERT_TRUE(segDir->IsExist(SEGMENT_FILE_LIST));

        StoreDefaultVersionContent(mRootDir);
        FileList localFileList;
        FileList remoteFileList;
        GetDeployFiles(mRootDir, "", mOnlineConfig, localFileList, remoteFileList, -1 /* lastest version */);

        ASSERT_THAT(localFileList,
                    UnorderedElementsAre(string("segment_0_level_0/"), string("segment_0_level_0/deploy_index"),
                                         string("segment_0_level_0/package_file.__data__0"),
                                         string("segment_0_level_0/package_file.__meta__"),
                                         string("segment_0_level_0/segment_info"),
                                         string("segment_0_level_0/segment_file_list")));

        ASSERT_EQ(8195,
                  FslibWrapper::GetFileLength(mRootDir + "segment_0_level_0/package_file.__data__0").GetOrThrow());
        std::string ip = autil::NetUtil::getBindIp(); // ip in version file
        ASSERT_EQ(304 + ip.size(), segInfo->ToString().size()) << segInfo->ToString();
        ASSERT_EQ(895, FslibWrapper::GetFileLength(mRootDir + "segment_0_level_0/package_file.__meta__").GetOrThrow());

        int64_t totalLength = DeployIndexWrapper::GetSegmentSize(segDir, true /* include patch */);
        // total length should include package_file.__data__0 & package_file.__meta__ & segment_info
        ASSERT_EQ(8195 + 895 + segInfo->ToString().size(), totalLength);
    }

    void TestDumpSegmentDeployIndex()
    {
        SCOPED_TRACE("TestDumpSegmentDeployIndex");
        {
            string testPath = util::PathUtil::JoinPath(mRootDir, "segment_0_level_0");
            DirectoryPtr testDir = GET_PARTITION_DIRECTORY()->MakeDirectory("segment_0_level_0");
            CreateFiles(testDir, "file1;file2;file3");
            auto segDir = GET_PARTITION_DIRECTORY()->GetDirectory("segment_0_level_0", false);
            ASSERT_NE(segDir, nullptr);
            DeployIndexWrapper::DumpSegmentDeployIndex(segDir, "");
            string deployIndexFile = util::PathUtil::JoinPath(testPath, SEGMENT_FILE_LIST);
            ASSERT_TRUE(FslibWrapper::IsExist(deployIndexFile).GetOrThrow());
            StoreDefaultVersionContent(mRootDir);
            FileList tmpList;
            FileList remoteFileList;
            GetDeployFiles(mRootDir, "", mOnlineConfig, tmpList, remoteFileList, -1 /* lastest version */);
            ASSERT_THAT(tmpList,
                        UnorderedElementsAre("segment_0_level_0/", "segment_0_level_0/deploy_index",
                                             "segment_0_level_0/file1", "segment_0_level_0/file2",
                                             "segment_0_level_0/file3", "segment_0_level_0/segment_file_list"));
            FslibWrapper::DeleteDirE(testPath, DeleteOption::NoFence(false));
        }
    }

    void TestGetFilesAbnormal()
    {
        SCOPED_TRACE("TestGetFilesAbnormal");
        // path not exist
        {
            string testPath = util::PathUtil::JoinPath(mRootDir, "pathNotExist");
            FileList fileList;
            FileList remoteFileList;
            ASSERT_TRUE(
                !GetDeployFiles(mRootDir, "", mOnlineConfig, fileList, remoteFileList, -1 /* lastest version */));
        }
        // index file not exist
        {
            string testPath = util::PathUtil::JoinPath(mRootDir, "tetGetFiles");
            CreateFiles(testPath, "file1;file2;file3");
            FileList tmpList;
            FileList remoteFileList;
            ASSERT_FALSE(
                GetDeployFiles(testPath, "", mOnlineConfig, tmpList, remoteFileList, -1 /* lastest version */));
        }
    }

    void TestGetDeployFiles()
    {
        // InnerTestGetDeployFiles(false);
        InnerTestGetDeployFiles(true);
    }

    void InnerTestGetDeployFiles(bool exception)
    {
        SCOPED_TRACE("InnerTestGetDeployFiles");
        {
            tearDown();
            setUp();

            // lastVersion is invalid
            const auto segDir = GET_PARTITION_DIRECTORY()->MakeDirectory("segment_0_level_0");
            ASSERT_NE(segDir, nullptr);
            CreateFiles(segDir, "file1;file2;file3");

            string versionPath = util::PathUtil::JoinPath(mRootDir, "version.0");
            StoreDefaultVersionContent(mRootDir, "0", 0);

            FileList tmpList;
            FileList remoteFileList;
            DeployIndexWrapper::DumpSegmentDeployIndex(segDir, "");
            bool result = GetDeployFiles(mRootDir, "", mOnlineConfig, tmpList, remoteFileList, 0);

            ASSERT_TRUE(result);
            EXPECT_THAT(tmpList, UnorderedElementsAre(string("segment_0_level_0/") + SEGMENT_FILE_LIST,
                                                      string("segment_0_level_0/") + DEPLOY_INDEX_FILE_NAME,
                                                      "segment_0_level_0/", "segment_0_level_0/file1",
                                                      "segment_0_level_0/file2", "segment_0_level_0/file3"));
        }

        {
            tearDown();
            setUp();
            // lastVersion is valid
            const auto segDir = GET_PARTITION_DIRECTORY()->MakeDirectory("segment_0_level_0");
            ASSERT_NE(segDir, nullptr);

            string testPath = util::PathUtil::JoinPath(mRootDir, "segment_0_level_0");
            CreateFiles(segDir, "file1;file2;file3");
            string testPath2 = util::PathUtil::JoinPath(mRootDir, "segment_0");
            CreateFiles(testPath2, "file1;file2;file3");

            StoreDefaultVersionContent(mRootDir, "1, 2", 0);
            StoreDefaultVersionContent(mRootDir, "0, 1, 2", 1);

            FileList tmpList;
            FileList remoteFileList;
            DeployIndexWrapper::DumpSegmentDeployIndex(segDir, "");
            // segment 1,2 not exist
            ASSERT_FALSE(GetDeployFiles(mRootDir, "", mOnlineConfig, tmpList, remoteFileList, 1, 0));
        }
    }

    void TestGetDeployFilesByConfig()
    {
        SCOPED_TRACE("TestGetDeployFilesByConfig");
        string loadConfigStr = R"(
        {
            "load_config" :
            [
                {
                    "file_patterns": [ "^.*file1" ],
                    "load_strategy": "cache",
                    "remote" : true
                },
                {
                    "file_patterns": [ "^.*file3" ],
                    "load_strategy": "cache",
                    "remote" : true,
                    "deploy" : false
                },
                {
                    "file_patterns": [ "^.*summary/.*" ],
                    "load_strategy": "cache",
                    "remote" : true,
                    "deploy" : false
                },
                {
                    "file_patterns": [ "^.*dir4/file4.*" ],
                    "load_strategy": "cache",
                    "remote" : true,
                    "deploy" : true
                },
                {
                    "file_patterns": [ "^.*dir4/" ],
                    "load_strategy": "cache",
                    "remote" : true,
                    "deploy" : false
                },
                {
                    "file_patterns": [ "^.*attribute/" ],
                    "load_strategy": "cache",
                    "remote" : true,
                    "deploy" : false
                }
            ]
        })";
        mOnlineConfig.needReadRemoteIndex = true;
        mOnlineConfig.loadConfigList = LoadConfigListCreator::CreateLoadConfigListFromJsonString(loadConfigStr);

        // lastVersion is invalid
        const auto segDir = GET_PARTITION_DIRECTORY()->MakeDirectory("segment_0_level_0");
        const auto indexDir = segDir->MakeDirectory("dir4");
        CreateFiles(segDir, "file1;file2;file3;dir4/file4");

        segDir->MakeDirectory("summary");
        segDir->MakeDirectory("deletionmap");
        segDir->MakeDirectory("attribute");

        DeployIndexWrapper::DumpSegmentDeployIndex(segDir, "");

        ASSERT_EQ(
            FSEC_OK,
            FslibWrapper::AtomicStore(util::PathUtil::JoinPath(mRootDir, INDEX_FORMAT_VERSION_FILE_NAME), "").Code());
        ASSERT_EQ(FSEC_OK, FslibWrapper::AtomicStore(util::PathUtil::JoinPath(mRootDir, SCHEMA_FILE_NAME), "").Code());
        StoreDefaultVersionContent(mRootDir);
        {
            // legacy index, no deploy_meta
            FileList tmpList;
            FileList remoteFileList;
            bool result = GetDeployFiles(mRootDir, "", mOnlineConfig, tmpList, remoteFileList, 0);

            ASSERT_TRUE(result);
            EXPECT_THAT(tmpList,
                        UnorderedElementsAre(INDEX_FORMAT_VERSION_FILE_NAME, SCHEMA_FILE_NAME,
                                             string("segment_0_level_0/") + SEGMENT_FILE_LIST,
                                             string("segment_0_level_0/") + DEPLOY_INDEX_FILE_NAME,
                                             "segment_0_level_0/", "segment_0_level_0/file1", "segment_0_level_0/file2",
                                             "segment_0_level_0/deletionmap/", "segment_0_level_0/dir4/file4"));
        }
        {
            // with deploy_meta
            {
                RESET_FILE_SYSTEM();
                Version v;
                v.Load(GET_PARTITION_DIRECTORY(), "version.0");
                DeployIndexWrapper::DumpDeployMeta(GET_PARTITION_DIRECTORY(), v);
            }

            FileList tmpList;
            FileList remoteFileList;
            bool result = GetDeployFiles(mRootDir, "", mOnlineConfig, tmpList, remoteFileList, 0);

            ASSERT_TRUE(result);
            EXPECT_THAT(tmpList, UnorderedElementsAre(INDEX_FORMAT_VERSION_FILE_NAME, SCHEMA_FILE_NAME,
                                                      // for now both DEPLOY_INDEX_FILE_NAME and SEGMENT_FILE_LIST exist
                                                      string("segment_0_level_0/") + DEPLOY_INDEX_FILE_NAME,
                                                      string("segment_0_level_0/") + SEGMENT_FILE_LIST,
                                                      "segment_0_level_0/", "segment_0_level_0/file1",
                                                      "segment_0_level_0/file2", "segment_0_level_0/deletionmap/",
                                                      "segment_0_level_0/dir4/file4", "deploy_meta.0"));
        }
    }

    void TestSimpleProcess()
    {
        SCOPED_TRACE("TestSimpleProcess");
        // lastVersion is invalid
        const auto newRootDir = GET_PARTITION_DIRECTORY()->MakeDirectory("newPath"); // remote
        const auto oldRootDir = GET_PARTITION_DIRECTORY()->MakeDirectory("oldPath"); // local
        const auto oldSegDir0 = oldRootDir->MakeDirectory("segment_0_level_0");
        const auto newSegDir0 = newRootDir->MakeDirectory("segment_0_level_0");
        const auto newSegDir2 = newRootDir->MakeDirectory("segment_2_level_0");

        CreateFiles(oldSegDir0, "file1;file2;file3");
        CreateFiles(newSegDir0, "file1;file2;file3");
        CreateFiles(newSegDir2, "file1;file2;file5");

        DeployIndexWrapper::DumpSegmentDeployIndex(oldSegDir0, "");
        DeployIndexWrapper::DumpSegmentDeployIndex(newSegDir0, "");
        DeployIndexWrapper::DumpSegmentDeployIndex(newSegDir2, "");

        string oldRoot = util::PathUtil::JoinPath(mRootDir, "oldPath");
        string newRoot = util::PathUtil::JoinPath(mRootDir, "newPath");
        StoreDefaultVersionContent(oldRoot, "0" /* segIds */, 0 /* versionId */);
        StoreDefaultVersionContent(newRoot, "0" /* segIds */, 0 /* versionId */);
        StoreDefaultVersionContent(newRoot, "0, 2", 2);

        // newRootDir->Sync(true);
        // oldRootDir->Sync(true);

        FileList tmpList;
        FileList remoteFileList;
        bool result = GetDeployFiles(newRoot, oldRoot, mOnlineConfig, tmpList, remoteFileList, 2, 0);

        ASSERT_TRUE(result);
        EXPECT_THAT(tmpList,
                    UnorderedElementsAre("segment_2_level_0/", string("segment_2_level_0/") + SEGMENT_FILE_LIST,
                                         string("segment_2_level_0/") + DEPLOY_INDEX_FILE_NAME,
                                         string("segment_2_level_0") + "/file1", string("segment_2_level_0") + "/file2",
                                         string("segment_2_level_0") + "/file5"));
    }

    void TestGetDeployFilesWithSubIndexPartition()
    {
        SCOPED_TRACE("TestGetDeployFilesWithSubIndexPartition");
        // lastVersion is invalid
        DirectoryPtr testDir = GET_PARTITION_DIRECTORY()->MakeDirectory("segment_0_level_0");
        CreateFiles(testDir, "file1;file2;file3");
        DirectoryPtr testSubDir = testDir->MakeDirectory(SUB_SEGMENT_DIR_NAME);
        CreateFiles(testSubDir, "file1;file2;file3");

        StoreDefaultVersionContent(mRootDir);
        auto segDir = GET_PARTITION_DIRECTORY()->GetDirectory("segment_0_level_0", false);
        ASSERT_NE(segDir, nullptr);
        FileList tmpList;
        FileList remoteFileList;
        DeployIndexWrapper::DumpSegmentDeployIndex(segDir, "");
        ASSERT_TRUE(GetDeployFiles(mRootDir, "", mOnlineConfig, tmpList, remoteFileList, 0));
        EXPECT_THAT(tmpList,
                    UnorderedElementsAre(
                        string("segment_0_level_0/"), string("segment_0_level_0/segment_file_list"),
                        string("segment_0_level_0/file1"), string("segment_0_level_0/file2"),
                        string("segment_0_level_0/file3"), string("segment_0_level_0/deploy_index"),
                        string("segment_0_level_0/sub_segment/"), string("segment_0_level_0/sub_segment/file1"),
                        string("segment_0_level_0/sub_segment/file2"), string("segment_0_level_0/sub_segment/file3")));
    }

    void TestGetDeployFilesWithLifecycle()
    {
        SCOPED_TRACE("TestGetDeployFilesWithLifecycle");
        // lastVersion is invalid
        DirectoryPtr seg0 = GET_PARTITION_DIRECTORY()->MakeDirectory("segment_0_level_0");
        DirectoryPtr seg1 = GET_PARTITION_DIRECTORY()->MakeDirectory("segment_1_level_0");
        CreateFiles(seg0, "file1;file2;file3");
        CreateFiles(seg1, "file1;file2;file3");

        StoreDefaultVersionContent(mRootDir, "0,1", 0, "HOT,WARM");

        string loadConfigStr = R"(
        {
            "load_config" :
            [
                {
                    "file_patterns": [ ".*" ],
                    "lifecycle" : "HOT",
                    "load_strategy": "cache",
                    "remote" : false,
                    "deploy" : true
                },
                {
                    "file_patterns": [ ".*" ],
                    "lifecycle" : "WARM",
                    "load_strategy": "cache",
                    "remote" : true,
                    "deploy" : false
                }
            ]
        })";
        mOnlineConfig.needReadRemoteIndex = true;
        mOnlineConfig.loadConfigList = LoadConfigListCreator::CreateLoadConfigListFromJsonString(loadConfigStr);

        FileList tmpList;
        FileList remoteFileList;
        DeployIndexWrapper::DumpSegmentDeployIndex(seg0, "");
        DeployIndexWrapper::DumpSegmentDeployIndex(seg1, "");
        ASSERT_TRUE(GetDeployFiles(mRootDir, "", mOnlineConfig, tmpList, remoteFileList, 0));
        EXPECT_THAT(tmpList,
                    UnorderedElementsAre(string("segment_0_level_0/"), string("segment_0_level_0/segment_file_list"),
                                         string("segment_0_level_0/file1"), string("segment_0_level_0/file2"),
                                         string("segment_0_level_0/file3"), string("segment_0_level_0/deploy_index")));

        EXPECT_THAT(remoteFileList,
                    UnorderedElementsAre(string("segment_1_level_0/"), string("segment_1_level_0/segment_file_list"),
                                         string("segment_1_level_0/file1"), string("segment_1_level_0/file2"),
                                         string("segment_1_level_0/file3"), string("segment_1_level_0/deploy_index")));

        // new version1
        tmpList.clear();
        remoteFileList.clear();
        DirectoryPtr seg2 = GET_PARTITION_DIRECTORY()->MakeDirectory("segment_2_level_0");
        CreateFiles(seg2, "file1;file2;file3");
        DeployIndexWrapper::DumpSegmentDeployIndex(seg2, "");
        StoreDefaultVersionContent(mRootDir, "0,1,2", 1, "HOT,WARM,WARM");
        ASSERT_TRUE(GetDeployFiles(mRootDir, "", mOnlineConfig, tmpList, remoteFileList, 1, 0));
        EXPECT_TRUE(tmpList.empty());
        EXPECT_THAT(remoteFileList,
                    UnorderedElementsAre(string("segment_2_level_0/"), string("segment_2_level_0/segment_file_list"),
                                         string("segment_2_level_0/file1"), string("segment_2_level_0/file2"),
                                         string("segment_2_level_0/file3"), string("segment_2_level_0/deploy_index")));
    }

    void TestDumpDeployIndexWithAtomicStoreFail()
    {
        SCOPED_TRACE("TestDumpDeployIndexWithAtomicStoreFail");
        string testPath = util::PathUtil::JoinPath(mRootDir, "segment_0_level_0");
        auto testDir = GET_PARTITION_DIRECTORY()->MakeDirectory("segment_0_level_0");
        ASSERT_NE(testDir, nullptr);

        CreateFiles(testDir, "file1;file2;file3");
        string tempDeployIndexFile = string(SEGMENT_FILE_LIST) + TEMP_FILE_SUFFIX;
        CreateFiles(testDir, tempDeployIndexFile);

        SegmentFileListWrapper::Dump(testDir, "");
        StoreDefaultVersionContent(mRootDir);
        FileList tmpList;
        FileList remoteFileList;
        GetDeployFiles(mRootDir, "", mOnlineConfig, tmpList, remoteFileList, -1 /* lastest version */);
        EXPECT_THAT(tmpList, UnorderedElementsAre(
                                 string("segment_0_level_0/"), string("segment_0_level_0/") + string(SEGMENT_FILE_LIST),
                                 string("segment_0_level_0/deploy_index"), string("segment_0_level_0/file1"),
                                 string("segment_0_level_0/file2"), string("segment_0_level_0/file3")));
    }

    void TestDumpDeployIndexWithLength()
    {
        string testPath = util::PathUtil::JoinPath(mRootDir, "DumpDeployIndex");
        string f1 = util::PathUtil::JoinPath(testPath, "f1");
        string f3 = util::PathUtil::JoinPath(testPath, "f3");
        string f5 = util::PathUtil::JoinPath(testPath, "f5");
        ASSERT_EQ(FSEC_OK, FslibWrapper::AtomicStore(f1, "1").Code());
        ASSERT_EQ(FSEC_OK, FslibWrapper::AtomicStore(f3, "333").Code());
        ASSERT_EQ(FSEC_OK, FslibWrapper::AtomicStore(f5, "55555").Code());

        struct utimbuf utimeBuf;
        utimeBuf.modtime = 11;
        utimeBuf.actime = 22;
        ASSERT_TRUE(utime(f1.c_str(), &utimeBuf) == 0);

        RESET_FILE_SYSTEM();
        auto testDir = GET_PARTITION_DIRECTORY()->GetDirectory("DumpDeployIndex", false);
        ASSERT_NE(testDir, nullptr);
        SegmentFileListWrapper::Dump(testDir, "");

        ASSERT_TRUE(testDir->IsExist(SEGMENT_FILE_LIST));

        string content;
        testDir->Load(SEGMENT_FILE_LIST, content);
        IndexFileList deployIndexMeta;
        deployIndexMeta.FromString(content);

        auto& deployFileMetas = deployIndexMeta.deployFileMetas;
        ASSERT_EQ(3, deployFileMetas.size());
        map<string, FileInfo> actualMap;
        for (auto meta : deployFileMetas) {
            actualMap[meta.filePath] = meta;
        }
        ASSERT_EQ(actualMap.size(), deployFileMetas.size());
        ASSERT_EQ(1, actualMap["f1"].fileLength);
        ASSERT_EQ(uint64_t(-1), actualMap["f1"].modifyTime);
        ASSERT_EQ(3, actualMap["f3"].fileLength);
        ASSERT_EQ(5, actualMap["f5"].fileLength);

        ASSERT_EQ(uint64_t(-1), actualMap["f3"].modifyTime);
        ASSERT_EQ(uint64_t(-1), actualMap["f5"].modifyTime);

        FslibWrapper::DeleteDirE(testPath, DeleteOption::NoFence(false));
    }

    void TestGetDeployIndexMeta()
    {
        auto offlineFs = FileSystemCreator::CreateForWrite("", GET_TEMP_DATA_PATH() + "/offline").GetOrThrow();
        DirectoryPtr offlineDir = Directory::Get(offlineFs);
        string offlinePath = offlineDir->GetOutputPath();
        string localPath = GET_PARTITION_DIRECTORY()->MakeDirectory("local")->GetOutputPath();
        DeployIndexMeta remoteDeployMeta, localDeployMeta;
        OnlineConfig targetOnlineConfig, baseOnlineConfig;

        ASSERT_FALSE(GetDeployIndexMeta(remoteDeployMeta, localDeployMeta, offlinePath, localPath, 3, 2,
                                        targetOnlineConfig, baseOnlineConfig));

        PartitionDataMaker::MakeVersions(offlineDir, "0:0; 1:0,1; 2:1,2");
        ASSERT_FALSE(GetDeployIndexMeta(remoteDeployMeta, localDeployMeta, offlinePath, localPath, 3, 2,
                                        targetOnlineConfig, baseOnlineConfig));

        ASSERT_TRUE(GetDeployIndexMeta(remoteDeployMeta, localDeployMeta, offlinePath, localPath, 2, INVALID_VERSIONID,
                                       targetOnlineConfig, baseOnlineConfig));

        ASSERT_TRUE(GetDeployIndexMeta(remoteDeployMeta, localDeployMeta, offlinePath, localPath, 2, 0,
                                       targetOnlineConfig, baseOnlineConfig));

        ASSERT_EQ(FSEC_OK, FslibWrapper::Copy(offlinePath + "/deploy_meta.0", localPath + "/deploy_meta.0").Code());
        ASSERT_TRUE(GetDeployIndexMeta(remoteDeployMeta, localDeployMeta, offlinePath, localPath, 2, 0,
                                       targetOnlineConfig, baseOnlineConfig));
    }

    void TestGetDeployIndexMetaBaseVersionInvalid()
    {
        SCOPED_TRACE(GET_TEST_NAME());
        DirectoryPtr offlineDir = GET_PARTITION_DIRECTORY()->MakeDirectory("offline");
        DirectoryPtr localDir = GET_PARTITION_DIRECTORY()->MakeDirectory("local");
        DeployIndexMeta remoteDeployMeta, localDeployMeta;
        OnlineConfig onlineConfig;

        IndexFileList baseVersionDeployMeta, targetVersionDeployMeta;
        targetVersionDeployMeta.Append(FileInfo("remote_1_deploy_1", 10));
        targetVersionDeployMeta.Append(FileInfo("remote_1_deploy_0", 10));
        targetVersionDeployMeta.Append(FileInfo("remote_0_deploy_1", 10));
        targetVersionDeployMeta.Append(FileInfo("deploy_meta.2", -1));
        targetVersionDeployMeta.AppendFinal(FileInfo("version.2", 10));
        offlineDir->Store("deploy_meta.2", targetVersionDeployMeta.ToString(), WriterOption());
        offlineDir->Store("version.2", "", WriterOption());

        FromJsonString(onlineConfig, R"( {
            "load_config": [
                {"file_patterns": ["remote_1_deploy_1"], "remote": true, "deploy": true },
                {"file_patterns": ["remote_1_deploy_0"], "remote": true, "deploy": false },
                {"file_patterns": ["remote_0_deploy_1"], "remote": false, "deploy": true }
            ],
            "need_read_remote_index": true
        } )");

        ASSERT_TRUE(GetDeployIndexMeta(remoteDeployMeta, localDeployMeta, offlineDir->GetOutputPath(),
                                       localDir->GetOutputPath(), 2, INVALID_VERSIONID, onlineConfig, onlineConfig));
        CheckDeployMeta(remoteDeployMeta, {"remote_1_deploy_1", "remote_1_deploy_0"}, {}, __FILE__, __LINE__);
        CheckDeployMeta(localDeployMeta, {"remote_1_deploy_1", "remote_0_deploy_1", "deploy_meta.2"}, {"version.2"},
                        __FILE__, __LINE__);
    }

    void TestGetDeployIndexMetaBaseVersionLost()
    {
        SCOPED_TRACE(GET_TEST_NAME());
        DirectoryPtr offlineDir = GET_PARTITION_DIRECTORY()->MakeDirectory("offline");
        DirectoryPtr localDir = GET_PARTITION_DIRECTORY()->MakeDirectory("local");
        DeployIndexMeta remoteDeployMeta, localDeployMeta;
        OnlineConfig onlineConfig;

        IndexFileList targetVersionDeployMeta;
        targetVersionDeployMeta.Append(FileInfo("remote_1_deploy_1", 10));
        targetVersionDeployMeta.Append(FileInfo("remote_1_deploy_0", 10));
        targetVersionDeployMeta.Append(FileInfo("remote_0_deploy_1", 10));
        targetVersionDeployMeta.Append(FileInfo("deploy_meta.2", -1));
        targetVersionDeployMeta.AppendFinal(FileInfo("version.2", 10));
        offlineDir->Store("deploy_meta.2", targetVersionDeployMeta.ToString(), WriterOption());
        offlineDir->Store("version.2", "", WriterOption());

        FromJsonString(onlineConfig, R"( {
            "load_config": [
                {"file_patterns": ["remote_1_deploy_1"], "remote": true, "deploy": true },
                {"file_patterns": ["remote_1_deploy_0"], "remote": true, "deploy": false },
                {"file_patterns": ["remote_0_deploy_1"], "remote": false, "deploy": true }
            ],
            "need_read_remote_index": true
        } )");

        ASSERT_TRUE(GetDeployIndexMeta(remoteDeployMeta, localDeployMeta, offlineDir->GetOutputPath(),
                                       localDir->GetOutputPath(), 2, 1, onlineConfig, onlineConfig));
        CheckDeployMeta(remoteDeployMeta, {"remote_1_deploy_1", "remote_1_deploy_0"}, {}, __FILE__, __LINE__);
        CheckDeployMeta(localDeployMeta, {"remote_1_deploy_1", "remote_0_deploy_1", "deploy_meta.2"}, {"version.2"},
                        __FILE__, __LINE__);
    }

    void TestGetDeployIndexMetaComplex()
    {
        SCOPED_TRACE(GET_TEST_NAME());
        DirectoryPtr offlineDir = GET_PARTITION_DIRECTORY()->MakeDirectory("offline");
        DirectoryPtr localDir = GET_PARTITION_DIRECTORY()->MakeDirectory("local");
        DeployIndexMeta remoteDeployMeta, localDeployMeta;
        OnlineConfig baseOnlineConfig, targetOnlineConfig;

        IndexFileList baseVersionDeployMeta, targetVersionDeployMeta;
        // fileName [r1d1-r0d1] means: base loadConfig remote [true] deploy [true]
        //                             dest loadConfig remote [false] deploy [true]
        baseVersionDeployMeta.Append(FileInfo("r1d1-r1d1", 10));
        baseVersionDeployMeta.Append(FileInfo("r1d1-r1d0", 10));
        baseVersionDeployMeta.Append(FileInfo("r1d1-r0d1", 10));
        baseVersionDeployMeta.Append(FileInfo("r1d0-r1d1", 10));
        baseVersionDeployMeta.Append(FileInfo("r1d0-r1d0", 10));
        baseVersionDeployMeta.Append(FileInfo("r1d0-r0d1", 10));
        baseVersionDeployMeta.Append(FileInfo("r0d1-r1d1", 10));
        baseVersionDeployMeta.Append(FileInfo("r0d1-r1d0", 10));
        baseVersionDeployMeta.Append(FileInfo("r0d1-r0d1", 10));
        targetVersionDeployMeta = baseVersionDeployMeta;
        // base
        baseVersionDeployMeta.Append(FileInfo("deploy_meta.1", -1));
        baseVersionDeployMeta.AppendFinal(FileInfo("version.1", 10));
        localDir->Store("deploy_meta.1", baseVersionDeployMeta.ToString(), WriterOption());
        offlineDir->Store("deploy_meta.1", baseVersionDeployMeta.ToString(), WriterOption());

        // target
        targetVersionDeployMeta.Append(FileInfo("-----r1d1", 10)); // new files
        targetVersionDeployMeta.Append(FileInfo("-----r1d0", 10));
        targetVersionDeployMeta.Append(FileInfo("-----r0d1", 10));
        targetVersionDeployMeta.Append(FileInfo("deploy_meta.2", -1));
        targetVersionDeployMeta.AppendFinal(FileInfo("version.2", 10));
        offlineDir->Store("deploy_meta.2", targetVersionDeployMeta.ToString(), WriterOption());
        offlineDir->Store("version.2", "", WriterOption());

        FromJsonString(baseOnlineConfig, R"( {
            "load_config": [
                {"file_patterns": ["r1d1-.*"], "remote": true, "deploy": true },
                {"file_patterns": ["r1d0-.*"], "remote": true, "deploy": false },
                {"file_patterns": ["r0d1-.*"], "remote": false, "deploy": true }
            ],
            "need_read_remote_index": true
        } )");
        FromJsonString(targetOnlineConfig, R"( {
            "load_config": [
                {"file_patterns": [".*-r1d1"], "remote": true, "deploy": true },
                {"file_patterns": [".*-r1d0"], "remote": true, "deploy": false },
                {"file_patterns": [".*-r0d1"], "remote": false, "deploy": true }
            ],
            "need_read_remote_index": true
        } )");

        ASSERT_TRUE(GetDeployIndexMeta(remoteDeployMeta, localDeployMeta, offlineDir->GetOutputPath(),
                                       offlineDir->GetOutputPath(), 2, 1, targetOnlineConfig, baseOnlineConfig));
        CheckDeployMeta(remoteDeployMeta, {"r0d1-r1d1", "r0d1-r1d0", "-----r1d1", "-----r1d0"}, {}, __FILE__, __LINE__);
        CheckDeployMeta(localDeployMeta, {"r1d0-r1d1", "r1d0-r0d1", "-----r1d1", "-----r0d1", "deploy_meta.2"},
                        {"version.2"}, __FILE__, __LINE__);
    }

    void TestGetDeployIndexMetaDeployMetaNotExist()
    {
        SCOPED_TRACE(GET_TEST_NAME());
        auto offlineFs = FileSystemCreator::CreateForWrite("", GET_TEMP_DATA_PATH() + "/offline").GetOrThrow();
        auto localFs = FileSystemCreator::CreateForWrite("", GET_TEMP_DATA_PATH() + "/local").GetOrThrow();
        DirectoryPtr offlineDir = Directory::Get(offlineFs);
        DirectoryPtr localDir = Directory::Get(localFs);
        DeployIndexMeta remoteDeployMeta, localDeployMeta;
        OnlineConfig baseOnlineConfig, targetOnlineConfig;

        IndexFileList seg0DeployMeta, seg2DeployMeta;
        // fileName [r1d1-r0d1] means: base loadConfig remote [true] deploy [true]
        //                             dest loadConfig remote [false] deploy [true]
        // base
        seg0DeployMeta.Append(FileInfo("r1d1-r1d1", 10));
        seg0DeployMeta.Append(FileInfo("r1d1-r1d0", 10));
        seg0DeployMeta.Append(FileInfo("r1d1-r0d1", 10));
        seg0DeployMeta.Append(FileInfo("r1d0-r1d1", 10));
        seg0DeployMeta.Append(FileInfo("r1d0-r1d0", 10));
        seg0DeployMeta.Append(FileInfo("r1d0-r0d1", 10));
        seg0DeployMeta.Append(FileInfo("r0d1-r1d1", 10));
        seg0DeployMeta.Append(FileInfo("r0d1-r1d0", 10));
        seg0DeployMeta.Append(FileInfo("r0d1-r0d1", 10));
        const auto seg0Dir = offlineDir->MakeDirectory("segment_0_level_0");
        seg0Dir->Store("deploy_index", seg0DeployMeta.ToString(), WriterOption());
        CreateFiles(seg0Dir, "r1d1-r1d1;r1d1-r1d0;r1d1-r0d1;r1d0-r1d1;r1d0-r1d0;r1d0-r0d1;r0d1-r1d1;r0d1-r1d0;r0d1-"
                             "r0d1");

        Version baseVersion(0);
        baseVersion.AddSegment(0);
        baseVersion.Store(offlineDir, true);
        // target
        seg2DeployMeta.Append(FileInfo("-----r1d1", 10));
        seg2DeployMeta.Append(FileInfo("-----r1d0", 10));
        seg2DeployMeta.Append(FileInfo("-----r0d1", 10));
        const auto seg2Dir = offlineDir->MakeDirectory("segment_2_level_0");
        seg2Dir->Store("deploy_index", seg2DeployMeta.ToString(), WriterOption());
        CreateFiles(seg2Dir, "-----r1d1;-----r1d0;-----r0d1");

        // local
        const auto localSeg1Dir = localDir->MakeDirectory("segment_1_level_0");
        Version localVersion(1);
        localVersion.AddSegment(1);
        localVersion.Store(localDir, true);

        RESET_FILE_SYSTEM("ut", false /* auto mount */);
        ASSERT_EQ(FSEC_OK, offlineDir->GetFileSystem()->MountDir(GET_TEMP_DATA_PATH(), "", "",
                                                                 file_system::FSMT_READ_ONLY, true));
        Version targetVerion(2);
        targetVerion.AddSegment(0);
        targetVerion.AddSegment(2);
        targetVerion.Store(offlineDir, true);

        FromJsonString(baseOnlineConfig, R"( {
            "load_config": [
                {"file_patterns": ["r1d1-.*"], "remote": true, "deploy": true },
                {"file_patterns": ["r1d0-.*"], "remote": true, "deploy": false },
                {"file_patterns": ["r0d1-.*"], "remote": false, "deploy": true },
                {"file_patterns": ["deploy_index"], "remote": true, "deploy": false }
            ],
            "need_read_remote_index": true
        } )");
        FromJsonString(targetOnlineConfig, R"( {
            "load_config": [
                {"file_patterns": [".*-r1d1"], "remote": true, "deploy": true },
                {"file_patterns": [".*-r1d0"], "remote": true, "deploy": false },
                {"file_patterns": [".*-r0d1"], "remote": false, "deploy": true }
            ],
            "need_read_remote_index": true
        } )");

        ASSERT_TRUE(GetDeployIndexMeta(remoteDeployMeta, localDeployMeta, offlineDir->GetOutputPath(),
                                       offlineDir->GetOutputPath(), 2, 0, targetOnlineConfig, baseOnlineConfig));
        CheckDeployMeta(remoteDeployMeta,
                        {"segment_0_level_0/r0d1-r1d1", "segment_0_level_0/r0d1-r1d0", "segment_2_level_0/-----r1d1",
                         "segment_2_level_0/-----r1d0"},
                        {}, __FILE__, __LINE__);
        CheckDeployMeta(localDeployMeta,
                        {"segment_0_level_0/r1d0-r1d1", "segment_0_level_0/r1d0-r0d1", "segment_0_level_0/deploy_index",
                         "segment_2_level_0/-----r1d1", "segment_2_level_0/-----r0d1", "segment_2_level_0/deploy_index",
                         "segment_2_level_0/"},
                        {"version.2"}, __FILE__, __LINE__);
    }

    void TestGetDeployIndexMetaVersionFile()
    {
        string offlinePath = GET_PARTITION_DIRECTORY()->MakeDirectory("offline")->GetOutputPath();
        string localPath = GET_PARTITION_DIRECTORY()->MakeDirectory("local")->GetOutputPath();
        IndexFileList targetVersionDeployMeta;
        targetVersionDeployMeta.Append(FileInfo("deploy_meta.2", -1));
        targetVersionDeployMeta.AppendFinal(FileInfo("version.2", 10));
        ASSERT_EQ(FSEC_OK,
                  FslibWrapper::Store(offlinePath + "/deploy_meta.2", targetVersionDeployMeta.ToString()).Code());
        ASSERT_EQ(FSEC_OK, FslibWrapper::Store(offlinePath + "/version.2", "").Code());

        {
            OnlineConfig baseOnlineConfig, targetOnlineConfig;
            DeployIndexMeta remoteDeployMeta, localDeployMeta;
            FromJsonString(targetOnlineConfig, R"( {
                "load_config": [
                    {"file_patterns": ["version.*"], "remote": true, "deploy": true }
                ],
                "need_read_remote_index": true
            } )");
            ASSERT_TRUE(GetDeployIndexMeta(remoteDeployMeta, localDeployMeta, offlinePath, localPath, 2,
                                           INVALID_VERSIONID, targetOnlineConfig, baseOnlineConfig));
            CheckDeployMeta(remoteDeployMeta, {}, {"version.2"}, __FILE__, __LINE__);
            CheckDeployMeta(localDeployMeta, {"deploy_meta.2"}, {"version.2"}, __FILE__, __LINE__);
        }
        {
            OnlineConfig baseOnlineConfig, targetOnlineConfig;
            DeployIndexMeta remoteDeployMeta, localDeployMeta;
            FromJsonString(targetOnlineConfig, R"( {
                "load_config": [
                    {"file_patterns": ["version.*"], "remote": true, "deploy": false }
                ],
                "need_read_remote_index": true
            } )");
            ASSERT_TRUE(GetDeployIndexMeta(remoteDeployMeta, localDeployMeta, offlinePath, localPath, 2,
                                           INVALID_VERSIONID, targetOnlineConfig, baseOnlineConfig));
            CheckDeployMeta(remoteDeployMeta, {}, {"version.2"}, __FILE__, __LINE__);
            CheckDeployMeta(localDeployMeta, {"deploy_meta.2"}, {}, __FILE__, __LINE__);
        }
        {
            OnlineConfig baseOnlineConfig, targetOnlineConfig;
            DeployIndexMeta remoteDeployMeta, localDeployMeta;
            FromJsonString(targetOnlineConfig, R"( {
                "load_config": [
                    {"file_patterns": ["version.*"], "remote": false, "deploy": true }
                ],
                "need_read_remote_index": true
            } )");
            ASSERT_TRUE(GetDeployIndexMeta(remoteDeployMeta, localDeployMeta, offlinePath, localPath, 2,
                                           INVALID_VERSIONID, targetOnlineConfig, baseOnlineConfig));
            CheckDeployMeta(remoteDeployMeta, {}, {}, __FILE__, __LINE__);
            CheckDeployMeta(localDeployMeta, {"deploy_meta.2"}, {"version.2"}, __FILE__, __LINE__);
        }
    }

    void TestDeployIndexMetaWithOngoingOps()
    {
        auto offlineFs = FileSystemCreator::CreateForWrite("", GET_TEMP_DATA_PATH() + "/offline").GetOrThrow();
        auto localFs = FileSystemCreator::CreateForWrite("", GET_TEMP_DATA_PATH() + "/local").GetOrThrow();
        DirectoryPtr offlineDir = Directory::Get(offlineFs);
        DirectoryPtr localDir = Directory::Get(localFs);
        Version oldVersion;
        oldVersion.SetVersionId(2);
        oldVersion.SetOngoingModifyOperations({3});
        oldVersion.SetSchemaVersionId(3);
        oldVersion.AddSegment(0);
        oldVersion.Store(offlineDir, true);
        ASSERT_EQ(FSEC_OK, FslibWrapper::Copy(PathUtil::JoinPath(GET_PRIVATE_TEST_DATA_PATH(),
                                                                 "schema_for_deploy_inedx_wrapper_test.json"),
                                              offlineDir->GetOutputPath() + "/schema.json.3")
                               .Code());
        ASSERT_EQ(FSEC_OK, FslibWrapper::Copy(PathUtil::JoinPath(GET_PRIVATE_TEST_DATA_PATH(),
                                                                 "schema_for_deploy_inedx_wrapper_test.json"),
                                              localDir->GetOutputPath() + "/schema.json.3")
                               .Code());

        IndexFileList oldDeployMeta;
        // nid price string2 is deleted,
        oldDeployMeta.Append(FileInfo("segment_0_level_0/", -1));
        oldDeployMeta.Append(FileInfo("segment_0_level_0/index/", -1));
        oldDeployMeta.Append(FileInfo("segment_0_level_0/index/new_nid/", -1));
        oldDeployMeta.Append(FileInfo("segment_0_level_0/index/new_nid/data", 10));
        oldDeployMeta.Append(FileInfo("segment_0_level_0/attribute/", -1));
        oldDeployMeta.Append(FileInfo("segment_0_level_0/attribute/price/", -1));
        oldDeployMeta.Append(FileInfo("segment_0_level_0/attribute/price/data", 10));
        oldDeployMeta.Append(FileInfo("segment_0_level_0/attribute/string2/", -1));
        oldDeployMeta.Append(FileInfo("segment_0_level_0/attribute/string2/data", 10));
        oldDeployMeta.Append(FileInfo("patch_index_0/", -1));
        oldDeployMeta.Append(FileInfo("patch_index_0/segment_0_level_0/", -1));
        oldDeployMeta.Append(FileInfo("patch_index_0/segment_0_level_0/index/", -1));
        oldDeployMeta.Append(FileInfo("patch_index_0/segment_0_level_0/index/nid/", -1));
        oldDeployMeta.Append(FileInfo("patch_index_0/segment_0_level_0/index/nid/data", 10));
        oldDeployMeta.Append(FileInfo("segment_1_level_0/", -1));
        oldDeployMeta.Append(FileInfo("segment_1_level_0/index/", -1));
        oldDeployMeta.Append(FileInfo("segment_1_level_0/index/nid/", -1));
        oldDeployMeta.Append(FileInfo("segment_1_level_0/index/nid/data", 10));

        int64_t schemaFileLen = 0;
        auto ec = FslibWrapper::GetFileLength(PathUtil::JoinPath(offlineDir->GetOutputPath(), "schema.json.3"),
                                              schemaFileLen);
        ASSERT_EQ(ec, file_system::ErrorCode::FSEC_OK);
        oldDeployMeta.Append(FileInfo("schema.json.3", schemaFileLen));
        // uid is added but not ready
        oldDeployMeta.Append(FileInfo("segment_1_level_0/index/uid/", -1));
        oldDeployMeta.Append(FileInfo("segment_1_level_0/index/uid/data", 10));

        auto getVersionFileLength = [&](const std::string versionName) -> size_t {
            std::string versionPath = PathUtil::JoinPath(offlineDir->GetOutputPath(), versionName);
            return file_system::FslibWrapper::GetFileLength(versionPath).GetOrThrow();
        };

        auto versionFileLen = getVersionFileLength("version.2");
        oldDeployMeta.Append(FileInfo("deploy_meta.2", -1));
        oldDeployMeta.AppendFinal(FileInfo("version.2", versionFileLen));
        string deployMetaFileName = DeployIndexWrapper::GetDeployMetaFileName(oldVersion.GetVersionId());
        offlineDir->Store(deployMetaFileName, oldDeployMeta.ToString());
        offlineDir->Sync(true);

        // make sure wrapper could mount from deploy_meta, or failed for segment not exist in
        // physical
        localDir->Store(deployMetaFileName, oldDeployMeta.ToString());
        localDir->Sync(true);
        DeployIndexMeta remoteDeploymeta, localDeployMeta;
        OnlineConfig onlineConfig;
        FromJsonString(onlineConfig, R"( {
        "disable_fields": {
            "attributes": ["string2"],
            "rewrite_load_config": true
        }
        } )");
        // deploy done
        GetDeployIndexMeta(remoteDeploymeta, localDeployMeta, offlineDir->GetOutputPath(), localDir->GetOutputPath(), 2,
                           -1, onlineConfig, onlineConfig);
        CheckDeployMeta(localDeployMeta,
                        {"deploy_meta.2", "schema.json.3", "patch_index_0/", "patch_index_0/segment_0_level_0/",
                         "patch_index_0/segment_0_level_0/index/", "segment_0_level_0/", "segment_0_level_0/attribute/",
                         "segment_0_level_0/index/", "segment_0_level_0/index/new_nid/",
                         "segment_0_level_0/index/new_nid/data", "segment_1_level_0/", "segment_1_level_0/index/"},
                        {"version.2"}, __FILE__, __LINE__);
        oldVersion.Store(localDir, true);

        // make op3 ready
        // RESET_FILE_SYSTEM("ut", false /* auto mount */);
        // offlineDir = GET_PARTITION_DIRECTORY()->MakeDirectory("offline");
        // offlineDir->GetFileSystem()->Mount(GET_TEMP_DATA_PATH(), "",
        // file_system::FSMT_READ_ONLY);

        Version newVersion;
        newVersion.SetVersionId(3);
        newVersion.SetOngoingModifyOperations({});
        newVersion.SetSchemaVersionId(3);
        newVersion.AddSegment(0);
        newVersion.AddSegment(2);
        newVersion.TEST_Store(offlineDir->GetOutputPath(), true);

        const auto versionName = newVersion.GetVersionFileName();
        ASSERT_EQ(FSEC_OK, offlineDir->GetFileSystem()->MountFile(offlineDir->GetOutputPath(), versionName, versionName,
                                                                  FSMT_READ_ONLY, -1, false));

        IndexFileList newDeployMeta;
        // nid & uid is ready
        newDeployMeta = oldDeployMeta;
        newDeployMeta.Append(FileInfo("segment_2_level_0/", -1));
        newDeployMeta.Append(FileInfo("segment_2_level_0/index/", -1));
        newDeployMeta.Append(FileInfo("segment_2_level_0/index/nid/", -1));
        newDeployMeta.Append(FileInfo("segment_2_level_0/index/nid/data", 10));
        newDeployMeta.Append(FileInfo("segment_2_level_0/index/uid/", -1));
        newDeployMeta.Append(FileInfo("segment_2_level_0/index/uid/data", 10));
        newDeployMeta.Append(FileInfo("segment_2_level_0/attribute/", -1));
        newDeployMeta.Append(FileInfo("segment_2_level_0/attribute/price/", -1));
        newDeployMeta.Append(FileInfo("segment_2_level_0/attribute/price/data", 10));
        newDeployMeta.Append(FileInfo("segment_2_level_0/attribute/string2/", -1));
        newDeployMeta.Append(FileInfo("segment_2_level_0/attribute/string2/data", 10));
        newDeployMeta.Append(FileInfo("patch_index_0/segment_0_level_0/index/uid/", -1));
        newDeployMeta.Append(FileInfo("patch_index_0/segment_0_level_0/index/uid/data", 10));

        newDeployMeta.AppendFinal(FileInfo("version.2", getVersionFileLength("version.2")));
        newDeployMeta.AppendFinal(FileInfo("deploy_meta.3", -1));
        newDeployMeta.AppendFinal(FileInfo("version.3", getVersionFileLength("version.3")));
        // newDeployMeta.AppendFinal(FileInfo("schema.json.3", schemaFileLen));
        deployMetaFileName = DeployIndexWrapper::GetDeployMetaFileName(newVersion.GetVersionId());
        offlineDir->Store(deployMetaFileName, newDeployMeta.ToString());
        offlineDir->Sync(true);

        DeployIndexMeta remoteDeploymeta2, localDeployMeta2;
        GetDeployIndexMeta(remoteDeploymeta2, localDeployMeta2, offlineDir->GetOutputPath(),
                           offlineDir->GetOutputPath(), 3, 2, onlineConfig, onlineConfig);
        CheckDeployMeta(localDeployMeta2,
                        {"deploy_meta.3", "segment_2_level_0/", "segment_2_level_0/attribute/",
                         "segment_2_level_0/index/", "segment_2_level_0/index/nid/",
                         "segment_2_level_0/index/nid/data", // new segment
                         "segment_2_level_0/index/uid/", "segment_2_level_0/index/uid/data",
                         // "segment_1_level_0/",
                         // "segment_1_level_0/index/",
                         "segment_1_level_0/index/nid/",
                         "segment_1_level_0/index/nid/data", // old segment
                         "segment_1_level_0/index/uid/", "segment_1_level_0/index/uid/data",
                         // "patch_index_0/",
                         // "patch_index_0/segment_0_level_0/",
                         // "patch_index_0/segment_0_level_0/index/",
                         "patch_index_0/segment_0_level_0/index/nid/", "patch_index_0/segment_0_level_0/index/nid/data",
                         "patch_index_0/segment_0_level_0/index/uid/",
                         "patch_index_0/segment_0_level_0/index/uid/data"},
                        {"version.3"}, __FILE__, __LINE__);
    }

    void TestGenerateDisableLoadConfig()
    {
        Version version;
        version.SetVersionId(10);
        version.SetOngoingModifyOperations({3});
        version.SetSchemaVersionId(3);
        string versionPath = util::PathUtil::JoinPath(mRootDir, "version.10");
        ASSERT_EQ(FSEC_OK, FslibWrapper::AtomicStore(versionPath, ToJsonString(version)).Code());
        ASSERT_EQ(FSEC_OK, FslibWrapper::Copy(
                               PathUtil::JoinPath(GET_PRIVATE_TEST_DATA_PATH(), "schema_with_modify_operations_2.json"),
                               mRootDir + "/schema.json.3")
                               .Code());
        RESET_FILE_SYSTEM();
        // index2, price, string2 will be delete
        // nid is deleted, but add again, not ready
        LoadConfig loadConfig;
        ASSERT_TRUE(DeployIndexWrapper::GenerateDisableLoadConfig(mRootDir, 10, loadConfig));
        ASSERT_TRUE(loadConfig.Match("segment_2_level_0/attribute/price", ""));
        ASSERT_TRUE(loadConfig.Match("segment_2_level_0/index/index2", ""));
        ASSERT_TRUE(loadConfig.Match("segment_2_level_0/attribute/string2/data", ""));
        ASSERT_TRUE(loadConfig.Match("segment_2_level_0/index/nid/data", ""));
        ASSERT_TRUE(loadConfig.Match("segment_2_level_0/attribute/nid/data", ""));

        // make operation 3 ready
        version.SetOngoingModifyOperations({});
        ASSERT_EQ(FSEC_OK, FslibWrapper::AtomicStore(versionPath, ToJsonString(version), true).Code());
        RESET_FILE_SYSTEM();
        ASSERT_TRUE(DeployIndexWrapper::GenerateDisableLoadConfig(mRootDir, 10, loadConfig));
        ASSERT_TRUE(loadConfig.Match("segment_2_level_0/attribute/price", ""));
        ASSERT_TRUE(loadConfig.Match("segment_2_level_0/index/index2", ""));
        ASSERT_TRUE(loadConfig.Match("segment_2_level_0/attribute/string2/data", ""));
        ASSERT_FALSE(loadConfig.Match("segment_2_level_0/index/nid/data", ""));
        ASSERT_FALSE(loadConfig.Match("segment_2_level_0/attribute/nid/data", ""));
    }

    void TestGetDeployIndexMetaWithDisableAttributeNoRemote()
    {
        DirectoryPtr offlineDir = GET_PARTITION_DIRECTORY()->MakeDirectory("offline");
        DirectoryPtr localDir = GET_PARTITION_DIRECTORY()->MakeDirectory("local");
        DeployIndexMeta remoteDeployMeta, localDeployMeta;
        OnlineConfig onlineConfig;

        IndexFileList baseVersionDeployMeta, targetVersionDeployMeta;
        targetVersionDeployMeta.Append(FileInfo("segment_1_level_0/", -1));
        targetVersionDeployMeta.Append(FileInfo("segment_1_level_0/attribute/", -1));
        targetVersionDeployMeta.Append(FileInfo("segment_1_level_0/attribute/norm_attr_1/", -1));
        targetVersionDeployMeta.Append(FileInfo("segment_1_level_0/attribute/disable_attr_1/", -1));
        targetVersionDeployMeta.Append(FileInfo("segment_1_level_0/attribute/norm_attr_1/data", 10));
        targetVersionDeployMeta.Append(FileInfo("segment_1_level_0/attribute/disable_attr_1/data", 10));
        targetVersionDeployMeta.Append(FileInfo("deploy_meta.2", -1));
        targetVersionDeployMeta.AppendFinal(FileInfo("version.2", 10));
        offlineDir->Store("deploy_meta.2", targetVersionDeployMeta.ToString(), WriterOption());
        offlineDir->Store("version.2", "", WriterOption());

        FromJsonString(onlineConfig, R"( {
            "load_config": [
                {"file_patterns": ["attribute"], "remote": true, "deploy": true }
            ],
            "need_read_remote_index": false,
            "disable_fields": { "attributes": ["disable_attr_1"] }
        } )");

        ASSERT_TRUE(GetDeployIndexMeta(remoteDeployMeta, localDeployMeta, offlineDir->GetOutputPath(),
                                       localDir->GetOutputPath(), 2, INVALID_VERSIONID, onlineConfig, onlineConfig));
        CheckDeployMeta(remoteDeployMeta, {}, {});
        CheckDeployMeta(localDeployMeta,
                        {"segment_1_level_0/", "segment_1_level_0/attribute/",
                         "segment_1_level_0/attribute/norm_attr_1/", "segment_1_level_0/attribute/norm_attr_1/data",
                         "deploy_meta.2"},
                        {"version.2"});
    }

    void TestGetDeployIndexMetaWithDisableAttribute()
    {
        DirectoryPtr offlineDir = GET_PARTITION_DIRECTORY()->MakeDirectory("offline");
        DirectoryPtr localDir = GET_PARTITION_DIRECTORY()->MakeDirectory("local");
        DeployIndexMeta remoteDeployMeta, localDeployMeta;
        OnlineConfig onlineConfig;

        IndexFileList baseVersionDeployMeta, targetVersionDeployMeta;
        targetVersionDeployMeta.Append(FileInfo("segment_1_level_0/", -1));
        targetVersionDeployMeta.Append(FileInfo("segment_1_level_0/attribute/", -1));
        targetVersionDeployMeta.Append(FileInfo("segment_1_level_0/attribute/norm_attr_1/", -1));
        targetVersionDeployMeta.Append(FileInfo("segment_1_level_0/attribute/norm_attr_1/data", 10));
        targetVersionDeployMeta.Append(FileInfo("segment_1_level_0/attribute/disable_attr_1/", -1));
        targetVersionDeployMeta.Append(FileInfo("segment_1_level_0/attribute/disable_attr_1/data", 10));
        targetVersionDeployMeta.Append(FileInfo("deploy_meta.2", -1));
        targetVersionDeployMeta.AppendFinal(FileInfo("version.2", 10));
        offlineDir->Store("deploy_meta.2", targetVersionDeployMeta.ToString(), WriterOption());
        offlineDir->Store("version.2", "", WriterOption());

        FromJsonString(onlineConfig, R"( {
            "load_config": [
                {"file_patterns": ["attribute"], "remote": true, "deploy": true }
            ],
            "need_read_remote_index": true,
            "disable_fields": { "attributes": ["disable_attr_1"] }
        } )");

        ASSERT_TRUE(GetDeployIndexMeta(remoteDeployMeta, localDeployMeta, offlineDir->GetOutputPath(),
                                       localDir->GetOutputPath(), 2, INVALID_VERSIONID, onlineConfig, onlineConfig));
        CheckDeployMeta(remoteDeployMeta,
                        {//"segment_1_level_0/",
                         "segment_1_level_0/attribute/", "segment_1_level_0/attribute/norm_attr_1/",
                         "segment_1_level_0/attribute/norm_attr_1/data"},
                        {}, __FILE__, __LINE__);
        CheckDeployMeta(localDeployMeta,
                        {"segment_1_level_0/", "segment_1_level_0/attribute/",
                         "segment_1_level_0/attribute/norm_attr_1/", "segment_1_level_0/attribute/norm_attr_1/data",
                         "deploy_meta.2"},
                        {"version.2"}, __FILE__, __LINE__);
    }

    void TestGetDeployIndexMetaWithDisableSummary()
    {
        DirectoryPtr offlineDir = GET_PARTITION_DIRECTORY()->MakeDirectory("offline");
        DirectoryPtr localDir = GET_PARTITION_DIRECTORY()->MakeDirectory("local");
        DeployIndexMeta remoteDeployMeta, localDeployMeta;
        OnlineConfig onlineConfig;

        IndexFileList baseVersionDeployMeta, targetVersionDeployMeta;
        targetVersionDeployMeta.Append(FileInfo("segment_1_level_0/", -1));
        targetVersionDeployMeta.Append(FileInfo("segment_1_level_0/summary/", -1));
        targetVersionDeployMeta.Append(FileInfo("segment_1_level_0/summary/data", 10));
        targetVersionDeployMeta.Append(FileInfo("segment_1_level_0/summary/offset", 10));
        targetVersionDeployMeta.Append(FileInfo("deploy_meta.2", -1));
        targetVersionDeployMeta.AppendFinal(FileInfo("version.2", 10));
        offlineDir->Store("deploy_meta.2", targetVersionDeployMeta.ToString(), WriterOption());
        offlineDir->Store("version.2", "", WriterOption());

        FromJsonString(onlineConfig, R"( {
            "load_config": [
                {"file_patterns": ["summary"], "remote": true, "deploy": true }
            ],
            "need_read_remote_index": true,
            "disable_fields": { "summarys": "__SUMMARY_FIELD_ALL__" }
        } )");

        ASSERT_TRUE(GetDeployIndexMeta(remoteDeployMeta, localDeployMeta, offlineDir->GetOutputPath(),
                                       localDir->GetOutputPath(), 2, INVALID_VERSIONID, onlineConfig, onlineConfig));
        CheckDeployMeta(remoteDeployMeta, {}, {}, __FILE__, __LINE__);
        CheckDeployMeta(localDeployMeta, {"deploy_meta.2", "segment_1_level_0/"}, {"version.2"}, __FILE__, __LINE__);
    }

    void TestGetDeployIndexMetaWithDisableSource()
    {
        DirectoryPtr offlineDir = GET_PARTITION_DIRECTORY()->MakeDirectory("offline");
        DirectoryPtr localDir = GET_PARTITION_DIRECTORY()->MakeDirectory("local");
        DeployIndexMeta remoteDeployMeta, localDeployMeta;
        OnlineConfig onlineConfig;

        IndexFileList baseVersionDeployMeta, targetVersionDeployMeta;
        targetVersionDeployMeta.Append(FileInfo("segment_1_level_0/", -1));
        targetVersionDeployMeta.Append(FileInfo("segment_1_level_0/source/", -1));
        targetVersionDeployMeta.Append(FileInfo("segment_1_level_0/source/data", 10));
        targetVersionDeployMeta.Append(FileInfo("segment_1_level_0/source/offset", 10));
        targetVersionDeployMeta.Append(FileInfo("deploy_meta.2", -1));
        targetVersionDeployMeta.AppendFinal(FileInfo("version.2", 10));
        offlineDir->Store("deploy_meta.2", targetVersionDeployMeta.ToString(), WriterOption());
        offlineDir->Store("version.2", "", WriterOption());

        FromJsonString(onlineConfig, R"( {
            "load_config": [
                {"file_patterns": ["summary"], "remote": true, "deploy": true }
            ],
            "need_read_remote_index": true,
            "disable_fields": { "sources": "__SOURCE_FIELD_ALL__" }
        } )");

        ASSERT_TRUE(GetDeployIndexMeta(remoteDeployMeta, localDeployMeta, offlineDir->GetOutputPath(),
                                       localDir->GetOutputPath(), 2, INVALID_VERSIONID, onlineConfig, onlineConfig));
        CheckDeployMeta(remoteDeployMeta, {}, {}, __FILE__, __LINE__);
        CheckDeployMeta(localDeployMeta, {"deploy_meta.2", "segment_1_level_0/"}, {"version.2"}, __FILE__, __LINE__);
    }

    void TestGetDeployIndexMetaWithDisableSourceGroup()
    {
        DirectoryPtr offlineDir = GET_PARTITION_DIRECTORY()->MakeDirectory("offline");
        DirectoryPtr localDir = GET_PARTITION_DIRECTORY()->MakeDirectory("local");
        DeployIndexMeta remoteDeployMeta, localDeployMeta;
        OnlineConfig onlineConfig;

        IndexFileList baseVersionDeployMeta, targetVersionDeployMeta;
        targetVersionDeployMeta.Append(FileInfo("segment_1_level_0/", -1));
        targetVersionDeployMeta.Append(FileInfo("segment_1_level_0/source/", -1));
        targetVersionDeployMeta.Append(FileInfo("segment_1_level_0/source/group_1/", -1));
        targetVersionDeployMeta.Append(FileInfo("segment_1_level_0/source/group_1/data", 10));
        targetVersionDeployMeta.Append(FileInfo("segment_1_level_0/source/group_1/offset", 8));
        targetVersionDeployMeta.Append(FileInfo("segment_1_level_0/source/group_0/", -1));
        targetVersionDeployMeta.Append(FileInfo("segment_1_level_0/source/group_0/data", 9));
        targetVersionDeployMeta.Append(FileInfo("segment_1_level_0/source/group_0/offset", 6));

        targetVersionDeployMeta.Append(FileInfo("deploy_meta.2", -1));
        targetVersionDeployMeta.AppendFinal(FileInfo("version.2", 10));
        offlineDir->Store("deploy_meta.2", targetVersionDeployMeta.ToString(), WriterOption());
        offlineDir->Store("version.2", "", WriterOption());

        FromJsonString(onlineConfig, R"( {
            "load_config": [
                {"file_patterns": ["summary"], "remote": true, "deploy": true }
            ],
            "need_read_remote_index": true,
            "disable_fields": { "sources": "__SOURCE_FIELD_GROUP__:0" }
        } )");

        ASSERT_TRUE(GetDeployIndexMeta(remoteDeployMeta, localDeployMeta, offlineDir->GetOutputPath(),
                                       localDir->GetOutputPath(), 2, INVALID_VERSIONID, onlineConfig, onlineConfig));
        CheckDeployMeta(remoteDeployMeta, {}, {});
        CheckDeployMeta(localDeployMeta,
                        {"segment_1_level_0/", "segment_1_level_0/source/", "segment_1_level_0/source/group_1/",
                         "segment_1_level_0/source/group_1/data", "segment_1_level_0/source/group_1/offset",
                         "deploy_meta.2"},
                        {"version.2"});
    }

    void TestLogicalFileSystem()
    {
        std::map<std::string, std::map<std::string, std::string>> version1Files;
        std::map<std::string, std::map<std::string, std::string>> version2Files;

        auto rootPath = util::PathUtil::JoinPath(mRootDir, "LogicalFilesystemRoot");
        version1Files.insert({rootPath, {{"f1", "f1"}, {"f2", "f2"}}});
        version2Files.insert({rootPath, {{"f1", "f1"}, {"f2", "f3"}}});

        CreateLogicalFileSystem(1, rootPath, version1Files);
        CreateLogicalFileSystem(2, rootPath, version2Files);

        config::OnlineConfig onlineConfig;
        DeployIndexWrapper::GetDeployIndexMetaInputParams params;
        params.rawPath = rootPath;
        params.localPath = rootPath;
        params.baseOnlineConfig = &onlineConfig;
        params.targetOnlineConfig = &onlineConfig;
        params.targetVersionId = 2;
        params.baseVersionId = 1;
        DeployIndexMetaVec localDeployIndexMetaVec;
        DeployIndexMetaVec remoteDeployIndexMetaVec;
        ASSERT_TRUE(DeployIndexWrapper::GetDeployIndexMeta(params, localDeployIndexMetaVec, remoteDeployIndexMetaVec));
        ASSERT_EQ(1, localDeployIndexMetaVec.size());
        ASSERT_EQ(0, remoteDeployIndexMetaVec.size());
        ASSERT_EQ(rootPath, localDeployIndexMetaVec[0]->sourceRootPath);
        ASSERT_EQ(rootPath, localDeployIndexMetaVec[0]->targetRootPath);
        ASSERT_EQ(3, localDeployIndexMetaVec[0]->deployFileMetas.size());
        ASSERT_EQ("f3", localDeployIndexMetaVec[0]->deployFileMetas[2].filePath);
        ASSERT_EQ("entry_table.2", localDeployIndexMetaVec[0]->deployFileMetas[1].filePath);
        ASSERT_EQ("deploy_meta.2", localDeployIndexMetaVec[0]->deployFileMetas[0].filePath);
        // for (auto& item : localDeployIndexMetaVec)
        // {
        //     std::cout << "-------------- root:" << item->sourceRootPath << ":" << item->targetRootPath << "
        //     --------------" << std::endl; for (auto& fileInfo : item->deployFileMetas)
        //     {
        //         std::cout << fileInfo.filePath << std::endl;
        //     }
        // }
    }

private:
    void CreateLogicalFileSystem(int version, const std::string& root,
                                 const std::map<std::string, std::map<std::string, std::string>>& versionFiles)
    {
        std::string entryMeta = R"({"files":{{files}},"dirs": []})";
        std::string entryFileName = EntryTable::GetEntryTableFileName(version);
        string deployMetaFileName = DeployIndexWrapper::GetDeployMetaFileName(version);
        IndexFileList deployMeta;
        Version vs;
        vs.SetVersionId(version);

        int i = 0;
        std::string replaceInTemplate;
        for (auto& item : versionFiles) {
            if (i > 0) {
                replaceInTemplate.append(",");
            }
            replaceInTemplate.append("\"");
            replaceInTemplate.append(item.first);
            replaceInTemplate.append("\": {");
            int j = 0;
            auto entries = item.second;
            if (i == 0) {
                entries.insert({deployMetaFileName, deployMetaFileName});
                entries.insert({vs.GetSchemaFileName(), vs.GetSchemaFileName()});
            }
            for (auto& entry : entries) {
                if (j > 0) {
                    replaceInTemplate.append(",");
                }
                char buffer[10240];
                snprintf(buffer, sizeof(buffer), "\"%s\":{\"path\": \"%s\", \"length\":0}", entry.first.c_str(),
                         entry.second.c_str());
                replaceInTemplate.append(buffer);
                deployMeta.Append(FileInfo(entry.first, 0));
                ++j;
            }
            replaceInTemplate.append("}");
            ++i;
        }

        StringUtil::replaceAll(entryMeta, "{files}", replaceInTemplate);
        ASSERT_EQ(FSEC_OK, FslibWrapper::AtomicStore(util::PathUtil::JoinPath(root, entryFileName), entryMeta).Code());
        ASSERT_EQ(FSEC_OK,
                  FslibWrapper::AtomicStore(util::PathUtil::JoinPath(root, deployMetaFileName), deployMeta.ToString())
                      .Code());
        ASSERT_EQ(
            FSEC_OK,
            FslibWrapper::AtomicStore(util::PathUtil::JoinPath(root, vs.GetVersionFileName()), vs.ToString()).Code());
        if (!FslibWrapper::IsExist(util::PathUtil::JoinPath(root, vs.GetSchemaFileName())).GetOrThrow()) {
            ASSERT_EQ(FSEC_OK,
                      FslibWrapper::AtomicStore(util::PathUtil::JoinPath(root, vs.GetSchemaFileName()), "").Code());
        }
    }

private:
    string mRootDir;
    OnlineConfig mOnlineConfig;

private:
    IE_LOG_DECLARE();
};

IE_LOG_SETUP(index_base, DeployIndexWrapperTest);

INDEXLIB_UNIT_TEST_CASE(DeployIndexWrapperTest, TestDumpDeployIndex);
INDEXLIB_UNIT_TEST_CASE(DeployIndexWrapperTest, TestDumpDeployIndexWithAtomicStoreFail);
INDEXLIB_UNIT_TEST_CASE(DeployIndexWrapperTest, TestDumpTruncateMetaIndex);
INDEXLIB_UNIT_TEST_CASE(DeployIndexWrapperTest, TestDumpSegmentDeployIndex);
INDEXLIB_UNIT_TEST_CASE(DeployIndexWrapperTest, TestGetFilesAbnormal);
INDEXLIB_UNIT_TEST_CASE(DeployIndexWrapperTest, TestGetDeployFiles);
INDEXLIB_UNIT_TEST_CASE(DeployIndexWrapperTest, TestGetDeployFilesByConfig);
INDEXLIB_UNIT_TEST_CASE(DeployIndexWrapperTest, TestGetDeployFilesWithSubIndexPartition);
INDEXLIB_UNIT_TEST_CASE(DeployIndexWrapperTest, TestDumpSegmentDeployIndexWithDirectoryAndPackageFile);
INDEXLIB_UNIT_TEST_CASE(DeployIndexWrapperTest, TestSimpleProcess);
INDEXLIB_UNIT_TEST_CASE(DeployIndexWrapperTest, TestDumpDeployIndexWithLength);
INDEXLIB_UNIT_TEST_CASE(DeployIndexWrapperTest, TestGetDeployIndexMeta);
INDEXLIB_UNIT_TEST_CASE(DeployIndexWrapperTest, TestGetDeployIndexMetaBaseVersionInvalid);
INDEXLIB_UNIT_TEST_CASE(DeployIndexWrapperTest, TestGetDeployIndexMetaBaseVersionLost);
INDEXLIB_UNIT_TEST_CASE(DeployIndexWrapperTest, TestGetDeployIndexMetaComplex);
INDEXLIB_UNIT_TEST_CASE(DeployIndexWrapperTest, TestGetDeployIndexMetaDeployMetaNotExist);
INDEXLIB_UNIT_TEST_CASE(DeployIndexWrapperTest, TestGetDeployIndexMetaVersionFile);
INDEXLIB_UNIT_TEST_CASE(DeployIndexWrapperTest, TestGenerateDisableLoadConfig);
INDEXLIB_UNIT_TEST_CASE(DeployIndexWrapperTest, TestDeployIndexMetaWithOngoingOps);
INDEXLIB_UNIT_TEST_CASE(DeployIndexWrapperTest, TestGetDeployIndexMetaWithDisableAttribute);
INDEXLIB_UNIT_TEST_CASE(DeployIndexWrapperTest, TestGetDeployIndexMetaWithDisableAttributeNoRemote);
INDEXLIB_UNIT_TEST_CASE(DeployIndexWrapperTest, TestGetDeployIndexMetaWithDisableSummary);
INDEXLIB_UNIT_TEST_CASE(DeployIndexWrapperTest, TestGetDeployIndexMetaWithDisableSource);
INDEXLIB_UNIT_TEST_CASE(DeployIndexWrapperTest, TestGetDeployIndexMetaWithDisableSourceGroup);
INDEXLIB_UNIT_TEST_CASE(DeployIndexWrapperTest, TestGetDeployFilesWithLifecycle);

INDEXLIB_UNIT_TEST_CASE(DeployIndexWrapperTest, TestLogicalFileSystem);
}} // namespace indexlib::index_base
