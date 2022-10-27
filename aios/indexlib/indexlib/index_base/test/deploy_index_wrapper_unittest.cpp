#include <sys/types.h>
#include <utime.h>
#include <autil/StringUtil.h>
#include "indexlib/common_define.h"
#include "indexlib/test/unittest.h"
#include "indexlib/test/partition_data_maker.h"
#include "indexlib/index_base/deploy_index_wrapper.h"
#include "indexlib/index_base/index_meta/version.h"
#include "indexlib/index_base/index_meta/segment_info.h"
#include "indexlib/index_base/index_meta/segment_file_list_wrapper.h"
#include "indexlib/index_base/version_loader.h"
#include "indexlib/index_define.h"
#include "indexlib/storage/file_system_wrapper.h"
#include "indexlib/file_system/indexlib_file_system_impl.h"
#include "indexlib/file_system/in_mem_directory.h"
#include "indexlib/file_system/test/package_file_util.h"
#include "indexlib/config/load_config_list.h"
#include "indexlib/config/online_config.h"
#include "indexlib/util/path_util.h"
#include "indexlib/file_system/test/package_file_util.h"
#include "indexlib/file_system/test/load_config_list_creator.h"

using namespace std;
using namespace autil;
using namespace autil::legacy;
using namespace fslib;
using namespace fslib::fs;
using testing::UnorderedElementsAreArray;

IE_NAMESPACE_USE(test);
IE_NAMESPACE_USE(storage);
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(util);
IE_NAMESPACE_USE(file_system);

IE_NAMESPACE_BEGIN(index_base);
class DeployIndexWrapperTest : public INDEXLIB_TESTBASE
{
public:
    DECLARE_CLASS_NAME(DeployIndexWrapperTest);

public:
    void CaseSetUp() override
    {
        mRootDir = GET_TEST_DATA_PATH();
    }

    void CaseTearDown() override
    {
    }

    // filePaths : file1;file2;file3
    void CreateFiles(const string& dir, const string& filePaths)
    {
        vector<string> fileNames;
        fileNames = StringUtil::split(filePaths, ";");
        for (size_t i = 0; i < fileNames.size(); ++i)
        {
            string filePath = FileSystemWrapper::JoinPath(dir, fileNames[i]);
            FileSystemWrapper::AtomicStore(filePath, "");
        }
    }

    void TestDumpDeployIndex()
    {
        string testPath = FileSystemWrapper::JoinPath(mRootDir, "DumpDeployIndex");
        {
            //normal case
            CreateFiles(testPath, "file1;file2;file3");
            DeployIndexWrapper deployIndexWrapper(testPath, "", mOnlineConfig);
            
            SegmentFileListWrapper::Dump(testPath);
        
            string deployIndexFile = FileSystemWrapper::JoinPath(
                    testPath, SEGMENT_FILE_LIST);
            INDEXLIB_TEST_TRUE(FileSystemWrapper::IsExist(deployIndexFile));

            // assure list from SEGMENT_FILE_LIST
            FileSystemWrapper::DeleteFile(PathUtil::JoinPath(testPath, "file1"));

            FileList tmpList;
            int64_t totalLength = 0;
            deployIndexWrapper.GetFiles("", tmpList, totalLength);
            INDEXLIB_TEST_EQUAL((size_t)4, tmpList.size());
            sort(tmpList.begin(), tmpList.end());
            //INDEXLIB_TEST_EQUAL(string("deploy_index"), tmpList[0]);
            INDEXLIB_TEST_EQUAL(string("file1"), tmpList[0]);
            INDEXLIB_TEST_EQUAL(string("file2"), tmpList[1]);
            INDEXLIB_TEST_EQUAL(string("file3"), tmpList[2]);
            INDEXLIB_TEST_EQUAL(string("segment_file_list"), tmpList[3]);
            FileSystemWrapper::DeleteDir(testPath);
        }
        {
            //path not exist
            string tmpPath = FileSystemWrapper::JoinPath(testPath, "pathNotExist");
            INDEXLIB_TEST_TRUE(
                    !SegmentFileListWrapper::Dump(tmpPath));
        }
        {
            //deploy index already exist
            CreateFiles(testPath, SEGMENT_FILE_LIST);
            INDEXLIB_TEST_TRUE(
                    SegmentFileListWrapper::Dump(testPath));
        }

    }

    void TestDumpTruncateMetaIndex()
    {
        string testPath = FileSystemWrapper::JoinPath(mRootDir, TRUNCATE_META_DIR_NAME);
        CreateFiles(testPath, "file1;file2;file3");
        DeployIndexWrapper deployIndexWrapper(mRootDir, "", mOnlineConfig);
        deployIndexWrapper.DumpTruncateMetaIndex();
        string deployIndexFile = FileSystemWrapper::JoinPath(
                testPath, SEGMENT_FILE_LIST);
        INDEXLIB_TEST_TRUE(FileSystemWrapper::IsExist(deployIndexFile));
        FileList tmpList;
        int64_t totalLength = 0;
        deployIndexWrapper.GetFiles(TRUNCATE_META_DIR_NAME, tmpList, totalLength);
        ASSERT_THAT(tmpList, UnorderedElementsAre(
                        "truncate_meta/file1",
                        "truncate_meta/file2",
                        "truncate_meta/file3",
                        "truncate_meta/segment_file_list"));
        FileSystemWrapper::DeleteDir(testPath);
    }

    void TestDumpSegmentDeployIndexWithDirectory()
    {
        file_system::DirectoryPtr directory = GET_PARTITION_DIRECTORY();
        file_system::DirectoryPtr segDirectory = MAKE_SEGMENT_DIRECTORY(0);
        file_system::IndexlibFileSystemPtr fileSystem = GET_FILE_SYSTEM();
        
        fileSystem->Sync(true);
        CreateFiles(segDirectory->GetPath(), "file1;file2;file3");

        SegmentInfoPtr segInfo(new SegmentInfo);
        segInfo->docCount = 100;
        
        DeployIndexWrapper::DumpSegmentDeployIndex(segDirectory, segInfo);

        fileSystem->Sync(true);
        INDEXLIB_TEST_TRUE(segDirectory->IsExist(SEGMENT_FILE_LIST));

        FileList tmpList;
        int64_t totalLength = 0;
        DeployIndexWrapper deployIndexWrapper(mRootDir, "", mOnlineConfig);
        deployIndexWrapper.GetFiles("segment_0_level_0", tmpList, totalLength);

        ASSERT_THAT(tmpList, UnorderedElementsAre(
                        string("segment_0_level_0/file1"),
                        string("segment_0_level_0/file2"),
                        string("segment_0_level_0/file3"),
                        string("segment_0_level_0/segment_file_list"),
                        string("segment_0_level_0/segment_info")));
        // total length should include segment_info
        ASSERT_EQ(segInfo->ToString().size(), totalLength);
    }

    void TestDumpSegmentDeployIndexWithDirectoryAndPackageFile()
    {
        file_system::DirectoryPtr directory = GET_PARTITION_DIRECTORY();
        file_system::DirectoryPtr segDirectory = MAKE_SEGMENT_DIRECTORY(0);
        file_system::IndexlibFileSystemPtr fileSystem = GET_FILE_SYSTEM();
        fileSystem->Sync(true);
        CreateFiles(segDirectory->GetPath(), "file1;file2;file3;index/file4");
        PackageFileUtil::CreatePackageFile(
                segDirectory, "package_file", 
                "file5:abc#index/file6:abc#attribute/file7:abc", "");
        fileSystem->Sync(true);
        ASSERT_TRUE(fileSystem->MountPackageFile(
                        PathUtil::JoinPath(segDirectory->GetPath(), "package_file")));
        SegmentInfoPtr segInfo(new SegmentInfo);
        segInfo->docCount = 100;
        DeployIndexWrapper::DumpSegmentDeployIndex(segDirectory, segInfo);
        fileSystem->Sync(true);
        INDEXLIB_TEST_TRUE(segDirectory->IsExist(SEGMENT_FILE_LIST));

        FileList tmpList;
        int64_t totalLength = 0;
        DeployIndexWrapper deployIndexWrapper(mRootDir, "", mOnlineConfig);
        deployIndexWrapper.GetFiles("segment_0_level_0", tmpList, totalLength);

        ASSERT_THAT(tmpList, ElementsAre(
                        string("segment_0_level_0/file1"),
                        string("segment_0_level_0/file2"),
                        string("segment_0_level_0/file3"),
                        string("segment_0_level_0/index/"),
                        string("segment_0_level_0/index/file4"),
                        string("segment_0_level_0/package_file.__data__0"),
                        string("segment_0_level_0/package_file.__meta__"),                        
                        string("segment_0_level_0/segment_info"),
                        string("segment_0_level_0/segment_file_list")));

        ASSERT_EQ(8195, FileSystemWrapper::GetFileLength(mRootDir + "segment_0_level_0/package_file.__data__0"));
        ASSERT_EQ(895, FileSystemWrapper::GetFileLength(mRootDir + "segment_0_level_0/package_file.__meta__"));
        ASSERT_EQ(162, segInfo->ToString().size());
        // total length should include package_file.__data__0 & package_file.__meta__ & segment_info
        ASSERT_EQ(8195 + 895 + segInfo->ToString().size(), totalLength);
    }

    void TestDumpSegmentDeployIndex()
    {
        {
            string testPath = FileSystemWrapper::JoinPath(mRootDir, "segment_0_level_0");
            CreateFiles(testPath, "file1;file2;file3");
            DeployIndexWrapper deployIndexWrapper(mRootDir, "", mOnlineConfig);
            deployIndexWrapper.DumpSegmentDeployIndex("segment_0_level_0");
            string deployIndexFile = FileSystemWrapper::JoinPath(
                    testPath, SEGMENT_FILE_LIST);
            INDEXLIB_TEST_TRUE(FileSystemWrapper::IsExist(deployIndexFile));
            FileList tmpList;
            int64_t totalLength = 0;
            deployIndexWrapper.GetFiles("segment_0_level_0", tmpList, totalLength);
            ASSERT_THAT(tmpList, UnorderedElementsAre(
                            "segment_0_level_0/file1",
                            "segment_0_level_0/file2",
                            "segment_0_level_0/file3",
                            "segment_0_level_0/segment_file_list"));
            FileSystemWrapper::DeleteDir(testPath);
        }
    }
    
    void TestGetFilesAbnormal()
    {
        //path not exist
        {
            string testPath = FileSystemWrapper::JoinPath(mRootDir, "pathNotExist");
            FileList fileList;
            int64_t totalLength = 0;
            DeployIndexWrapper deployIndexWrapper(mRootDir, "", mOnlineConfig);
            INDEXLIB_TEST_TRUE(!deployIndexWrapper.GetFiles("", fileList, totalLength));
                    
        }
        //index file not exist
        {
            string testPath = FileSystemWrapper::JoinPath(mRootDir, "tetGetFiles");
            CreateFiles(testPath, "file1;file2;file3");
            DeployIndexWrapper deployIndexWrapper(testPath, "", mOnlineConfig);

            FileList tmpList;
            int64_t totalLength = 0;
            ASSERT_FALSE(deployIndexWrapper.GetFiles("", tmpList, totalLength));
        }
    }

    void TestGetTruncateMetaDeployFiles()
    {
        {
            //normal case
            string testPath = FileSystemWrapper::JoinPath(mRootDir, TRUNCATE_META_DIR_NAME);
            CreateFiles(testPath, "file1;file2;file3");
            DeployIndexWrapper deployIndexWrapper(mRootDir, "", mOnlineConfig);
            deployIndexWrapper.DumpTruncateMetaIndex();

            FileList tmpList;
            int64_t totalLength = 0;
            deployIndexWrapper.GetTruncateMetaDeployFiles(tmpList, totalLength);
            ASSERT_THAT(tmpList, UnorderedElementsAre(
                            "truncate_meta/",
                            "truncate_meta/file1",
                            "truncate_meta/file2",
                            "truncate_meta/file3",
                            "truncate_meta/segment_file_list"));
            FileSystemWrapper::DeleteDir(testPath);
        }
        {
            //abnormal case
            FileList tmpList;
            int64_t totalLength = 0;
            DeployIndexWrapper deployIndexWrapper(mRootDir, "", mOnlineConfig);
            deployIndexWrapper.GetTruncateMetaDeployFiles(tmpList, totalLength);
            INDEXLIB_TEST_EQUAL((size_t)0, tmpList.size());
        }
    }

    void TestGetDiffSegmentIds()
    {
        string versionPath = FileSystemWrapper::JoinPath(mRootDir, "version.0");
        string content = "{\"segments\":[0,1],\"versionid\":0}";
        FileSystemWrapper::AtomicStore(versionPath, content);
        
        {
            Version newVersion;
            VersionLoader::GetVersion(versionPath, newVersion);
                    
            //last version invalid
            DeployIndexWrapper deployIndexWrapper(mRootDir, "", mOnlineConfig);
            vector<segmentid_t> segIds;
            versionid_t newVersionId = 0;
            versionid_t lastVersionId = INVALID_VERSION;
            ASSERT_TRUE(deployIndexWrapper.GetDiffSegmentIds(segIds, newVersion, newVersionId, lastVersionId));
            sort(segIds.begin(), segIds.end());
            INDEXLIB_TEST_EQUAL((size_t)2, segIds.size());
            INDEXLIB_TEST_EQUAL(0, segIds[0]);
            INDEXLIB_TEST_EQUAL(1, segIds[1]);
        }
        {
            Version newVersion;
            VersionLoader::GetVersion(versionPath, newVersion);

            //last version not exist
            DeployIndexWrapper deployIndexWrapper(mRootDir, "", mOnlineConfig);
            vector<segmentid_t> segIds;
            versionid_t newVersionId = 0;
            versionid_t lastVersionId = -3;
            ASSERT_TRUE(deployIndexWrapper.GetDiffSegmentIds(segIds, newVersion, newVersionId, lastVersionId));
            INDEXLIB_TEST_EQUAL((size_t)2, segIds.size());
            INDEXLIB_TEST_EQUAL(0, segIds[0]);
            INDEXLIB_TEST_EQUAL(1, segIds[1]);
        }
        {
            //normal
            string versionPathForNew = FileSystemWrapper::JoinPath(mRootDir, "version.1");
            content = "{\"segments\":[0,1,2],\"versionid\":1}";
            FileSystemWrapper::AtomicStore(versionPathForNew, content);

            Version newVersion;
            VersionLoader::GetVersion(versionPathForNew, newVersion);
            DeployIndexWrapper deployIndexWrapper(mRootDir, "", mOnlineConfig);
            vector<segmentid_t> segIds;
            versionid_t newVersionId = 1;
            versionid_t lastVersionId = 0;
            ASSERT_TRUE(deployIndexWrapper.GetDiffSegmentIds(segIds, newVersion, newVersionId, lastVersionId));
            INDEXLIB_TEST_EQUAL((size_t)1, segIds.size());
            INDEXLIB_TEST_EQUAL(2, segIds[0]);
        }
    }
    
    void TestGetSegmentDeployFiles()
    {
        {   //normal case
            Version version;
            string testPath = FileSystemWrapper::JoinPath(mRootDir, "segment_0_level_0");
            CreateFiles(testPath, "file1;file2;file3");
            DeployIndexWrapper deployIndexWrapper(mRootDir, "", mOnlineConfig);
            deployIndexWrapper.DumpSegmentDeployIndex("segment_0_level_0");
            vector<segmentid_t> segIds;
            segIds.push_back(0);
            version.AddSegment(0);
            FileList tmpList;
            int64_t totalLength = 0;
            bool result = deployIndexWrapper.GetSegmentDeployFiles(version, segIds, tmpList, totalLength);
            INDEXLIB_TEST_TRUE(result);
            INDEXLIB_TEST_EQUAL((size_t)4, tmpList.size());
            sort(tmpList.begin(), tmpList.end());
            INDEXLIB_TEST_EQUAL(string("segment_0_level_0/segment_file_list"), tmpList[3]);
            INDEXLIB_TEST_EQUAL(string("segment_0_level_0/file1"), tmpList[0]);
            INDEXLIB_TEST_EQUAL(string("segment_0_level_0/file2"), tmpList[1]);
            INDEXLIB_TEST_EQUAL(string("segment_0_level_0/file3"), tmpList[2]);
            FileSystemWrapper::DeleteDir(testPath);
        }
        {
            //abnormal case
            Version version;
            string testPath = FileSystemWrapper::JoinPath(mRootDir, "segment_0_level_0");
            CreateFiles(testPath, "file1;file2;file3");
            DeployIndexWrapper deployIndexWrapper(mRootDir, "", mOnlineConfig);
            deployIndexWrapper.DumpSegmentDeployIndex("segment_0_level_0");
            vector<segmentid_t> segIds;
            segIds.push_back(0);
            segIds.push_back(1);
            version.AddSegment(0);
            version.AddSegment(1);            
            FileList tmpList;
            int64_t totalLength = 0;
            bool result = deployIndexWrapper.GetSegmentDeployFiles(version, segIds, tmpList, totalLength);
            INDEXLIB_TEST_TRUE(!result);
            FileSystemWrapper::DeleteDir(testPath);
        }
    }

    void TestGetDeployFiles()    
    {
        //InnerTestGetDeployFiles(false);
        InnerTestGetDeployFiles(true);
    }

    void InnerTestGetDeployFiles(bool exception)
    {
        {
            TearDown();
            SetUp();

            //lastVersion is invalid
            string testPath = FileSystemWrapper::JoinPath(mRootDir, "segment_0_level_0");
            CreateFiles(testPath, "file1;file2;file3");

            string versionPath = FileSystemWrapper::JoinPath(mRootDir, "version.0");
            string content = "{\"segments\":[0],\"versionid\":0, \"format_version\": 1}";
            FileSystemWrapper::AtomicStore(versionPath, content);

            FileList tmpList;
            DeployIndexWrapper deployIndexWrapper(mRootDir, "", mOnlineConfig);
            deployIndexWrapper.DumpSegmentDeployIndex("segment_0_level_0");
            if (exception)
            {
                FileSystemWrapper::SetError(
                        FileSystemWrapper::OPEN_ERROR, versionPath);
            }
            bool result = deployIndexWrapper.GetDeployFiles(tmpList, 0);

            if (exception)
            {
                ASSERT_FALSE(result);
            }
            else
            {
                INDEXLIB_TEST_TRUE(result);
                EXPECT_THAT(tmpList, UnorderedElementsAre(
                                INDEX_FORMAT_VERSION_FILE_NAME,
                                index::SCHEMA_FILE_NAME,
                                string("segment_0_level_0/")  + SEGMENT_FILE_LIST,
                                "segment_0_level_0/file1",
                                "segment_0_level_0/file2",
                                "segment_0_level_0/file3"));
            }
        }

        {
            TearDown();
            SetUp();
            //lastVersion is valid
            string testPath = FileSystemWrapper::JoinPath(mRootDir, "segment_0_level_0");
            CreateFiles(testPath, "file1;file2;file3");
            string testPath2 = FileSystemWrapper::JoinPath(mRootDir, "segment_0");
            CreateFiles(testPath2, "file1;file2;file3");
            string lastVersionPath = FileSystemWrapper::JoinPath(mRootDir, "version.0");
            string content = "{\"segments\":[1,2],\"versionid\":0, \"format_version\": 1}";
            FileSystemWrapper::AtomicStore(lastVersionPath, content);

            string newVersionPath = FileSystemWrapper::JoinPath(mRootDir, "version.1");
            content = "{\"segments\":[0,1,2],\"versionid\":1, \"format_version\": 1}";
            FileSystemWrapper::AtomicStore(newVersionPath, content);

            FileList tmpList;
            DeployIndexWrapper deployIndexWrapper(mRootDir, "", mOnlineConfig);
            deployIndexWrapper.DumpSegmentDeployIndex("segment_0_level_0");
            if (exception)
            {
                FileSystemWrapper::SetError(
                        FileSystemWrapper::OPEN_ERROR, testPath + "/segment_file_list");
                FileSystemWrapper::SetError(
                        FileSystemWrapper::OPEN_ERROR, testPath + "/deploy_index");

            }
            ASSERT_FALSE(deployIndexWrapper.GetDeployFiles(tmpList, 0, 1));
            bool result = deployIndexWrapper.GetDeployFiles(tmpList, 1, 0);
            if (exception)
            {
                ASSERT_FALSE(result);
            }
            else
            {
                INDEXLIB_TEST_TRUE(result);
                EXPECT_THAT(tmpList, UnorderedElementsAre(
                                string("segment_0_level_0") + "/" + SEGMENT_FILE_LIST,
                                "segment_0_level_0/file1",
                                "segment_0_level_0/file2",
                                "segment_0_level_0/file3"));
            }
        }
    }

    void TestGetDeployFilesByConfig()
    {
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
        mOnlineConfig.loadConfigList =
            LoadConfigListCreator::CreateLoadConfigListFromJsonString(loadConfigStr);

        //lastVersion is invalid
        string testPath = FileSystemWrapper::JoinPath(mRootDir, "segment_0_level_0");
        CreateFiles(testPath, "file1;file2;file3;dir4/file4");

        string dirPath0 = FileSystemWrapper::JoinPath(testPath, "summary");
        FileSystemWrapper::MkDir(dirPath0);

        string dirPath1 = FileSystemWrapper::JoinPath(testPath, "deletionmap");
        FileSystemWrapper::MkDir(dirPath1);

        string dirPath2 = FileSystemWrapper::JoinPath(testPath, "attribute");
        FileSystemWrapper::MkDir(dirPath2);

        string versionPath = FileSystemWrapper::JoinPath(mRootDir, "version.0");
        string content = "{\"segments\":[0],\"versionid\":0, \"format_version\": 1}";
        FileSystemWrapper::AtomicStore(versionPath, content);
        {
            DeployIndexWrapper deployIndexWrapper(mRootDir, "");
            deployIndexWrapper.DumpSegmentDeployIndex("segment_0_level_0");
        }

        FileSystemWrapper::AtomicStore(FileSystemWrapper::JoinPath(
                        mRootDir, INDEX_FORMAT_VERSION_FILE_NAME), "");
        FileSystemWrapper::AtomicStore(FileSystemWrapper::JoinPath(
                                           mRootDir, index::SCHEMA_FILE_NAME), "");

        {
            // legacy index, no deploy_meta
            FileList tmpList;
            DeployIndexWrapper deployIndexWrapper(mRootDir, "", mOnlineConfig);

            bool result = deployIndexWrapper.GetDeployFiles(tmpList, 0);

            INDEXLIB_TEST_TRUE(result);
            EXPECT_THAT(tmpList, UnorderedElementsAre(
                            INDEX_FORMAT_VERSION_FILE_NAME,
                            index::SCHEMA_FILE_NAME,
                            string("segment_0_level_0/")  + SEGMENT_FILE_LIST,
                            "segment_0_level_0/file1",
                            "segment_0_level_0/file2",
                            "segment_0_level_0/deletionmap/",
                            "segment_0_level_0/dir4/file4"));
        }
        {
            // with deploy_meta
            {
                Version v;
                v.Load(versionPath);
                DeployIndexWrapper::DumpDeployIndexMeta(mRootDir, v);
            }

            FileList tmpList;
            DeployIndexWrapper deployIndexWrapper(mRootDir, "", mOnlineConfig);

            bool result = deployIndexWrapper.GetDeployFiles(tmpList, 0);

            INDEXLIB_TEST_TRUE(result);
            EXPECT_THAT(tmpList, UnorderedElementsAre(
                            INDEX_FORMAT_VERSION_FILE_NAME,
                            index::SCHEMA_FILE_NAME,
                            // for now both DEPLOY_INDEX_FILE_NAME and SEGMENT_FILE_LIST exist
                            string("segment_0_level_0/")  + DEPLOY_INDEX_FILE_NAME,
                            string("segment_0_level_0/")  + SEGMENT_FILE_LIST,
                            "segment_0_level_0/",
                            "segment_0_level_0/file1",
                            "segment_0_level_0/file2",
                            "segment_0_level_0/deletionmap/",
                            "segment_0_level_0/dir4/file4",
                            "deploy_meta.0"));
        }
    }

    void TestSimpleProcess()
    {
        //lastVersion is invalid
        string oldRoot = FileSystemWrapper::JoinPath(mRootDir, "oldPath");
        string newRoot = FileSystemWrapper::JoinPath(mRootDir, "newPath");
        Version version;
        version.SetFormatVersion(0);

        CreateFiles(FileSystemWrapper::JoinPath(oldRoot, "segment_0_level_0"), "file1;file2;file3");
        CreateFiles(FileSystemWrapper::JoinPath(newRoot, "segment_0_level_0"), "file1;file2;file3");
        CreateFiles(FileSystemWrapper::JoinPath(newRoot, "segment_2_level_0"), "file1;file2;file5");

        string oldVersionPath = FileSystemWrapper::JoinPath(oldRoot, "version.0");
        string newVersionPath = FileSystemWrapper::JoinPath(newRoot, "version.2");
        string content = "{\"segments\":[0],\"versionid\":0, \"format_version\": 1}";
        FileSystemWrapper::AtomicStore(oldVersionPath, content);
        content = "{\"segments\":[0, 2],\"versionid\":2, \"format_version\": 1}";
        FileSystemWrapper::AtomicStore(newVersionPath, content);

        FileList tmpList;
        Version newVersion;
        newVersion.Load(newVersionPath);

        DeployIndexWrapper deployIndexWrapper(newRoot, oldRoot, mOnlineConfig);
        deployIndexWrapper.DumpSegmentDeployIndex(newVersion.GetSegmentDirName(0));
        deployIndexWrapper.DumpSegmentDeployIndex(newVersion.GetSegmentDirName(0));
        deployIndexWrapper.DumpSegmentDeployIndex(newVersion.GetSegmentDirName(2));

        bool result = deployIndexWrapper.GetDeployFiles(tmpList, 2, 0);

        INDEXLIB_TEST_TRUE(result);
        EXPECT_THAT(tmpList, UnorderedElementsAre(
                        newVersion.GetSegmentDirName(2) + "/" + SEGMENT_FILE_LIST,
                        newVersion.GetSegmentDirName(2) + "/file1",
                        newVersion.GetSegmentDirName(2) + "/file2",
                        newVersion.GetSegmentDirName(2) + "/file5"));                        

    }

    void TestGetDeployFilesWithSubIndexPartition()
    {
        //lastVersion is invalid
        string testPath = FileSystemWrapper::JoinPath(
                mRootDir, "segment_0_level_0");
        CreateFiles(testPath, "file1;file2;file3");
        string testSubPath = FileSystemWrapper::JoinPath(
                testPath, SUB_SEGMENT_DIR_NAME);
        CreateFiles(testSubPath, "file1;file2;file3");
        
        string versionPath = FileSystemWrapper::JoinPath(
                mRootDir, "version.0");
        string content = "{\"segments\":[0],\"versionid\":0, \"format_version\": 1}";
        FileSystemWrapper::AtomicStore(versionPath, content);

        FileList tmpList;
        DeployIndexWrapper deployIndexWrapper(mRootDir, "", mOnlineConfig);
        deployIndexWrapper.DumpSegmentDeployIndex("segment_0_level_0");
        INDEXLIB_TEST_TRUE(deployIndexWrapper.GetDeployFiles(tmpList, 0));
        EXPECT_THAT(tmpList, UnorderedElementsAre(
                        INDEX_FORMAT_VERSION_FILE_NAME,
                        index::SCHEMA_FILE_NAME,
                        string("segment_0_level_0/") + SEGMENT_FILE_LIST,
                        string("segment_0_level_0/file1"),
                        string("segment_0_level_0/file2"),
                        string("segment_0_level_0/file3"),
                        string("segment_0_level_0/sub_segment/"),
                        string("segment_0_level_0/sub_segment/file1"),
                        string("segment_0_level_0/sub_segment/file2"),
                        string("segment_0_level_0/sub_segment/file3")));
    }
    
    void TestDumpDeployIndexWithAtomicStoreFail()
    {
        string testPath = FileSystemWrapper::JoinPath(mRootDir, "segment_0");
        CreateFiles(testPath, "file1;file2;file3");
        string tempDeployIndexFile = string(SEGMENT_FILE_LIST) + FileSystemWrapper::TEMP_SUFFIX;
        CreateFiles(testPath, tempDeployIndexFile);

        DeployIndexWrapper deployIndexWrapper(testPath, "", mOnlineConfig);
        SegmentFileListWrapper::Dump(testPath);
        FileList tmpList;
        int64_t totalLength = 0;
        deployIndexWrapper.GetFiles("", tmpList, totalLength);
        EXPECT_THAT(tmpList, UnorderedElementsAre(
                        string(SEGMENT_FILE_LIST),
                        string("file1"), string("file2"), string("file3")));
    }

    void TestDumpDeployIndexWithLength()
    {
        string testPath = FileSystemWrapper::JoinPath(mRootDir, "DumpDeployIndex");
        string f1 = FileSystemWrapper::JoinPath(testPath, "f1");
        string f3 = FileSystemWrapper::JoinPath(testPath, "f3");
        string f5 = FileSystemWrapper::JoinPath(testPath, "f5");
        FileSystemWrapper::AtomicStore(f1, "1");
        FileSystemWrapper::AtomicStore(f3, "333");
        FileSystemWrapper::AtomicStore(f5, "55555");

        struct utimbuf utimeBuf;
        utimeBuf.modtime = 11;
        utimeBuf.actime = 22;
        ASSERT_TRUE(utime(f1.c_str(), &utimeBuf) == 0);

        DeployIndexWrapper deployIndexWrapper(testPath);
        SegmentFileListWrapper::Dump(testPath);
        string deployIndexFile = FileSystemWrapper::JoinPath(
                testPath, SEGMENT_FILE_LIST);
        INDEXLIB_TEST_TRUE(FileSystemWrapper::IsExist(deployIndexFile));

        string content;
        FileSystemWrapper::AtomicLoad(deployIndexFile, content);
        IndexFileList deployIndexMeta;
        deployIndexMeta.FromString(content);

        auto& deployFileMetas = deployIndexMeta.deployFileMetas;
        ASSERT_EQ(3, deployFileMetas.size());
        map<string, FileInfo> actualMap;
        for (auto meta : deployFileMetas)
        {
            actualMap[meta.filePath] = meta;
        }
        ASSERT_EQ(actualMap.size(), deployFileMetas.size());
        ASSERT_EQ(1, actualMap["f1"].fileLength);
        ASSERT_EQ(11, actualMap["f1"].modifyTime);
        ASSERT_EQ(3, actualMap["f3"].fileLength);
        ASSERT_EQ(5, actualMap["f5"].fileLength);

        struct stat sb;
        ASSERT_TRUE(stat(f3.c_str(), &sb) == 0);
        ASSERT_EQ(sb.st_mtime, actualMap["f3"].modifyTime);
        ASSERT_TRUE(stat(f5.c_str(), &sb) == 0);
        ASSERT_EQ(sb.st_mtime, actualMap["f5"].modifyTime);

        FileSystemWrapper::DeleteDir(testPath);
    }

    void TestGetDeployIndexMeta()
    {
        DirectoryPtr offlineDir = GET_PARTITION_DIRECTORY()->MakeDirectory("offline");
        string offlinePath = offlineDir->GetPath();
        string localPath = GET_PARTITION_DIRECTORY()->MakeDirectory("local")->GetPath();
        DeployIndexMeta remoteDeployMeta, localDeployMeta;
        OnlineConfig targetOnlineConfig, baseOnlineConfig;
        
        ASSERT_FALSE(DeployIndexWrapper::GetDeployIndexMeta(
                         remoteDeployMeta, localDeployMeta, offlinePath, localPath,
                         3, 2, targetOnlineConfig, baseOnlineConfig));
        
        PartitionDataMaker::MakeVersions(offlineDir, "0:0; 1:0,1; 2:1,2");
        ASSERT_FALSE(DeployIndexWrapper::GetDeployIndexMeta(
                         remoteDeployMeta, localDeployMeta, offlinePath, localPath,
                         3, 2, targetOnlineConfig, baseOnlineConfig));

        ASSERT_TRUE(DeployIndexWrapper::GetDeployIndexMeta(
                        remoteDeployMeta, localDeployMeta, offlinePath, localPath,
                        2, INVALID_VERSION, targetOnlineConfig, baseOnlineConfig));

        ASSERT_TRUE(DeployIndexWrapper::GetDeployIndexMeta(
                        remoteDeployMeta, localDeployMeta, offlinePath, localPath,
                        2, 0, targetOnlineConfig, baseOnlineConfig));
        
        FileSystemWrapper::Copy(offlinePath + "/deploy_meta.0", localPath + "/deploy_meta.0");
        ASSERT_TRUE(DeployIndexWrapper::GetDeployIndexMeta(
                        remoteDeployMeta, localDeployMeta, offlinePath, localPath,
                        2, 0, targetOnlineConfig, baseOnlineConfig));
    }

    void TestGetDeployIndexMetaBaseVersionInvalid()
    {
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
        offlineDir->Store("deploy_meta.2", targetVersionDeployMeta.ToString(), false);
        
        FromJsonString(onlineConfig, R"( { 
            "load_config": [
                {"file_patterns": ["remote_1_deploy_1"], "remote": true, "deploy": true }, 
                {"file_patterns": ["remote_1_deploy_0"], "remote": true, "deploy": false },
                {"file_patterns": ["remote_0_deploy_1"], "remote": false, "deploy": true }
            ],
            "need_read_remote_index": true
        } )");

        ASSERT_TRUE(DeployIndexWrapper::GetDeployIndexMeta(
            remoteDeployMeta, localDeployMeta, offlineDir->GetPath(), localDir->GetPath(),
            2, INVALID_VERSION, onlineConfig, onlineConfig));
        CheckDeployMeta(remoteDeployMeta, {"remote_1_deploy_1", "remote_1_deploy_0"}, {});
        CheckDeployMeta(localDeployMeta, {"remote_1_deploy_1", "remote_0_deploy_1", "deploy_meta.2"}, {"version.2"});
    }

    void TestGetDeployIndexMetaComplex()
    {
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
        localDir->Store("deploy_meta.1", baseVersionDeployMeta.ToString(), false);
        // target
        targetVersionDeployMeta.Append(FileInfo("-----r1d1", 10)); // new files
        targetVersionDeployMeta.Append(FileInfo("-----r1d0", 10));
        targetVersionDeployMeta.Append(FileInfo("-----r0d1", 10));
        targetVersionDeployMeta.Append(FileInfo("deploy_meta.2", -1));
        targetVersionDeployMeta.AppendFinal(FileInfo("version.2", 10));
        offlineDir->Store("deploy_meta.2", targetVersionDeployMeta.ToString(), false);
        
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

        ASSERT_TRUE(DeployIndexWrapper::GetDeployIndexMeta(
            remoteDeployMeta, localDeployMeta, offlineDir->GetPath(), localDir->GetPath(),
            2, 1, targetOnlineConfig, baseOnlineConfig));
        CheckDeployMeta(remoteDeployMeta,
                {"r0d1-r1d1", "r0d1-r1d0", "-----r1d1", "-----r1d0"}, {});
        CheckDeployMeta(localDeployMeta,
                {"r1d0-r1d1", "r1d0-r0d1", "-----r1d1", "-----r0d1", "deploy_meta.2"},
                {"version.2"});
    }

    void TestGetDeployIndexMetaDeployMetaNotExist()
    {
        DirectoryPtr offlineDir = GET_PARTITION_DIRECTORY()->MakeDirectory("offline");
        DirectoryPtr localDir = GET_PARTITION_DIRECTORY()->MakeDirectory("local");
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
        offlineDir->MakeDirectory("segment_0_level_0")->Store("deploy_index", seg0DeployMeta.ToString(), false);
        Version baseVersion(0);
        baseVersion.AddSegment(0);
        baseVersion.Store(offlineDir, true);
        // target
        seg2DeployMeta.Append(FileInfo("-----r1d1", 10));
        seg2DeployMeta.Append(FileInfo("-----r1d0", 10));
        seg2DeployMeta.Append(FileInfo("-----r0d1", 10));
        offlineDir->MakeDirectory("segment_2_level_0")->Store("deploy_index", seg2DeployMeta.ToString(), false);
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

        ASSERT_TRUE(DeployIndexWrapper::GetDeployIndexMeta(
            remoteDeployMeta, localDeployMeta, offlineDir->GetPath(), localDir->GetPath(),
            2, 0, targetOnlineConfig, baseOnlineConfig));
        CheckDeployMeta(remoteDeployMeta,
                {"segment_0_level_0/r0d1-r1d1", "segment_0_level_0/r0d1-r1d0","segment_2_level_0/-----r1d1", "segment_2_level_0/-----r1d0"}, {});
        CheckDeployMeta(localDeployMeta,
                {"segment_0_level_0/r1d0-r1d1", "segment_0_level_0/r1d0-r0d1", "segment_0_level_0/deploy_index", "segment_2_level_0/-----r1d1", "segment_2_level_0/-----r0d1", "segment_2_level_0/deploy_index", "segment_2_level_0/"},
                {"version.2"});
    }

    void TestGetDeployIndexMetaVersionFile()
    {
        string offlinePath = GET_PARTITION_DIRECTORY()->MakeDirectory("offline")->GetPath();
        string localPath = GET_PARTITION_DIRECTORY()->MakeDirectory("local")->GetPath();
        IndexFileList targetVersionDeployMeta;
        targetVersionDeployMeta.AppendFinal(FileInfo("version.2", 10));
        FileSystemWrapper::Store(offlinePath + "/deploy_meta.2",
                                 targetVersionDeployMeta.ToString(), false);

        {
            OnlineConfig baseOnlineConfig, targetOnlineConfig;
            DeployIndexMeta remoteDeployMeta, localDeployMeta;
            FromJsonString(targetOnlineConfig, R"( { 
                "load_config": [
                    {"file_patterns": ["version.*"], "remote": true, "deploy": true }
                ],
                "need_read_remote_index": true
            } )");        
            ASSERT_TRUE(DeployIndexWrapper::GetDeployIndexMeta(
                            remoteDeployMeta, localDeployMeta, offlinePath, localPath,
                            2, INVALID_VERSION, targetOnlineConfig, baseOnlineConfig));
            CheckDeployMeta(remoteDeployMeta, {}, {"version.2"});
            CheckDeployMeta(localDeployMeta, {}, {"version.2"});
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
            ASSERT_TRUE(DeployIndexWrapper::GetDeployIndexMeta(
                            remoteDeployMeta, localDeployMeta, offlinePath, localPath,
                            2, INVALID_VERSION, targetOnlineConfig, baseOnlineConfig));
            CheckDeployMeta(remoteDeployMeta, {}, {"version.2"});
            CheckDeployMeta(localDeployMeta, {}, {});
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
            ASSERT_TRUE(DeployIndexWrapper::GetDeployIndexMeta(
                            remoteDeployMeta, localDeployMeta, offlinePath, localPath,
                            2, INVALID_VERSION, targetOnlineConfig, baseOnlineConfig));
            CheckDeployMeta(remoteDeployMeta, {}, {});
            CheckDeployMeta(localDeployMeta, {}, {"version.2"});
        }
    }

    void TestDeployIndexMetaWithOngoingOps()
    {
        DirectoryPtr localDir = GET_PARTITION_DIRECTORY()->MakeDirectory("local");
        DirectoryPtr offlineDir = GET_PARTITION_DIRECTORY()->MakeDirectory("offline");
        Version oldVersion;
        oldVersion.SetVersionId(2);
        oldVersion.SetOngoingModifyOperations({3});
        oldVersion.SetSchemaVersionId(3);
        oldVersion.AddSegment(0);
        oldVersion.Store(offlineDir, true);
        FileSystemWrapper::Copy(string(TEST_DATA_PATH) + "schema_for_deploy_inedx_wrapper_test.json",
                                offlineDir->GetPath() + "/schema.json.3");
        FileSystemWrapper::Copy(string(TEST_DATA_PATH) + "schema_for_deploy_inedx_wrapper_test.json",
                                localDir->GetPath() + "/schema.json.3");

        IndexFileList oldDeployMeta;
        //nid price string2 is deleted, 
        oldDeployMeta.Append(FileInfo("segment_0_level_0/index/new_nid/data", 10));
        oldDeployMeta.Append(FileInfo("segment_0_level_0/attribute/price/data", 10));
        oldDeployMeta.Append(FileInfo("segment_0_level_0/attribute/string2/data", 10));
        oldDeployMeta.Append(FileInfo("patch_index_0/segment_0_level_0/index/nid/data", 10));
        oldDeployMeta.Append(FileInfo("segment_1_level_0/index/nid/data", 10));
        //uid is added but not ready
        oldDeployMeta.Append(FileInfo("segment_1_level_0/index/uid/data", 10));
        oldDeployMeta.AppendFinal(FileInfo("version.2", 10));
        string deployMetaFileName = DeployIndexWrapper::GetDeployMetaFileName(oldVersion.GetVersionId());
        offlineDir->Store(deployMetaFileName, oldDeployMeta.ToString());
        offlineDir->Sync(true);
        DeployIndexMeta remoteDeploymeta, localDeployMeta;
        OnlineConfig onlineConfig;
        FromJsonString(onlineConfig, R"( { 
        "disable_fields": {
            "attributes": ["string2"],
            "rewrite_load_config": true
        }
        } )");
        //deploy done
        DeployIndexWrapper::GetDeployIndexMeta(remoteDeploymeta, localDeployMeta, 
                offlineDir->GetPath(), localDir->GetPath(), 2, -1, onlineConfig, onlineConfig);
        CheckDeployMeta(localDeployMeta, {"segment_0_level_0/index/new_nid/data"}, {"version.2"});
        oldVersion.Store(localDir, true);

        //make op3 ready
        Version newVersion;
        newVersion.SetVersionId(3);
        newVersion.SetOngoingModifyOperations({});
        newVersion.SetSchemaVersionId(3);
        newVersion.AddSegment(0);
        newVersion.AddSegment(2);
        newVersion.Store(offlineDir, true);

        IndexFileList newDeployMeta;
        //nid & uid is ready
        newDeployMeta = oldDeployMeta;
        newDeployMeta.Append(FileInfo("segment_2_level_0/index/nid/data", 10));
        newDeployMeta.Append(FileInfo("segment_2_level_0/index/uid/data", 10));
        newDeployMeta.Append(FileInfo("segment_2_level_0/attribute/price/data", 10));
        newDeployMeta.Append(FileInfo("segment_2_level_0/attribute/string2/data", 10));
        newDeployMeta.Append(FileInfo("patch_index_0/segment_0_level_0/index/uid/data", 10));

        newDeployMeta.AppendFinal(FileInfo("version.2", 10));
        newDeployMeta.AppendFinal(FileInfo("version.3", 10));
        deployMetaFileName = DeployIndexWrapper::GetDeployMetaFileName(newVersion.GetVersionId());
        offlineDir->Store(deployMetaFileName, newDeployMeta.ToString());
        offlineDir->Sync(true);
        
        DeployIndexMeta remoteDeploymeta2, localDeployMeta2;
        DeployIndexWrapper::GetDeployIndexMeta(remoteDeploymeta2, localDeployMeta2, 
                offlineDir->GetPath(), localDir->GetPath(), 3, 2, onlineConfig, onlineConfig);
        CheckDeployMeta(localDeployMeta2, 
                        {"segment_2_level_0/index/nid/data", // new segment
                         "segment_2_level_0/index/uid/data", 
                         "segment_1_level_0/index/nid/data", // old segment
                         "segment_1_level_0/index/uid/data",
                         "patch_index_0/segment_0_level_0/index/nid/data",
                         "patch_index_0/segment_0_level_0/index/uid/data"}, 
                        {"version.3"});
    }

    void TestGenerateLoadConfigFromVersion()
    {
        Version version;
        version.SetVersionId(10);
        version.SetOngoingModifyOperations({3});
        version.SetSchemaVersionId(4);
        string versionPath = FileSystemWrapper::JoinPath(mRootDir, "version.10");
        FileSystemWrapper::AtomicStore(versionPath, ToJsonString(version));
        FileSystemWrapper::Copy(string(TEST_DATA_PATH) + "schema_with_modify_operations.json",
                                mRootDir + "/schema.json.4");
        // index2, price, string2 will be delete
        // nid is deleted, but add again, not ready
        LoadConfig loadConfig;
        ASSERT_TRUE(DeployIndexWrapper::GenerateLoadConfigFromVersion(mRootDir, 10, loadConfig));
        ASSERT_TRUE(loadConfig.Match("segment_2_level_0/attribute/price", ""));
        ASSERT_TRUE(loadConfig.Match("segment_2_level_0/index/index2", ""));
        ASSERT_TRUE(loadConfig.Match("segment_2_level_0/attribute/string2/data", ""));
        ASSERT_TRUE(loadConfig.Match("segment_2_level_0/index/nid/data", ""));
        ASSERT_TRUE(loadConfig.Match("segment_2_level_0/attribute/nid/data", ""));

        // make operation 3 ready
        version.SetOngoingModifyOperations({});
        FileSystemWrapper::AtomicStoreIgnoreExist(versionPath, ToJsonString(version));
        ASSERT_TRUE(DeployIndexWrapper::GenerateLoadConfigFromVersion(mRootDir, 10, loadConfig));
        ASSERT_TRUE(loadConfig.Match("segment_2_level_0/attribute/price", ""));
        ASSERT_TRUE(loadConfig.Match("segment_2_level_0/index/index2", ""));
        ASSERT_TRUE(loadConfig.Match("segment_2_level_0/attribute/string2/data", ""));
        ASSERT_FALSE(loadConfig.Match("segment_2_level_0/index/nid/data", ""));
        ASSERT_FALSE(loadConfig.Match("segment_2_level_0/attribute/nid/data", ""));
    }

    void TestGetDeployIndexMetaWithDisableAttribute()
    {
        DirectoryPtr offlineDir = GET_PARTITION_DIRECTORY()->MakeDirectory("offline");
        DirectoryPtr localDir = GET_PARTITION_DIRECTORY()->MakeDirectory("local");

        IndexFileList baseVersionDeployMeta, targetVersionDeployMeta;
        targetVersionDeployMeta.Append(FileInfo("segment_1_level_0/attribute/norm_attr_1/data", 10));
        targetVersionDeployMeta.Append(FileInfo("segment_1_level_0/attribute/disable_attr_1/data", 10));
        targetVersionDeployMeta.Append(FileInfo("deploy_meta.2", -1));
        targetVersionDeployMeta.AppendFinal(FileInfo("version.2", 10));
        offlineDir->Store("deploy_meta.2", targetVersionDeployMeta.ToString(), false);

        {
            OnlineConfig onlineConfig;
            FromJsonString(onlineConfig, R"( {
                "load_config": [
                     {"file_patterns": ["attribute"], "remote": true, "deploy": true }
                ],
                "need_read_remote_index": true,
                "disable_fields": { "attributes": ["disable_attr_1"] }
            } )");
            {
                DeployIndexMeta remoteDeployMeta, localDeployMeta;
                ASSERT_TRUE(DeployIndexWrapper::GetDeployIndexMeta(
                                remoteDeployMeta, localDeployMeta, offlineDir->GetPath(), localDir->GetPath(),
                                2, INVALID_VERSION, onlineConfig, onlineConfig));
                CheckDeployMeta(remoteDeployMeta, {"segment_1_level_0/attribute/norm_attr_1/data"}, {});
                CheckDeployMeta(localDeployMeta, {"segment_1_level_0/attribute/norm_attr_1/data", "deploy_meta.2"}, {"version.2"});
            }
            cout << 1 << endl;
            {
                DeployIndexMeta localDeployMeta;
                DeployIndexWrapper deployIndexWrapper(offlineDir->GetPath(), localDir->GetPath(), onlineConfig);
                ASSERT_TRUE(deployIndexWrapper.GetDeployIndexMeta(localDeployMeta, 2, INVALID_VERSION));
                CheckDeployMeta(localDeployMeta, {"segment_1_level_0/attribute/norm_attr_1/data", "deploy_meta.2"}, {"version.2"});
            }
            cout << 2 << endl;
        }
        {
            OnlineConfig onlineConfig;
            FromJsonString(onlineConfig, R"( {
                "load_config": [
                     {"file_patterns": ["attribute"], "remote": true, "deploy": true }
                ],
                "disable_fields": { "attributes": ["disable_attr_1"] }
            } )");
            {
                DeployIndexMeta remoteDeployMeta, localDeployMeta;
                ASSERT_TRUE(DeployIndexWrapper::GetDeployIndexMeta(
                                remoteDeployMeta, localDeployMeta, offlineDir->GetPath(), localDir->GetPath(),
                                2, INVALID_VERSION, onlineConfig, onlineConfig));
                CheckDeployMeta(remoteDeployMeta, {}, {});
                CheckDeployMeta(localDeployMeta, {"segment_1_level_0/attribute/norm_attr_1/data", "deploy_meta.2"}, {"version.2"});
            }
            cout << 3 << endl;
            {
                DeployIndexMeta localDeployMeta;
                DeployIndexWrapper deployIndexWrapper(offlineDir->GetPath(), localDir->GetPath(), onlineConfig);
                ASSERT_TRUE(deployIndexWrapper.GetDeployIndexMeta(localDeployMeta, 2, INVALID_VERSION));
                CheckDeployMeta(localDeployMeta, {"segment_1_level_0/attribute/norm_attr_1/data", "deploy_meta.2"}, {"version.2"});
            }
            cout << 4 << endl;
        }
    }

    void CheckDeployMeta(const DeployIndexMeta& deployMeta,
                         vector<string> expectFileNames,
                         vector<string> expectFinalFileNames)
    {
        ASSERT_TRUE(deployMeta.isComplete);
        vector<string> actualFileNames, actualFinalFileNames;
        for (const FileInfo& fileInfo : deployMeta.deployFileMetas)
        {
            ASSERT_TRUE(fileInfo.isValid());
            actualFileNames.push_back(fileInfo.filePath);
        }
        EXPECT_THAT(actualFileNames, UnorderedElementsAreArray(expectFileNames));
        for (const FileInfo& fileInfo : deployMeta.finalDeployFileMetas)
        {
            ASSERT_TRUE(fileInfo.isValid());
            actualFinalFileNames.push_back(fileInfo.filePath);
        }
        EXPECT_THAT(actualFinalFileNames, UnorderedElementsAreArray(expectFinalFileNames));
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
INDEXLIB_UNIT_TEST_CASE(DeployIndexWrapperTest, TestGetTruncateMetaDeployFiles);
INDEXLIB_UNIT_TEST_CASE(DeployIndexWrapperTest, TestGetDiffSegmentIds);
INDEXLIB_UNIT_TEST_CASE(DeployIndexWrapperTest, TestGetSegmentDeployFiles);
INDEXLIB_UNIT_TEST_CASE(DeployIndexWrapperTest, TestGetDeployFiles);
INDEXLIB_UNIT_TEST_CASE(DeployIndexWrapperTest, TestGetDeployFilesByConfig);
INDEXLIB_UNIT_TEST_CASE(DeployIndexWrapperTest, TestGetDeployFilesWithSubIndexPartition);
INDEXLIB_UNIT_TEST_CASE(DeployIndexWrapperTest, TestDumpSegmentDeployIndexWithDirectory);
INDEXLIB_UNIT_TEST_CASE(DeployIndexWrapperTest, TestDumpSegmentDeployIndexWithDirectoryAndPackageFile);
INDEXLIB_UNIT_TEST_CASE(DeployIndexWrapperTest, TestSimpleProcess);
INDEXLIB_UNIT_TEST_CASE(DeployIndexWrapperTest, TestDumpDeployIndexWithLength);
INDEXLIB_UNIT_TEST_CASE(DeployIndexWrapperTest, TestGetDeployIndexMeta);
INDEXLIB_UNIT_TEST_CASE(DeployIndexWrapperTest, TestGetDeployIndexMetaBaseVersionInvalid);
INDEXLIB_UNIT_TEST_CASE(DeployIndexWrapperTest, TestGetDeployIndexMetaComplex);
INDEXLIB_UNIT_TEST_CASE(DeployIndexWrapperTest, TestGetDeployIndexMetaDeployMetaNotExist);
INDEXLIB_UNIT_TEST_CASE(DeployIndexWrapperTest, TestGetDeployIndexMetaVersionFile);
INDEXLIB_UNIT_TEST_CASE(DeployIndexWrapperTest, TestGenerateLoadConfigFromVersion);
INDEXLIB_UNIT_TEST_CASE(DeployIndexWrapperTest, TestDeployIndexMetaWithOngoingOps);
INDEXLIB_UNIT_TEST_CASE(DeployIndexWrapperTest, TestGetDeployIndexMetaWithDisableAttribute);

IE_NAMESPACE_END(index_base);


