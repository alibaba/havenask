#include "indexlib/file_system/test/package_file_mount_table_unittest.h"
#include "indexlib/file_system/test/package_file_util.h"

using namespace std;
using namespace autil;
using namespace fslib;

IE_NAMESPACE_USE(util);
IE_NAMESPACE_USE(storage);

IE_NAMESPACE_BEGIN(file_system);
IE_LOG_SETUP(file_system, PackageFileMountTableTest);

PackageFileMountTableTest::PackageFileMountTableTest()
{
}

PackageFileMountTableTest::~PackageFileMountTableTest()
{
}

void PackageFileMountTableTest::CaseSetUp()
{
    // 5 file + 2 inner segment
    mInnerFileDespStr = "file1:abc#file2:abc#"
                        "segment/file3:abc#"
                        "segment/file4:abc#"
                        "segment/subSeg/file5:abc";

    // 2 empty inner segment
    mInnerDirDespStr = "empty_segment:"
                       "segment/empty_subSeg";
}

void PackageFileMountTableTest::CaseTearDown()
{
}

void PackageFileMountTableTest::TestMountPackageFile()
{
    string packageFileName = "package_file";
    string packageFilePath = PathUtil::JoinPath(
            GET_SEGMENT_DIRECTORY()->GetPath(), packageFileName);

    PackageFileMountTable mountTable;
    size_t fileLen = 0;
    string packageFileDataPath = PackageFileMeta::GetPackageFileDataPath(
        packageFilePath, "", 0);
    // not exist package file
    ASSERT_FALSE(mountTable.MountPackageFile(packageFilePath));
    ASSERT_FALSE(mountTable.GetPackageFileLength(
                     packageFileDataPath, fileLen));
    
    PreparePackageFile(packageFileName, mInnerFileDespStr, mInnerDirDespStr);
    // normal mount 
    ASSERT_TRUE(mountTable.MountPackageFile(packageFilePath));
    ASSERT_EQ((size_t)9, mountTable.mTable.size());
    ASSERT_EQ((size_t)1, mountTable.mMountedMap.size());
    ASSERT_TRUE(mountTable.GetPackageFileLength(
                    packageFileDataPath, fileLen));
    ASSERT_EQ((size_t)16387, fileLen);
    ASSERT_TRUE(mountTable.GetPackageFileLength(
                    PackageFileMeta::GetPackageFileMetaPath(packageFilePath), fileLen));
    ASSERT_EQ((size_t)1491, fileLen);

    PackageFileMeta packageFileMeta;
    string packageFileMetaStr;
    FileSystemWrapper::AtomicLoad(PackageFileMeta::GetPackageFileMetaPath(packageFilePath),
                                  packageFileMetaStr);
    packageFileMeta.InitFromString(packageFileMetaStr);
    ASSERT_EQ(1, packageFileMeta.GetPhysicalFileNames().size());
    ASSERT_EQ("package_file.__data__0", packageFileMeta.GetPhysicalFileNames()[0]);
    TearDown(); // remove package file
    SetUp();

    // repeated mount
    ASSERT_TRUE(mountTable.MountPackageFile(packageFilePath));
    ASSERT_EQ((size_t)9, mountTable.mTable.size());
    ASSERT_EQ((size_t)1, mountTable.mMountedMap.size());

    // mount another package file with duplicate inner dir
    packageFileName = "duplicate_dir_package_file";
    packageFilePath = PathUtil::JoinPath(
            GET_SEGMENT_DIRECTORY()->GetPath(), packageFileName);
    PreparePackageFile(packageFileName, "", mInnerDirDespStr);
    ASSERT_TRUE(mountTable.MountPackageFile(packageFilePath));

    // mount another package file with duplicate inner file
    packageFileName = "duplicate_file_package_file";
    packageFilePath = PathUtil::JoinPath(
            GET_SEGMENT_DIRECTORY()->GetPath(), packageFileName);
    PreparePackageFile(packageFileName, mInnerFileDespStr, "");
    ASSERT_THROW(mountTable.MountPackageFile(packageFilePath), 
                 misc::InconsistentStateException);

    {
        // test mount empty PackageFileMeta
        packageFileName = "empty_package_file";
        packageFilePath = PathUtil::JoinPath(
            GET_SEGMENT_DIRECTORY()->GetPath(), packageFileName);
        PreparePackageFile(packageFileName, "", mInnerDirDespStr);
        mountTable.MountPackageFile(packageFilePath);
        size_t fileLen = 1;
        string packageFileDataPath =
            PackageFileMeta::GetPackageFileDataPath(packageFilePath, "", 0);
        ASSERT_TRUE(mountTable.GetPackageFileLength(packageFileDataPath, fileLen));
        ASSERT_EQ(0, fileLen);
    }
}

void PackageFileMountTableTest::TestGetMountMeta()
{
    string packageParentPath = GET_SEGMENT_DIRECTORY()->GetPath();
    string packageFileName = "package_file";
    string packageFilePath = PathUtil::JoinPath(
            packageParentPath, packageFileName);

    PackageFileMountTable mountTable;
    PreparePackageFile(packageFileName, mInnerFileDespStr, mInnerDirDespStr);
    // not exist package file
    ASSERT_TRUE(mountTable.MountPackageFile(packageFilePath));

    string packageFileDataPath = packageFilePath + ".__data__0";
    CheckInnerFileMeta(mountTable, packageFileDataPath,
                       "file1:FILE:0:3:0");
    CheckInnerFileMeta(mountTable, packageFileDataPath,
                       "file2:FILE:4096:3:0");
    CheckInnerFileMeta(mountTable, packageFileDataPath, 
                       "segment/file3:FILE:8192:3:0");
    CheckInnerFileMeta(mountTable, packageFileDataPath, 
                       "segment/file4:FILE:12288:3:0");
    CheckInnerFileMeta(mountTable, packageFileDataPath,
                       "segment/subSeg/file5:FILE:16384:3:0");
    CheckInnerFileMeta(mountTable, packageFileDataPath,
                       "empty_segment:DIR:0:0:0");
    CheckInnerFileMeta(mountTable, packageFileDataPath,
                       "segment/empty_subSeg:DIR:0:0:0");

    PackageFileMeta::InnerFileMeta fileMountMeta;
    ASSERT_FALSE(mountTable.GetMountMeta(
                    "non-exist-file", fileMountMeta));
}

void PackageFileMountTableTest::TestIsExist()
{
    string packageParentPath = GET_SEGMENT_DIRECTORY()->GetPath();

    string packageFileName = "package_file";
    string packageFilePath = PathUtil::JoinPath(
            packageParentPath, packageFileName);

    PackageFileMountTable mountTable;
    PreparePackageFile(packageFileName, mInnerFileDespStr, mInnerDirDespStr);
    ASSERT_TRUE(mountTable.MountPackageFile(packageFilePath));

    ASSERT_TRUE(mountTable.IsExist(packageParentPath + "/file1"));
    ASSERT_TRUE(mountTable.IsExist(packageParentPath + "/file2"));
    ASSERT_TRUE(mountTable.IsExist(packageParentPath + "/segment"));
    ASSERT_TRUE(mountTable.IsExist(packageParentPath + "/segment/file3"));
    ASSERT_TRUE(mountTable.IsExist(packageParentPath + "/segment/file4"));
    ASSERT_TRUE(mountTable.IsExist(packageParentPath + "/segment/subSeg"));
    ASSERT_TRUE(mountTable.IsExist(packageParentPath + "/segment/subSeg/file5"));
    ASSERT_TRUE(mountTable.IsExist(packageParentPath + "/empty_segment"));
    ASSERT_TRUE(mountTable.IsExist(packageParentPath + "/segment/empty_subSeg"));

    ASSERT_FALSE(mountTable.IsExist(packageParentPath + "/not_exist"));
}

void PackageFileMountTableTest::TestListFile()
{
    string packageParentPath = GET_SEGMENT_DIRECTORY()->GetPath();
    string packageFileName = "package_file";
    string packageFilePath = PathUtil::JoinPath(
            packageParentPath, packageFileName);

    PackageFileMountTable mountTable;
    PreparePackageFile(packageFileName, mInnerFileDespStr, mInnerDirDespStr);
    ASSERT_TRUE(mountTable.MountPackageFile(packageFilePath));

    CheckListFile(packageParentPath, mountTable, true, 
                  "file1:file2:"
                  "segment/:segment/file3:segment/file4:"
                  "segment/subSeg/:segment/subSeg/file5:"
                  "segment/empty_subSeg/:"
                  "empty_segment/");

    CheckListFile(packageParentPath, mountTable, false, 
                  "file1:file2:segment:empty_segment");

    CheckListFile(packageParentPath+"/segment", mountTable, false,
                  "file3:file4:subSeg:empty_subSeg");

    CheckListFile(packageParentPath+"/segment", mountTable, true,
                  "file3:file4:subSeg/:subSeg/file5:empty_subSeg/");

    CheckListFile(packageParentPath+"empty_segment", mountTable, true, "");
    CheckListFile(packageParentPath+"non-exist-dir", mountTable, true, "");
    CheckListFile(packageParentPath+"/file1", mountTable, true, "");
}

void PackageFileMountTableTest::TestRemoveFile()
{
    string packageParentPath = GET_SEGMENT_DIRECTORY()->GetPath();
    string packageFileName = "package_file";
    string packageFilePath = PathUtil::JoinPath(
            packageParentPath, packageFileName);

    PackageFileMountTable mountTable;
    PreparePackageFile(packageFileName, mInnerFileDespStr, mInnerDirDespStr);
    ASSERT_TRUE(mountTable.MountPackageFile(packageFilePath));

    ASSERT_FALSE(mountTable.RemoveFile(packageParentPath + "/non-exist"));
    ASSERT_FALSE(mountTable.RemoveFile(packageParentPath + "/segment"));

    ASSERT_TRUE(mountTable.IsExist(packageParentPath + "/segment/file3"));
    ASSERT_TRUE(mountTable.RemoveFile(packageParentPath + "/segment/file3"));
    ASSERT_EQ(8ul, mountTable.mPhysicalFileInfoMap[packageFilePath + ".__data__0"].refCount);
    ASSERT_FALSE(mountTable.IsExist(packageParentPath + "/segment/file3"));

    // remove file trigger remove physical files
    packageFileName = "new_package_file";
    packageFilePath = PathUtil::JoinPath(packageParentPath, packageFileName);
    PreparePackageFile(packageFileName, "single_file:abc", "");
    ASSERT_TRUE(mountTable.MountPackageFile(packageFilePath));
    
    string packageMetaPath = packageFilePath + ".__meta__";
    string packageDataPath = packageFilePath + ".__data__0";
    ASSERT_TRUE(FileSystemWrapper::IsExist(packageMetaPath));
    ASSERT_TRUE(FileSystemWrapper::IsExist(packageDataPath));

    ASSERT_TRUE(mountTable.RemoveFile(packageParentPath + "/single_file"));
    ASSERT_FALSE(FileSystemWrapper::IsExist(packageMetaPath));
    ASSERT_FALSE(FileSystemWrapper::IsExist(packageDataPath));

    ASSERT_EQ((size_t)2, mountTable.mMountedMap.size());
    ASSERT_EQ(2ul, mountTable.mPhysicalFileInfoMap.size());
    ASSERT_EQ(0, mountTable.mPhysicalFileInfoMap[packageFilePath + ".__data__0"].refCount);
}

void PackageFileMountTableTest::TestRemoveDirectory()
{
    string packageParentPath = GET_SEGMENT_DIRECTORY()->GetPath();
    string packageFileName = "package_file";
    string packageFilePath = PathUtil::JoinPath(
            packageParentPath, packageFileName);

    PackageFileMountTable mountTable;
    PreparePackageFile(packageFileName, mInnerFileDespStr, mInnerDirDespStr);
    ASSERT_TRUE(mountTable.MountPackageFile(packageFilePath));

    string newInnerFileDespStr = "file6:abc#file7:abc#"
                                 "segment/file8:abc#"
                                 "segment/subSeg/file9:abc#"
                                 "segment/uniqSeg/file10:abc";

    packageFileName = "new_package_file";
    packageFilePath = PathUtil::JoinPath(packageParentPath, packageFileName);
    PreparePackageFile(packageFileName, newInnerFileDespStr, mInnerDirDespStr);
    ASSERT_TRUE(mountTable.MountPackageFile(packageFilePath));

    CheckRemoveDirectory(mountTable, packageParentPath, 
                         "non-exist-dir", "true:package_file#9:new_package_file#10");
    CheckRemoveDirectory(mountTable, packageParentPath, "/file1", "false");
    CheckRemoveDirectory(mountTable, packageParentPath, 
                         "empty_segment", "true:package_file#8:new_package_file#9");
    CheckRemoveDirectory(mountTable, packageParentPath, 
                         "segment/subSeg", "true:package_file#6:new_package_file#7");
    CheckRemoveDirectory(mountTable, packageParentPath, 
                         "segment/uniqSeg", "true:package_file#6:new_package_file#5");

    // remove parent dir
    CheckRemoveDirectory(mountTable, packageParentPath, "", "true");
    ASSERT_TRUE(mountTable.mMountedMap.empty());
    ASSERT_TRUE(mountTable.mTable.empty());
    // package file is already mounted in FileSystem
    ASSERT_FALSE(GET_SEGMENT_DIRECTORY()->IsExist("package_file.__meta__"));
    ASSERT_FALSE(GET_SEGMENT_DIRECTORY()->IsExist("package_file.__data__0"));
    ASSERT_FALSE(GET_SEGMENT_DIRECTORY()->IsExist("new_package_file.__meta__"));
    ASSERT_FALSE(GET_SEGMENT_DIRECTORY()->IsExist("new_package_file.__data__0"));
}

void PackageFileMountTableTest::PreparePackageFile(
        const std::string& packageFileName,
        const string& innerFileDespStr,
        const string& innerDirDespStr)
{
    DirectoryPtr segmentDirectory = GET_SEGMENT_DIRECTORY();
    PackageFileUtil::CreatePackageFile(segmentDirectory, packageFileName,
            innerFileDespStr, innerDirDespStr);
    segmentDirectory->Sync(true);
}

void PackageFileMountTableTest::CheckInnerFileMeta(
        const PackageFileMountTable& mountTable, 
        const string& packageFileDataPath,
        const string& despStr)
{
    vector<string> fileInfo;
    StringUtil::fromString(despStr, fileInfo, ":");

    string parentDir = PathUtil::GetParentDirPath(packageFileDataPath);
    string fileAbsPath = PathUtil::JoinPath(parentDir, fileInfo[0]);

    bool isDir = (fileInfo[1] == "DIR");
    size_t offset = StringUtil::numberFromString<size_t>(fileInfo[2]);
    size_t length = StringUtil::numberFromString<size_t>(fileInfo[3]);
    uint32_t fileIdx = StringUtil::numberFromString<uint32_t>(fileInfo[4]);
    PackageFileMeta::InnerFileMeta expectedMeta(packageFileDataPath, isDir);
    expectedMeta.Init(offset, length, fileIdx);

    PackageFileMeta::InnerFileMeta actualMeta;
    ASSERT_TRUE(mountTable.GetMountMeta(fileAbsPath, actualMeta));
    ASSERT_EQ(expectedMeta, actualMeta);
}

void PackageFileMountTableTest::CheckListFile(
        const string& dirPath, 
        const PackageFileMountTable& mountTable, 
        bool recursive,
        const string& despStr)
{
    vector<string> filePaths;
    StringUtil::fromString(despStr, filePaths, ":");
    sort(filePaths.begin(), filePaths.end());
    
    FileList fileList;
    mountTable.ListFile(dirPath, fileList, recursive);
    ASSERT_EQ(filePaths.size(), fileList.size());
    
    for (size_t i = 0; i < filePaths.size(); ++i)
    {
        ASSERT_EQ(filePaths[i], fileList[i]);
    }
}

// expectResultStr: (true|false):packageFileName#remainFileCount:packageFileName#remainFileCount...
void PackageFileMountTableTest::CheckRemoveDirectory(
        PackageFileMountTable& mountTable,
        const std::string& parentDirPath,
        const std::string& dirName,
        const string& expectResultStr)
{
    string dirPath = PathUtil::JoinPath(parentDirPath, dirName);

    vector<vector<string> > resultInfos;
    StringUtil::fromString(expectResultStr, resultInfos, "#", ":");
    assert(!resultInfos.empty());
    assert(resultInfos[0].size() == 1);

    bool removeOk = (resultInfos[0][0] == "true") ? true : false;
    if (removeOk)
    {
        ASSERT_TRUE(mountTable.RemoveDirectory(dirPath));
        ASSERT_FALSE(mountTable.IsExist(dirPath));
        FileList fileList;
        mountTable.ListFile(dirPath, fileList, true);
        ASSERT_TRUE(fileList.empty());

        for (size_t i = 1; i < resultInfos.size(); ++i)
        {
            assert(resultInfos[i].size() == 2);
            string packageFilePath = PathUtil::JoinPath(
                    parentDirPath, resultInfos[i][0]) + ".__data__0";
            uint32_t expectFileCount = 
                StringUtil::numberFromString<uint32_t>(resultInfos[i][1]);
            ASSERT_EQ(expectFileCount, mountTable.mPhysicalFileInfoMap[packageFilePath].refCount);
        }
    }
    else 
    {
        ASSERT_FALSE(mountTable.RemoveDirectory(dirPath));
    }
}

void PackageFileMountTableTest::TestGetStorageMetrics()
{
    string packageParentPath = GET_SEGMENT_DIRECTORY()->GetPath();
    string packageFileName = "package_file";
    string packageFilePath = PathUtil::JoinPath(
            packageParentPath, packageFileName);

    StorageMetrics storageMetrics;
    PackageFileMountTable mountTable(&storageMetrics);

    PreparePackageFile(packageFileName, mInnerFileDespStr, mInnerDirDespStr);
    ASSERT_TRUE(mountTable.MountPackageFile(packageFilePath));

    ASSERT_EQ((int64_t)1, storageMetrics.GetPackageFileCount());
    ASSERT_EQ((int64_t)5, storageMetrics.GetPackageInnerFileCount());
    ASSERT_EQ((int64_t)4, storageMetrics.GetPackageInnerDirCount());

    // mount another package file with duplicate inner dirs
    packageFileName = "duplicate_dir_package_file";
    packageFilePath = PathUtil::JoinPath(
            GET_SEGMENT_DIRECTORY()->GetPath(), packageFileName);
    PreparePackageFile(packageFileName, 
                       "segment/subSeg/file6:bcd",
                       "segment/empty_subSeg");

    ASSERT_TRUE(mountTable.MountPackageFile(packageFilePath));

    ASSERT_EQ((int64_t)2, storageMetrics.GetPackageFileCount());
    ASSERT_EQ((int64_t)6, storageMetrics.GetPackageInnerFileCount());
    ASSERT_EQ((int64_t)4, storageMetrics.GetPackageInnerDirCount());

    // delete a single file
    ASSERT_TRUE(mountTable.RemoveFile(packageParentPath + "/file1"));
    ASSERT_EQ((int64_t)5, storageMetrics.GetPackageInnerFileCount());

    // delete a shared non-empty dir 
    ASSERT_TRUE(mountTable.RemoveDirectory(
                    packageParentPath + "/segment/subSeg"));
    ASSERT_EQ((int64_t)3, storageMetrics.GetPackageInnerFileCount());
    ASSERT_EQ((int64_t)3, storageMetrics.GetPackageInnerDirCount());

    // delete a shared empty dir
    ASSERT_TRUE(mountTable.RemoveDirectory(
                    packageParentPath + "/segment/empty_subSeg"));
    ASSERT_EQ((int64_t)2, storageMetrics.GetPackageInnerDirCount());

    // delete root dir
    ASSERT_TRUE(mountTable.RemoveDirectory(packageParentPath));
    ASSERT_EQ((int64_t)0, storageMetrics.GetPackageInnerFileCount());
    ASSERT_EQ((int64_t)0, storageMetrics.GetPackageInnerDirCount());
    ASSERT_EQ((int64_t)0, storageMetrics.GetPackageFileCount());
}


IE_NAMESPACE_END(file_system);

