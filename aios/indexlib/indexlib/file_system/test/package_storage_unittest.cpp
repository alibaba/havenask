#include "indexlib/misc/exception.h"
#include "indexlib/file_system/test/package_storage_unittest.h"
#include "indexlib/file_system/indexlib_file_system_creator.h"
#include "indexlib/file_system/buffered_package_file_writer.h"
#include "indexlib/storage/file_system_wrapper.h"
#include "indexlib/util/env_util.h"
#include "indexlib/util/memory_control/memory_quota_controller_creator.h"
#include "indexlib/file_system/directory_merger.h"

using namespace std;
using namespace autil;

IE_NAMESPACE_USE(misc);
IE_NAMESPACE_USE(util);
IE_NAMESPACE_USE(storage);

IE_NAMESPACE_BEGIN(file_system);
IE_LOG_SETUP(file_system, PackageStorageTest);

PackageStorageTest::PackageStorageTest()
{
}

PackageStorageTest::~PackageStorageTest()
{
}

void PackageStorageTest::CaseSetUp()
{
    mRootDir = PathUtil::JoinPath(GET_TEST_DATA_PATH(), "root");
    FileSystemWrapper::MkDirIfNotExist(mRootDir);
    
    FileSystemOptions fsOptions;
    mFs = IndexlibFileSystemCreator::Create(mRootDir, "", NULL, fsOptions);
}

void PackageStorageTest::CaseTearDown()
{
    
}

void PackageStorageTest::TestSimpleProcess()
{
    string packageRoot = PathUtil::JoinPath(mRootDir, "segment_0");
    FileSystemWrapper::MkDir(packageRoot);
    mFs->MountPackageStorage(packageRoot, "i1_t1");
    const PackageStoragePtr& storage = mFs->GetPackageStorage();
    
    DirectoryPtr directory = DirectoryCreator::Get(mFs, packageRoot, true);
    FileWriterPtr writer = directory->CreateFileWriter("data1");
    auto bufferedPackageFileWriter = DYNAMIC_POINTER_CAST(BufferedPackageFileWriter, writer);
    ASSERT_TRUE(bufferedPackageFileWriter);
    writer->Write("1234567890ABCDEF", 16);
    writer->Close();
    fslib::FileList fileList;
    FileSystemWrapper::ListDir(packageRoot, fileList);
    ASSERT_TRUE(fileList.empty());
    ASSERT_TRUE(storage->Sync());
    
    FileSystemWrapper::ListDir(packageRoot, fileList);
    ASSERT_EQ(1, fileList.size());
}

PackageStoragePtr PackageStorageTest::CreateStorage(int32_t recoverMetaId)
{
    string packageRoot = PathUtil::JoinPath(mRootDir, "segment_0");
    FileSystemWrapper::MkDirIfNotExist(packageRoot);
    mFs->MountPackageStorage(packageRoot, "i1_t1");
    fslib::FileList fileList;
    FileSystemWrapper::ListDir(packageRoot, fileList);
    mFs->GetPackageStorage()->Recover(packageRoot, recoverMetaId, fileList);
    return mFs->GetPackageStorage();
}

void PackageStorageTest::AssertInnerFileMeta(
        const PackageFileMeta::InnerFileMeta& meta, const string& expectPath, bool expectIsDir, 
        size_t expectOffset, size_t expectLength, uint32_t expectFileIdx)
{
    PackageFileMeta::InnerFileMeta expectMeta(expectPath, expectIsDir);
    if (!expectIsDir)
    {
        expectMeta.Init(expectOffset, expectLength, expectFileIdx);
    }
    ASSERT_EQ(expectMeta, meta) << expectMeta.GetFilePath() << ":" << meta.GetFilePath();
}

void PackageStorageTest::TestPackageFileMeta()
{
    const PackageStoragePtr& storage = CreateStorage();
    string packageRoot = PathUtil::JoinPath(mRootDir, "segment_0");
    DirectoryPtr directory = DirectoryCreator::Get(mFs, packageRoot, true);
    FileWriterPtr writer1 = directory->CreateFileWriter("data1");
    FileWriterPtr writer2 = directory->CreateFileWriter("data2");
    writer1->Write("1234567890ABCDEF", 16);
    writer2->Write("", 0);
    writer1->Close();
    writer2->Close();
    DirectoryPtr subDirectory = directory->MakeDirectory("dir1/dir2");
    FileWriterPtr writer3 = subDirectory->CreateFileWriter("data3");
    writer3->Write("1234567890", 10);
    writer3->Close();
    ASSERT_THROW(storage->MakeDirectory(packageRoot + "/dir3/dir4", false), NonExistException);
    storage->MakeDirectory(packageRoot + "/dir3", false);
    storage->MakeDirectory(packageRoot + "/dir3/dir4", false);
    storage->Commit();
    
    fslib::FileList fileList;
    FileSystemWrapper::ListDir(packageRoot, fileList);
    ASSERT_EQ(2, fileList.size());
    
    const VersionedPackageFileMeta& meta = GetPackageMeta(packageRoot, "i1_t1", 0);
    ASSERT_EQ(4096, meta.GetFileAlignSize());
    ASSERT_EQ(7, meta.GetInnerFileCount());
    
    const auto& metas = meta.mFileMetaVec;
    AssertInnerFileMeta(metas[0], "dir1", true);
    AssertInnerFileMeta(metas[1], "dir1/dir2", true);
    AssertInnerFileMeta(metas[2], "dir3", true);
    AssertInnerFileMeta(metas[3], "dir3/dir4", true);
    AssertInnerFileMeta(metas[4], "data1", false, 0, 16, 0);
    AssertInnerFileMeta(metas[5], "data2", false, 4096, 0, 0);
    AssertInnerFileMeta(metas[6], "dir1/dir2/data3", false, 4096, 10, 0);

}

void PackageStorageTest::TestCommitMultiTimes()
{
    const PackageStoragePtr& storage = CreateStorage();
    string packageRoot = PathUtil::JoinPath(mRootDir, "segment_0");
    DirectoryPtr directory = DirectoryCreator::Get(mFs, packageRoot, true);
    
    FileWriterPtr writer1 = directory->CreateFileWriter("data1");
    writer1->Write("1234567890", 10);
    writer1->Close();
    storage->Commit();
    
    FileWriterPtr writer2 = directory->CreateFileWriter("data2");
    writer2->Write("ABCDEFG", 7);
    writer2->Close();
    storage->Commit();

    fslib::FileList fileList;
    FileSystemWrapper::ListDir(packageRoot, fileList);
    ASSERT_EQ(4, fileList.size());
    
    // PrintPackageMeta(packageRoot, "i1_t1", 0);
    // PrintPackageMeta(packageRoot, "i1_t1", 1);
    
    const VersionedPackageFileMeta& meta = GetPackageMeta(packageRoot, "i1_t1", 1);
    ASSERT_EQ(2, meta.GetInnerFileCount());
    const auto& metas = meta.mFileMetaVec;
    AssertInnerFileMeta(metas[0], "data1", false, 0, 10, 0);
    AssertInnerFileMeta(metas[1], "data2", false, 0, 7, 1);
}

void PackageStorageTest::TestRecover()
{
    string packageRoot = PathUtil::JoinPath(mRootDir, "segment_0");
    
    {
        const PackageStoragePtr& storage = CreateStorage();
        DirectoryPtr directory = DirectoryCreator::Get(mFs, packageRoot, true);
        FileWriterPtr writer1 = directory->CreateFileWriter("data1");
        writer1->Write("1234567890", 10);
        writer1->Close();
        storage->Commit();

        // FileWriterPtr writer2 = directory->CreateFileWriter("data2");
        // writer2->Write("ABCDEFG", 7);
        // writer2->Close();
    }
    
    CaseTearDown();
    CaseSetUp();
    
    {
        const PackageStoragePtr& storage2 = CreateStorage(0);
        DirectoryPtr directory = DirectoryCreator::Get(mFs, packageRoot, true);
        ASSERT_TRUE(directory->IsExist("data1"));
        ASSERT_FALSE(directory->IsExist("data2"));
    
        FileWriterPtr writer3 = directory->CreateFileWriter("data3");
        writer3->Write("abc", 3);
        writer3->Close();
        storage2->Commit();

        // PrintPackageMeta(packageRoot, "i1_t1", 0);
        // PrintPackageMeta(packageRoot, "i1_t1", 1);
        const VersionedPackageFileMeta& meta = GetPackageMeta(packageRoot, "i1_t1", 1);
        ASSERT_EQ(2, meta.GetInnerFileCount());
        const auto& metas = meta.mFileMetaVec;
        AssertInnerFileMeta(metas[0], "data1", false, 0, 10, 0);
        AssertInnerFileMeta(metas[1], "data3", false, 0, 3, 1);
    }
}

void PackageStorageTest::TestRecoverAfterCommitMoreThen10Times()
{
    string packageRoot = PathUtil::JoinPath(mRootDir, "segment_0");
    int successCommitTime = 12;
    {
        const PackageStoragePtr& storage = CreateStorage();
        for (int i = 0; i < successCommitTime; ++i)
        {
            string fileName = "data_" + StringUtil::toString(i);
            string content = "success commit file: " + StringUtil::toString(i);
            
            DirectoryPtr directory = DirectoryCreator::Get(mFs, packageRoot, true);
            FileWriterPtr writer = directory->CreateFileWriter(fileName);
            writer->Write(content.c_str(), content.size());
            writer->Close();
            storage->Commit();
        }
        // DirectoryPtr directory = DirectoryCreator::Get(mFs, packageRoot, true);
        // FileWriterPtr writer = directory->CreateFileWriter("data");
        // writer->Write("ABCDEFG", 7);
        // writer->Close();
    }
    
    CaseTearDown();
    CaseSetUp();
    CreateStorage(successCommitTime - 1)->Close();  // for recover
    DirectoryMerger::MergePackageFiles(packageRoot);
    
    DirectoryPtr directory = DirectoryCreator::Create(packageRoot);
    directory->MountPackageFile("package_file");
    for (int i = 0; i < successCommitTime; ++i)
    {
        string fileName = "data_" + StringUtil::toString(i);
        string expectContent = "success commit file: " + StringUtil::toString(i);
            
        EXPECT_TRUE(directory->IsExist(fileName)) << "file [" << fileName << "] not exist";
        string actualContent;
        ASSERT_NO_THROW(directory->Load(fileName, actualContent));
        EXPECT_EQ(expectContent, actualContent);
    }
    ASSERT_FALSE(directory->IsExist("data"));
}

VersionedPackageFileMeta PackageStorageTest::GetPackageMeta(
    const string& root, const string& description, int32_t metaVersionId)
{
    string path = VersionedPackageFileMeta::GetPackageMetaFileName(description, metaVersionId);
    VersionedPackageFileMeta packageMeta;
    packageMeta.Load(PathUtil::JoinPath(root, path));
    return packageMeta;
}

void PackageStorageTest::PrintPackageMeta(
    const string& root, const string& description, int32_t metaVersionId)
{
    const VersionedPackageFileMeta& packageMeta = GetPackageMeta(root, description, metaVersionId);
    cout << "=== " << PathUtil::JoinPath(root, VersionedPackageFileMeta::GetPackageMetaFileName(description, metaVersionId)) << " ===" << endl;
    cout << "FileCount: " << packageMeta.GetInnerFileCount()
         << " AlignSize: " << packageMeta.GetFileAlignSize() << endl;
    for (auto iter = packageMeta.Begin(); iter != packageMeta.End(); iter++)
    {
        cout << iter->GetFilePath() << endl;
        cout << "IsDir[" << iter->IsDir() << "]";
        if (!iter->IsDir())
        {
            cout << "  Offset[" << iter->GetOffset() << "]"
                 << "  Length[" << iter->GetLength() << "]"
                 << "  FileIdx[" << iter->GetDataFileIdx() << "]";
        }
        cout << endl;
    }
    cout << "============================================" << endl;
}

void PackageStorageTest::TestDirectorOnly()
{
    string packageRoot = PathUtil::JoinPath(mRootDir, "segment_0");
    FileSystemWrapper::MkDirIfNotExist(packageRoot);
    PackageStorage packageStorage(mRootDir, "test", MemoryQuotaControllerCreator::CreateBlockMemoryController());
    packageStorage.Init(FileSystemOptions());
    packageStorage.MakeRoot(packageRoot);
    packageStorage.MakeDirectory(PathUtil::JoinPath(packageRoot, "index"));
    packageStorage.Commit();
    DirectoryMerger::MergePackageFiles(packageRoot);
    ASSERT_TRUE(FileSystemWrapper::IsExist(PathUtil::JoinPath(packageRoot, "package_file.__data__0")));
    ASSERT_TRUE(FileSystemWrapper::IsExist(PathUtil::JoinPath(packageRoot, "package_file.__meta__")));
    PackageFileMeta packageFileMeta;
    packageFileMeta.Load(packageRoot);
    ASSERT_EQ(1, packageFileMeta.GetPhysicalFileNames().size());
    ASSERT_EQ(1, packageFileMeta.GetPhysicalFileTags().size());
    ASSERT_EQ(1, packageFileMeta.GetPhysicalFileLengths().size());
    ASSERT_EQ("package_file.__data__0", packageFileMeta.GetPhysicalFileName(0));
    ASSERT_EQ("", packageFileMeta.GetPhysicalFileTag(0));
    ASSERT_EQ(0, packageFileMeta.GetPhysicalFileLength(0));
}

IE_NAMESPACE_END(file_system);

