#include <fslib/fslib.h>
#include "indexlib/file_system/load_config/load_config_list.h"
#include "indexlib/misc/exception.h"
#include "indexlib/util/memory_control/memory_quota_controller.h"
#include "indexlib/util/memory_control/memory_quota_controller_creator.h"
#include "indexlib/storage/file_system_wrapper.h"
#include "indexlib/util/path_util.h"
#include "indexlib/file_system/test/indexlib_file_system_unittest.h"
#include "indexlib/file_system/test/file_system_test_util.h"
#include "indexlib/file_system/test/file_system_creator.h"
#include "indexlib/file_system/test/test_file_creator.h"
#include "indexlib/file_system/indexlib_file_system_impl.h"
#include "indexlib/file_system/slice_file_writer.h"
#include "indexlib/file_system/slice_file_reader.h"

using namespace std;
using namespace fslib;
using namespace fslib::fs;

IE_NAMESPACE_USE(storage);
IE_NAMESPACE_USE(util);
IE_NAMESPACE_USE(misc);

IE_NAMESPACE_BEGIN(file_system);
IE_LOG_SETUP(file_system, IndexlibFileSystemTest);

IndexlibFileSystemTest::IndexlibFileSystemTest()
{
}

IndexlibFileSystemTest::~IndexlibFileSystemTest()
{
}

void IndexlibFileSystemTest::CaseSetUp()
{
    mRootDir = PathUtil::JoinPath(GET_TEST_DATA_PATH(), "primary");
    mSecondaryRootDir = PathUtil::JoinPath(GET_TEST_DATA_PATH(), "secondary");
    FileSystemWrapper::MkDirIfNotExist(mRootDir);
    FileSystemWrapper::MkDirIfNotExist(mSecondaryRootDir);
    mInMemDir = PathUtil::JoinPath(mRootDir, "InMem");
    mDiskDir = PathUtil::JoinPath(mRootDir, "OnDisk");
}

void IndexlibFileSystemTest::CaseTearDown()
{
}

void IndexlibFileSystemTest::TestFileSystemInit()
{
    IndexlibFileSystemImplPtr ifs(
            new IndexlibFileSystemImpl(PathUtil::JoinPath(mRootDir, "notExistDir")));
    FileSystemOptions options;
    options.memoryQuotaController = MemoryQuotaControllerCreator::CreatePartitionMemoryController();
    ASSERT_THROW(ifs->Init(options), NonExistException);

    ifs.reset(new IndexlibFileSystemImpl(mRootDir));
    ASSERT_NO_THROW(ifs->Init(options));
}

void IndexlibFileSystemTest::TestCreateFileWriter()
{
    IndexlibFileSystemPtr fileSystem = CreateFileSystem();

    string filePath = mInMemDir + "/testfile.txt";
    FileWriterPtr fileWriter = fileSystem->CreateFileWriter(filePath);
    ASSERT_EQ(filePath, fileWriter->GetPath());
    ASSERT_EQ((size_t)0, fileWriter->GetLength());
    size_t writeSize = fileWriter->Write("abc", 3);
    ASSERT_EQ((size_t)3, writeSize);
    ASSERT_EQ((size_t)writeSize, fileWriter->GetLength());
    ASSERT_FALSE(fileSystem->IsExist(filePath));
    fileWriter->Close();
    ASSERT_TRUE(fileSystem->IsExist(filePath));
}

void IndexlibFileSystemTest::TestCreateFileReader()
{
    IndexlibFileSystemPtr ifs = CreateFileSystem();
    string filePath = mInMemDir + "/testfile.txt";
    TestFileCreator::CreateFiles(ifs, FSST_IN_MEM, FSOT_IN_MEM, "abc", filePath);

    FileReaderPtr fileReader = ifs->CreateFileReader(filePath, FSOT_IN_MEM);
    ASSERT_EQ((size_t)3, fileReader->GetLength());
    ASSERT_EQ(filePath, fileReader->GetPath());
    uint8_t buffer[1024];
    ASSERT_EQ((size_t)3, fileReader->Read(buffer, 3, 0));
    ASSERT_TRUE(memcmp("abc", buffer, 3) == 0);    
}

void IndexlibFileSystemTest::TestGetStorageMetricsForLocal()
{
    util::PartitionMemoryQuotaControllerPtr controller(
            new util::PartitionMemoryQuotaController(
                    MemoryQuotaControllerPtr(
                            new util::MemoryQuotaController(10000))));

    IndexlibFileSystemPtr ifs = FileSystemCreator::CreateFileSystem(
            mRootDir, "", LoadConfigList(), false, false, false, controller);

    string text = "disk file";
    FileSystemTestUtil::CreateDiskFiles(PathUtil::JoinPath(mRootDir, "disk/"), 5, text);

    ifs->CreateFileReader(PathUtil::JoinPath(mRootDir, "disk/0"), FSOT_IN_MEM);
    ifs->CreateFileReader(PathUtil::JoinPath(mRootDir, "disk/4"), FSOT_BUFFERED);

    const StorageMetrics& metrics = ifs->GetStorageMetrics(FSST_LOCAL);
    ASSERT_EQ((int64_t)text.size() * 2, metrics.GetTotalFileLength());
    ASSERT_EQ(2, metrics.GetTotalFileCount());
    ASSERT_EQ((int64_t)text.size(), metrics.GetFileLength(FSFT_IN_MEM));
    ASSERT_EQ((int64_t)text.size(), metrics.GetFileLength(FSFT_BUFFERED));
    ASSERT_EQ(1, metrics.GetFileCount(FSFT_IN_MEM));
    ASSERT_EQ(1, metrics.GetFileCount(FSFT_BUFFERED));

    ASSERT_EQ((int64_t)4 * 1024 * 1024, controller->GetUsedQuota());
    ifs.reset();
    ASSERT_EQ((int64_t)0, controller->GetUsedQuota());
}

void IndexlibFileSystemTest::TestGetStorageMetricsException()
{
    IndexlibFileSystemPtr ifs = FileSystemCreator::CreateFileSystem(
            mRootDir, "", LoadConfigList(), false);

    ASSERT_THROW(ifs->GetStorageMetrics(FSST_UNKNOWN), BadParameterException);
}

void IndexlibFileSystemTest::TestGetStorageMetricsForInMem()
{
    IndexlibFileSystemPtr ifs = FileSystemCreator::CreateFileSystem(
            mRootDir, "", LoadConfigList(), false);

    string text = "in memory file";
    TestFileCreator::CreateFiles(ifs, FSST_IN_MEM, FSOT_IN_MEM, text,
                                 PathUtil::JoinPath(mRootDir, "rt/"), 5);

    const StorageMetrics& metrics = ifs->GetStorageMetrics(FSST_IN_MEM);
    ASSERT_EQ((int64_t)(text.size() * 5), metrics.GetTotalFileLength());
    ASSERT_EQ(6, metrics.GetTotalFileCount());
    ASSERT_EQ((int64_t)(text.size() * 5), metrics.GetFileLength(FSFT_IN_MEM));
    ASSERT_EQ(5, metrics.GetFileCount(FSFT_IN_MEM));
}

void IndexlibFileSystemTest::TestRemoveDirectory()
{
    IndexlibFileSystemPtr ifs = CreateFileSystem();

    ASSERT_TRUE(ifs->IsExist(mInMemDir));
    ASSERT_TRUE(ifs->IsExist(mDiskDir));
    
    ASSERT_THROW(ifs->RemoveDirectory(mRootDir), BadParameterException);
    ASSERT_NO_THROW(ifs->RemoveDirectory(mInMemDir));
    ASSERT_FALSE(ifs->IsExist(mInMemDir));
    ASSERT_NO_THROW(ifs->RemoveDirectory(mDiskDir));
    ASSERT_FALSE(ifs->IsExist(mDiskDir));
}

void IndexlibFileSystemTest::TestMakeDirectory()
{
    IndexlibFileSystemPtr ifs = CreateFileSystem();
    ASSERT_TRUE(ifs->IsExist(mRootDir));
    ASSERT_TRUE(ifs->IsExist(mInMemDir));
    ASSERT_TRUE(ifs->IsExist(mDiskDir));

    ifs->MakeDirectory(mInMemDir + "/abc");
    ifs->MakeDirectory(mDiskDir + "/abc");
    ASSERT_TRUE(ifs->IsExist(mInMemDir + "/abc"));
    ASSERT_TRUE(ifs->IsExist(mDiskDir + "/abc"));

    ASSERT_THROW(ifs->MakeDirectory(mRootDir), ExistException);
    // ExistException
    ASSERT_NO_THROW(ifs->MakeDirectory(mInMemDir));
    ASSERT_NO_THROW(ifs->MakeDirectory(mDiskDir));
}

void IndexlibFileSystemTest::TestEstimateFileLockMemoryUse()
{
    string jsonStr = "{\
    \"load_config\" : [                                   \
    {                                                    \
        \"file_patterns\" : [\"cache\"],                 \
        \"load_strategy\" : \"cache\"                    \
    },                                                   \
    {                                                    \
        \"file_patterns\" : [\"lock\"],                  \
        \"load_strategy\" : \"mmap\",                    \
        \"load_strategy_param\" : {                      \
            \"lock\" : true                              \
        }                                                \
    }                                                    \
    ]}";

    LoadConfigList loadConfigList;
    FromJsonString(loadConfigList, jsonStr);
    IndexlibFileSystemPtr ifs = CreateFileSystem(loadConfigList);
    string filePath = mDiskDir + "/on_disk_file.txt";
    TestFileCreator::CreateFiles(ifs, FSST_LOCAL, FSOT_IN_MEM, "abc", filePath);

    string cacheFile = mDiskDir + "/cache_file.txt";
    TestFileCreator::CreateFiles(ifs, FSST_LOCAL, FSOT_IN_MEM, "abc", cacheFile);

    string lockFile = mDiskDir + "/lock_file.txt";
    TestFileCreator::CreateFiles(ifs, FSST_LOCAL, FSOT_IN_MEM, "abc", lockFile);

    ASSERT_EQ((size_t)3, ifs->EstimateFileLockMemoryUse(filePath, FSOT_IN_MEM));
    ASSERT_EQ((size_t)0, ifs->EstimateFileLockMemoryUse(filePath, FSOT_MMAP));
    ASSERT_EQ((size_t)3, ifs->EstimateFileLockMemoryUse(lockFile, FSOT_MMAP));
    ASSERT_EQ((size_t)0, ifs->EstimateFileLockMemoryUse(cacheFile, FSOT_CACHE));
    ASSERT_EQ((size_t)0, ifs->EstimateFileLockMemoryUse(filePath, FSOT_BUFFERED));
    ASSERT_EQ((size_t)0, ifs->EstimateFileLockMemoryUse(filePath, FSOT_LOAD_CONFIG));
    ASSERT_EQ((size_t)0, ifs->EstimateFileLockMemoryUse(cacheFile, FSOT_LOAD_CONFIG));
    ASSERT_EQ((size_t)3, ifs->EstimateFileLockMemoryUse(lockFile, FSOT_LOAD_CONFIG));
    ASSERT_EQ((size_t)3, ifs->EstimateFileLockMemoryUse(filePath, FSOT_SLICE));
}

void IndexlibFileSystemTest::TestEstimateFileLockMemoryUseWithInMemStorage()
{
    IndexlibFileSystemPtr ifs = CreateFileSystem();
    string filePath = mInMemDir + "/in_mem_file.txt";
    TestFileCreator::CreateFiles(ifs, FSST_IN_MEM, FSOT_IN_MEM, "abc", filePath);

    ASSERT_EQ((size_t)3, ifs->EstimateFileLockMemoryUse(filePath, FSOT_IN_MEM));
    ASSERT_EQ((size_t)3, ifs->EstimateFileLockMemoryUse(filePath, FSOT_SLICE));
}

void IndexlibFileSystemTest::TestCreateFileReaderWithRootlink()
{
    string jsonStr = "{\
    \"load_config\" : [                                   \
    {                                                    \
        \"file_patterns\" : [\"cache\"],                 \
        \"load_strategy\" : \"cache\"                    \
    }                                                    \
    ]}";

    LoadConfigList loadConfigList;
    FromJsonString(loadConfigList, jsonStr);

    IndexlibFileSystemPtr ifs = CreateFileSystem(loadConfigList, true);
    string filePath = mInMemDir + "/cache_file.txt";
    TestFileCreator::CreateFiles(ifs, FSST_IN_MEM, FSOT_IN_MEM, "abc", filePath);
    ifs->Sync(true);

    FileReaderPtr fileReader = ifs->CreateFileReader(filePath, FSOT_IN_MEM);
    ASSERT_EQ((size_t)3, fileReader->GetLength());
    ASSERT_TRUE(fileReader->GetBaseAddress() != NULL);
    ASSERT_EQ(filePath, fileReader->GetPath());

    filePath = PathUtil::JoinPath(ifs->GetRootLinkPath(), "InMem/cache_file.txt");
    FileReaderPtr linkFileReader = ifs->CreateFileReader(filePath, FSOT_LOAD_CONFIG);
    ASSERT_EQ((size_t)3, linkFileReader->GetLength());
    ASSERT_TRUE(linkFileReader->GetBaseAddress() == NULL);
    ASSERT_EQ(filePath, linkFileReader->GetPath());
}

void IndexlibFileSystemTest::TestCreatePackageFileWriter()
{
    InnerTestCreatePackageFileWriter(true);
    // TODO: support ondisk
    // InnerTestCreatePackageFileWriter(false);
}

void IndexlibFileSystemTest::TestIsExistInSecondaryPath()
{
    IndexlibFileSystemPtr ifs = CreateFileSystem();
    string text = "in memory file";
    FileSystemTestUtil::CreateDiskFiles(PathUtil::JoinPath(mRootDir, "disk/"), 5, text);
    FileSystemWrapper::Copy(PathUtil::JoinPath(mRootDir, "disk/"),
                            PathUtil::JoinPath(mSecondaryRootDir, "disk/"));
    FileSystemWrapper::DeleteFile(PathUtil::JoinPath(mSecondaryRootDir, "disk/0"));
    TestFileCreator::CreateFiles(ifs, FSST_IN_MEM, FSOT_IN_MEM, text,
                                 PathUtil::JoinPath(mRootDir, "rt/"), 5);
    ASSERT_TRUE(ifs->IsExist(PathUtil::JoinPath(mRootDir, "disk/0")));
    ASSERT_FALSE(ifs->IsExistInSecondaryPath(PathUtil::JoinPath(mRootDir, "disk/0")));

    ASSERT_TRUE(ifs->IsExist(PathUtil::JoinPath(mRootDir, "rt/0")));
    ASSERT_FALSE(ifs->IsExistInSecondaryPath(PathUtil::JoinPath(mRootDir, "rt/0")));
    // TODO test package file
}

void IndexlibFileSystemTest::InnerTestCreatePackageFileWriter(bool inMem)
{
    TearDown();
    SetUp();

    IndexlibFileSystemPtr ifs = CreateFileSystem();
    string packageFilePath;
    if (inMem)
    {
        packageFilePath = mInMemDir + "/package_file";
    }
    else
    {
        packageFilePath = mDiskDir + "/package_file";
    }

    // first create writer
    PackageFileWriterPtr packageFileWriter = 
        ifs->CreatePackageFileWriter(packageFilePath);
    ASSERT_TRUE(packageFileWriter);

    // get created package file writer
    PackageFileWriterPtr fileWriter = 
        ifs->GetPackageFileWriter(packageFilePath);
    ASSERT_TRUE(fileWriter);

    // re-create package file writer 
    ASSERT_THROW(ifs->CreatePackageFileWriter(packageFilePath), ExistException);

    // file writer close
    packageFileWriter->Close();

    // get closed package file writer
    ASSERT_THROW(ifs->GetPackageFileWriter(packageFilePath), UnSupportedException);

    // create closed package file writer
    ASSERT_THROW(ifs->CreatePackageFileWriter(packageFilePath), ExistException);
}

IndexlibFileSystemPtr IndexlibFileSystemTest::CreateFileSystem()
{
    return CreateFileSystem(LoadConfigList());
}

IndexlibFileSystemPtr IndexlibFileSystemTest::CreateFileSystem(
        const LoadConfigList& loadConfigList, bool useRootLink)
{
    IndexlibFileSystemPtr fileSystem = FileSystemCreator::CreateFileSystem(
            mRootDir, mSecondaryRootDir, loadConfigList, true, false, useRootLink);
    fileSystem->MountInMemStorage(mInMemDir);
    fileSystem->MakeDirectory(mDiskDir);
    
    return fileSystem;
}


IE_NAMESPACE_END(file_system);

