#include <autil/StringUtil.h>
#include <fslib/fslib.h>
#include "indexlib/misc/exception.h"
#include "indexlib/util/path_util.h"
#include "indexlib/storage/file_system_wrapper.h"
#include "indexlib/file_system/test/file_system_file_unittest.h"
#include "indexlib/file_system/test/test_file_creator.h"
#include "indexlib/file_system/test/load_config_list_creator.h"
#include "indexlib/file_system/indexlib_file_system_impl.h"
#include "indexlib/file_system/test/file_system_creator.h"
#include "indexlib/file_system/test/file_system_test_util.h"
#include "indexlib/common/byte_slice_reader.h"

using namespace std;
using namespace fslib;
using namespace autil;

IE_NAMESPACE_USE(common);
IE_NAMESPACE_USE(misc);
IE_NAMESPACE_USE(util);
IE_NAMESPACE_USE(storage);

IE_NAMESPACE_BEGIN(file_system);
IE_LOG_SETUP(file_system, FileSystemFileTest);

#define STORAGE_TYPE GET_PARAM_VALUE(0)
#define OPEN_TYPE GET_PARAM_VALUE(1)

FileSystemFileTest::FileSystemFileTest()
{
}

FileSystemFileTest::~FileSystemFileTest()
{
}

void FileSystemFileTest::CaseSetUp()
{
    mRootDir = GET_TEST_DATA_PATH();
}

void FileSystemFileTest::CaseTearDown()
{
}

void FileSystemFileTest::TestCreateFileWriter()
{
    IndexlibFileSystemPtr fileSystem = GET_FILE_SYSTEM();

    string dirPath = PathUtil::JoinPath(fileSystem->GetRootPath(), 
            "writer_dir_path");
    string filePath = PathUtil::JoinPath(dirPath, "writer_file");
    FileWriterPtr fileWriter = TestFileCreator::CreateWriter(
        fileSystem, STORAGE_TYPE, OPEN_TYPE, "", filePath, FSWriterParam(true));
    ASSERT_EQ((size_t)0, fileWriter->GetLength());
    ASSERT_EQ(filePath, fileWriter->GetPath());

    string fileContext = "hello";
    fileWriter->Write(fileContext.c_str(), fileContext.size());
    ASSERT_EQ(fileContext.size(), fileWriter->GetLength());
    
    ASSERT_FALSE(fileSystem->IsExist(filePath));
    fileWriter->Close();
    ASSERT_TRUE(fileSystem->IsExist(filePath));

    if (STORAGE_TYPE == FSST_IN_MEM)
    {
        ASSERT_TRUE(FileSystemTestUtil::CheckFileStat(fileSystem,
                        filePath, STORAGE_TYPE, FSFT_IN_MEM, FSOT_IN_MEM,
                        5, true, false, false));
    }
    else
    {
        ASSERT_TRUE(FileSystemTestUtil::CheckFileStat(fileSystem,
                        filePath, STORAGE_TYPE, FSFT_UNKNOWN, FSOT_UNKNOWN,
                        5, false, true , false));
    }
}

void FileSystemFileTest::TestCreateFileReader()
{
    IndexlibFileSystemPtr fileSystem = CreateFileSystem();

    string fileContext = "hello";
    ASSERT_NE(OPEN_TYPE, FSOT_SLICE);
    FileReaderPtr fileReader = TestFileCreator::CreateReader(
            fileSystem, STORAGE_TYPE, OPEN_TYPE, fileContext);
    ASSERT_EQ(fileContext.size(), fileReader->GetLength());

    // read without offset
    uint8_t buffer[1024];
    size_t readSize = fileReader->Read(buffer, fileContext.size());
    ASSERT_EQ(fileContext.size(), readSize);
    ASSERT_TRUE(memcmp(fileContext.data(), buffer, readSize) == 0);

    // read with offset
    ASSERT_EQ((size_t)3, fileReader->Read(buffer, 3, 2));
    ASSERT_TRUE(memcmp("llo", buffer, 3) == 0);

    // read with slicelist
    if (OPEN_TYPE != FSOT_BUFFERED)
    {
        ByteSliceList* byteSliceList = fileReader->Read((size_t)3, 1);
        ASSERT_TRUE(byteSliceList != NULL);
        ByteSliceReader sliceListReader(byteSliceList);
        void* data;
        ASSERT_EQ(3u, sliceListReader.ReadMayCopy(data, 3));
        ASSERT_EQ(0, memcmp(data, "ell", 3));
        delete byteSliceList;
    }

    fileReader->Close();
}

void FileSystemFileTest::TestReadFileInPackage()
{
    IndexlibFileSystemPtr fileSystem = CreateFileSystem();
    ASSERT_NE(OPEN_TYPE, FSOT_SLICE);
    
    vector<string> innerFileContents;
    innerFileContents.push_back("hello");
    innerFileContents.push_back("world");
    innerFileContents.push_back("123456");
    innerFileContents.push_back("");

    vector<FileReaderPtr> fileReaders = 
        TestFileCreator::CreateInPackageFileReader(
            fileSystem, STORAGE_TYPE, OPEN_TYPE, innerFileContents);
    for (size_t i = 0; i < innerFileContents.size(); i++)
    {
        CheckInnerFile(innerFileContents[i], fileReaders[i], OPEN_TYPE);
        fileReaders[i].reset();
    }

    // check reader after cleanCache and reload from disk
    fileSystem->Sync(true);
    fileSystem->CleanCache();
    StorageMetrics storageMetrics = 
        fileSystem->GetStorageMetrics(STORAGE_TYPE);
    ASSERT_EQ((int64_t)0, storageMetrics.GetTotalFileLength());

    fileReaders = TestFileCreator::CreateInPackageFileReader(
            fileSystem, STORAGE_TYPE, OPEN_TYPE, innerFileContents);
    for (size_t i = 0; i < innerFileContents.size(); i++)
    {
        CheckInnerFile(innerFileContents[i], fileReaders[i], OPEN_TYPE);
        fileReaders[i].reset();
    }
}

void FileSystemFileTest::CheckInnerFile(const string& fileContent,
                                        const FileReaderPtr& fileReader,
                                        FSOpenType openType)
{
    ASSERT_EQ(openType, fileReader->GetOpenType());
    ASSERT_EQ(fileContent.size(), fileReader->GetLength());
    ASSERT_TRUE(fileReader->GetFileNode()->IsInPackage());

    // read without offset
    uint8_t buffer[1024];
    size_t readSize = fileReader->Read(buffer, fileContent.size());
    ASSERT_EQ(fileContent.size(), readSize);
    ASSERT_EQ(fileContent, string((char*)buffer, readSize));

    // read with offset
    if (fileContent.size() > 3)
    {
        size_t remainLen = fileContent.size() - 3;
        string expectValue = fileContent.substr(3);
        ASSERT_EQ(remainLen, fileReader->Read(buffer, remainLen, 3));
        ASSERT_EQ(expectValue, string((char*)buffer, remainLen));

        if (openType != FSOT_BUFFERED)
        {
            // read with slicelist
            ByteSliceList* byteSliceList = fileReader->Read((size_t)remainLen, 3);
            ByteSliceReader reader(byteSliceList);
            vector<char> data(' ', remainLen);
            ASSERT_EQ(remainLen, reader.Read(data.data(), remainLen));
            ASSERT_EQ(expectValue, string(data.data(), remainLen));
            delete byteSliceList;
        }
    }
    fileReader->Close();
}

void FileSystemFileTest::TestCreateFileReaderWithEmpthFile()
{
    IndexlibFileSystemPtr fileSystem = CreateFileSystem();

    ASSERT_NE(OPEN_TYPE, FSOT_SLICE);
    FileReaderPtr fileReader = TestFileCreator::CreateReader(
            fileSystem, STORAGE_TYPE, OPEN_TYPE, "");
    ASSERT_EQ((size_t)0, fileReader->GetLength());

    // read without offset
    uint8_t buffer[1024];
    ASSERT_EQ((size_t)0, fileReader->Read(buffer, 0));

    // read with offset
    ASSERT_EQ((size_t)0, fileReader->Read(buffer, 0, 0));

    // read with slicelist
    if (OPEN_TYPE != FSOT_BUFFERED)
    {
        ByteSliceListPtr byteSliceList(fileReader->Read((size_t)0, 0));
        ASSERT_TRUE(byteSliceList != NULL);
        ASSERT_EQ(0u, byteSliceList->GetTotalSize());
    }

    fileReader->Close();
}

void FileSystemFileTest::TestReadByteSliceForFileReader()
{
    IndexlibFileSystemPtr fileSystem = CreateFileSystem();
    
    string fileContext = "hello";
    FileReaderPtr fileReader = TestFileCreator::CreateReader(
            fileSystem, STORAGE_TYPE, OPEN_TYPE, fileContext);
    if (OPEN_TYPE != FSOT_BUFFERED)
    {
        uint8_t buffer[1024];
        size_t readSize = fileReader->Read(buffer, fileContext.size(), 0);
        ASSERT_EQ(fileContext.size(), readSize);
        ASSERT_EQ(0, memcmp(fileContext.data(), buffer, readSize));

        ByteSliceList* byteSliceList = fileReader->Read(fileContext.size(), 0);
        ASSERT_TRUE(byteSliceList);
        ByteSliceReader reader(byteSliceList);
        ASSERT_EQ(fileContext.size(), reader.Read(buffer, fileContext.size()));
        ASSERT_EQ(0, memcmp(fileContext.data(), buffer, fileContext.size()));
        delete byteSliceList;
    }
    else
    {
        ASSERT_THROW(fileReader->Read(fileContext.size(), 0),
                     UnSupportedException);
    }
}

void FileSystemFileTest::TestGetBaseAddressForFileReader()
{
    IndexlibFileSystemPtr fileSystem = CreateFileSystem();
    
    string fileContext = "hello";
    FileReaderPtr fileReader = TestFileCreator::CreateReader(
            fileSystem, STORAGE_TYPE, OPEN_TYPE, fileContext);
    if (OPEN_TYPE == FSOT_IN_MEM || OPEN_TYPE == FSOT_MMAP)
    {
        char* data = (char*) fileReader->GetBaseAddress();
        ASSERT_TRUE(data);
        ASSERT_EQ(0, memcmp(fileContext.data(), data, fileContext.size()));
    }
    else
    {
        ASSERT_FALSE(fileReader->GetBaseAddress());
    }
}

void FileSystemFileTest::TestGetBaseAddressForFileReaderWithEmptyFile()
{
    IndexlibFileSystemPtr fileSystem = CreateFileSystem();
    FileReaderPtr fileReader = TestFileCreator::CreateReader(
            fileSystem, STORAGE_TYPE, OPEN_TYPE, "");
    ASSERT_FALSE(fileReader->GetBaseAddress());
}

void FileSystemFileTest::TestStorageMetrics()
{
    IndexlibFileSystemPtr fileSystem = CreateFileSystem();

    // may be we need CreateReader with FSFileType in test file creator
    FSFileType fileType;
    switch(OPEN_TYPE)
    {
    case FSOT_IN_MEM:      fileType = FSFT_IN_MEM; break;
    case FSOT_MMAP:     fileType = FSFT_MMAP; break;
    case FSOT_CACHE:       fileType = FSFT_BLOCK; break;
    case FSOT_BUFFERED:    fileType = FSFT_BUFFERED; break;
    case FSOT_LOAD_CONFIG: fileType = FSFT_BLOCK; break;
    default:               fileType = FSFT_UNKNOWN; break;
    }

    CheckStorageMetrics(fileSystem, STORAGE_TYPE, fileType, 0, 0, __LINE__);
    FileReaderPtr fileReader = TestFileCreator::CreateReader(
            fileSystem, STORAGE_TYPE, OPEN_TYPE, "hello");
    ASSERT_EQ(OPEN_TYPE, fileReader->GetOpenType());
    CheckStorageMetrics(fileSystem, STORAGE_TYPE, fileType, 1, 5, __LINE__);
    fileSystem->Sync(true);
    fileReader.reset();
    fileSystem->CleanCache();
    CheckStorageMetrics(fileSystem, STORAGE_TYPE, fileType, 0, 0, __LINE__);
}

void FileSystemFileTest::TestRemoveFileInCache()
{
    IndexlibFileSystemPtr fileSystem = GET_FILE_SYSTEM();

    ASSERT_NE(OPEN_TYPE, FSOT_SLICE);
    FileWriterPtr fileWriter = TestFileCreator::CreateWriter(
        fileSystem, STORAGE_TYPE, OPEN_TYPE, "", "", FSWriterParam(true));
    string filePath = fileWriter->GetPath();
    ASSERT_FALSE(fileSystem->IsExist(filePath));
    fileWriter->Close();
    fileWriter.reset();

    ASSERT_TRUE(fileSystem->IsExist(filePath));
    fileSystem->RemoveFile(filePath);
    ASSERT_FALSE(fileSystem->IsExist(filePath));
}

void FileSystemFileTest::TestRemoveFileOnDisk()
{
    IndexlibFileSystemPtr fileSystem = CreateFileSystem();
    ASSERT_NE(OPEN_TYPE, FSOT_SLICE);
    FileReaderPtr fileReader = TestFileCreator::CreateReader(
            fileSystem, STORAGE_TYPE, OPEN_TYPE, "hello");
    string filePath = fileReader->GetPath();
    fileReader.reset();
    fileSystem->Sync(true);
    fileSystem->CleanCache();

    FileStat fileStat;
    fileSystem->GetFileStat(filePath, fileStat);
    ASSERT_TRUE(fileStat.onDisk);
    ASSERT_FALSE(fileStat.inCache);
    ASSERT_TRUE(fileSystem->IsExist(filePath));
    fileSystem->RemoveFile(filePath);
    ASSERT_FALSE(fileSystem->IsExist(filePath));
}

void FileSystemFileTest::TestRemoveFileInFlushQueue()
{
    IndexlibFileSystemPtr fileSystem = CreateFileSystem();
    ASSERT_NE(OPEN_TYPE, FSOT_SLICE);
    // normal file
    FileReaderPtr fileReader = TestFileCreator::CreateReader(
            fileSystem, STORAGE_TYPE, OPEN_TYPE, "hello");
    string filePath = fileReader->GetPath();
    fileReader.reset();
    FileStat fileStat;
    fileSystem->GetFileStat(filePath, fileStat);
    ASSERT_EQ(STORAGE_TYPE != FSST_IN_MEM, fileStat.onDisk);
    ASSERT_TRUE(fileStat.inCache);
    ASSERT_TRUE(fileSystem->IsExist(filePath));
    // in mem file
    string inMemPath = PathUtil::JoinPath(fileSystem->GetRootPath(), "inmem");
    InMemoryFilePtr inMemFile = fileSystem->CreateInMemoryFile(inMemPath, 10);
    fileSystem->GetFileStat(inMemPath, fileStat);
    ASSERT_FALSE(fileStat.onDisk);
    ASSERT_TRUE(fileStat.inCache);
    ASSERT_TRUE(fileSystem->IsExist(inMemPath));

    fileSystem->CleanCache();

    inMemFile.reset();
    fileSystem->RemoveFile(inMemPath); 
    ASSERT_TRUE(fileSystem->IsExist(filePath));
    ASSERT_FALSE(fileSystem->IsExist(inMemPath));

    // remove does not trigger sync
    fileSystem->GetFileStat(filePath, fileStat);
    ASSERT_EQ(STORAGE_TYPE != FSST_IN_MEM, fileStat.onDisk);

    fileSystem->Sync(true);
    fileSystem->GetFileStat(filePath, fileStat);
    ASSERT_TRUE(fileStat.onDisk);
}

void FileSystemFileTest::TestCacheHitWhenFileOnDisk()
{
    IndexlibFileSystemPtr fileSystem = CreateFileSystem();

    FileWriterPtr writer = 
        TestFileCreator::CreateWriter(fileSystem, STORAGE_TYPE, OPEN_TYPE, "hello");
    writer->Close();
    // flush file to disk
    fileSystem->Sync(true);
    fileSystem->CleanCache();

    FileReaderPtr fileReader1 = TestFileCreator::CreateReader(
            fileSystem, STORAGE_TYPE, OPEN_TYPE, "");
    FileReaderPtr fileReader2 = TestFileCreator::CreateReader(
            fileSystem, STORAGE_TYPE, OPEN_TYPE, "");
    ASSERT_EQ(OPEN_TYPE, fileReader1->GetOpenType());
    ASSERT_EQ(fileReader1->GetFileNode(), fileReader2->GetFileNode());
    CheckFileCacheMetrics(fileSystem, STORAGE_TYPE, 1, 1, 0, __LINE__);
}

void FileSystemFileTest::TestCacheHitWhenFileInCacheForInMemStorage()
{
    IndexlibFileSystemPtr fileSystem = CreateFileSystem();
    FileReaderPtr fileReader1 = TestFileCreator::CreateReader(
            fileSystem, FSST_IN_MEM, FSOT_IN_MEM, "hello");
    FileReaderPtr fileReader2 = TestFileCreator::CreateReader(
            fileSystem, FSST_IN_MEM, FSOT_IN_MEM, "");
    ASSERT_EQ(FSOT_IN_MEM, fileReader1->GetOpenType());
    ASSERT_EQ(fileReader1->GetFileNode(), fileReader2->GetFileNode());
    CheckFileCacheMetrics(fileSystem, FSST_IN_MEM, 0, 2, 0, __LINE__);
}

void FileSystemFileTest::TestForDisableCache()
{
    RESET_FILE_SYSTEM(LoadConfigList(), false, false);
    IndexlibFileSystemPtr fileSystem = GET_FILE_SYSTEM();

    FileReaderPtr fileReader1 = TestFileCreator::CreateReader(
            fileSystem, FSST_LOCAL, FSOT_IN_MEM, "hello");
    FileStat fileStat;
    fileSystem->GetFileStat(fileReader1->GetPath(), fileStat);
    ASSERT_FALSE(fileStat.inCache);

    FileReaderPtr fileReader2 = TestFileCreator::CreateReader(
            fileSystem, FSST_LOCAL, FSOT_IN_MEM, "");
    fileSystem->GetFileStat(fileReader2->GetPath(), fileStat);
    ASSERT_FALSE(fileStat.inCache);

    ASSERT_EQ(FSOT_IN_MEM, fileReader1->GetOpenType());
    ASSERT_NE(fileReader1->GetFileNode(), fileReader2->GetFileNode());
}

void FileSystemFileTest::TestCacheHitReplace()
{
    IndexlibFileSystemPtr fileSystem = CreateFileSystem();

    for (int type = 0; type < FSOT_SLICE; ++type)
    {
        if (type == FSOT_BUFFERED)
        {
            continue;
        }
        ResetFileSystem(fileSystem);
        FileReaderPtr fileReader = TestFileCreator::CreateReader(
                fileSystem, STORAGE_TYPE, OPEN_TYPE, "hello");
        ASSERT_EQ(OPEN_TYPE, fileReader->GetOpenType());
        FSOpenType newOpenType = (FSOpenType)type;
        fileReader.reset();
        fileReader = TestFileCreator::CreateReader(
                fileSystem, STORAGE_TYPE, newOpenType, "");
        ASSERT_EQ(newOpenType, fileReader->GetOpenType());
        if (newOpenType == OPEN_TYPE)
        {
            continue;
        }
        CheckFileCacheMetrics(fileSystem, STORAGE_TYPE, 1, 0, 1, __LINE__);
    }
}

void FileSystemFileTest::TestCreateFileWriterForSync()
{
    IndexlibFileSystemPtr fileSystem = CreateFileSystem();
 
    string filePath = GET_ABS_PATH("inmem/file");
    TestFileCreator::CreateFiles(fileSystem, FSST_IN_MEM, FSOT_IN_MEM,
                                 "hello", filePath);
    ASSERT_FALSE(FileSystemTestUtil::IsOnDisk(fileSystem, filePath));

    FileWriterPtr fileWriter = fileSystem->CreateFileWriter(GET_ABS_PATH("disk"));
    fileWriter->Close();
    ASSERT_TRUE(FileSystemTestUtil::IsOnDisk(fileSystem, filePath));
}

void FileSystemFileTest::TestCacheHitException()
{
    IndexlibFileSystemPtr fileSystem = CreateFileSystem();

    ASSERT_NE(OPEN_TYPE, FSOT_SLICE);
    FileReaderPtr fileReader1 = TestFileCreator::CreateReader(
            fileSystem, STORAGE_TYPE, OPEN_TYPE, "hello");
    
    for (int type = 0; type < FSOT_UNKNOWN; ++type)
    {
        if (type == FSOT_BUFFERED)
        {
            continue;
        }
        FSOpenType fsOpenType = (FSOpenType)type;
        if (fsOpenType != OPEN_TYPE)
        {
            ASSERT_THROW(TestFileCreator::CreateReader(fileSystem, STORAGE_TYPE, 
                            fsOpenType, ""), InconsistentStateException);
        }
        CheckFileCacheMetrics(fileSystem, STORAGE_TYPE, 1, 0, 0, __LINE__);
    }
}

void FileSystemFileTest::TestCreateFileReaderNotSupport()
{
    IndexlibFileSystemPtr fileSystem = GET_FILE_SYSTEM();

    ASSERT_THROW(TestFileCreator::CreateReader(fileSystem,
                    STORAGE_TYPE, OPEN_TYPE, "hello"), UnSupportedException);
}

void FileSystemFileTest::CheckStorageMetrics(const IndexlibFileSystemPtr& ifs,
        FSStorageType storageType, FSFileType fileType,
        int64_t fileCount, int64_t fileLength, int lineNo)
{
    SCOPED_TRACE(string("see line:") + StringUtil::toString(lineNo) + "]");

    StorageMetrics metrics = ifs->GetStorageMetrics(storageType);
    ASSERT_EQ(fileCount, metrics.GetFileCount(fileType));
    ASSERT_EQ(fileLength, metrics.GetFileLength(fileType));
}

void FileSystemFileTest::CheckFileCacheMetrics(
        const IndexlibFileSystemPtr& ifs, FSStorageType storageType,
        int64_t missCount, int64_t hitCount, int64_t replaceCount,
        int lineNo)
{
    SCOPED_TRACE(string("see line:") + StringUtil::toString(lineNo) + "]");

    StorageMetrics metrics = ifs->GetStorageMetrics(storageType);
    ASSERT_EQ(missCount, metrics.GetFileCacheMetrics().missCount.getValue());
    ASSERT_EQ(hitCount, metrics.GetFileCacheMetrics().hitCount.getValue());
    ASSERT_EQ(replaceCount, metrics.GetFileCacheMetrics().replaceCount.getValue());
}

void FileSystemFileTest::MakeStorageRoot(IndexlibFileSystemPtr ifs, 
        FSStorageType storageType, const string& rootPath)
{
    if (storageType == FSST_IN_MEM)
    {
        ifs->MountInMemStorage(rootPath);
    }
    else
    {
        ifs->MakeDirectory(rootPath);
    }
}

IndexlibFileSystemPtr FileSystemFileTest::CreateFileSystem()
{
    // cache has not default creator when create file reader
    LoadConfigList loadConfigList = 
        LoadConfigListCreator::CreateLoadConfigList(READ_MODE_CACHE);
    IndexlibFileSystemPtr fileSystem = FileSystemCreator::CreateFileSystem(
            mRootDir, "", loadConfigList, true);
    return fileSystem;
}

void FileSystemFileTest::ResetFileSystem(IndexlibFileSystemPtr& fileSystem)
{
    if (fileSystem)
    {
        fileSystem->Sync(true);
    }
    fileSystem.reset();
    TestDataPathTearDown();
    TestDataPathSetup();
    
    fileSystem = CreateFileSystem();
}

IE_NAMESPACE_END(file_system);

