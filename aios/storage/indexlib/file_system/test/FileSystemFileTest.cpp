#include "gtest/gtest.h"
#include <future>
#include <iosfwd>
#include <memory>
#include <stdint.h>
#include <string.h>
#include <string>
#include <vector>

#include "autil/CommonMacros.h"
#include "autil/Log.h"
#include "fslib/common/common_type.h"
#include "indexlib/file_system/ByteSliceReader.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/FileSystemCreator.h"
#include "indexlib/file_system/FileSystemDefine.h"
#include "indexlib/file_system/FileSystemOptions.h"
#include "indexlib/file_system/IFileSystem.h"
#include "indexlib/file_system/RemoveOption.h"
#include "indexlib/file_system/WriterOption.h"
#include "indexlib/file_system/file/FileNode.h"
#include "indexlib/file_system/file/FileReader.h"
#include "indexlib/file_system/file/FileWriter.h"
#include "indexlib/file_system/load_config/LoadConfigList.h"
#include "indexlib/file_system/test/FileSystemTestUtil.h"
#include "indexlib/file_system/test/LoadConfigListCreator.h"
#include "indexlib/file_system/test/TestFileCreator.h"
#include "indexlib/util/PathUtil.h"
#include "indexlib/util/byte_slice_list/ByteSliceList.h"
#include "indexlib/util/testutil/unittest.h"

namespace indexlib { namespace util {
class InconsistentStateException;
class UnSupportedException;
}} // namespace indexlib::util

using namespace std;
using namespace autil;

using namespace indexlib::util;

namespace indexlib { namespace file_system {

typedef std::tuple<FSStorageType, FSOpenType> FileSystemType;

class FileSystemFileTest : public INDEXLIB_TESTBASE_WITH_PARAM<FileSystemType>
{
public:
    FileSystemFileTest();
    ~FileSystemFileTest();

    DECLARE_CLASS_NAME(FileSystemFileTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;

    void TestCreateFileWriter();
    void TestCreateFileReader();
    void TestCreateFileReaderWithEmpthFile();
    void TestGetBaseAddressForFileReader();
    void TestGetBaseAddressForFileReaderWithEmptyFile();
    void TestReadByteSliceForFileReader();
    void TestStorageMetrics();
    void TestRemoveFileInCache();
    void TestRemoveFileOnDisk();
    void TestRemoveFileInFlushQueue();
    void TestCacheHitWhenFileOnDisk();
    void TestCacheHitWhenFileInCacheForInMemStorage();
    void TestForDisableCache();
    void TestCacheHitReplace();
    void TestCreateFileReaderNotSupport();
    void TestCreateFileWriterForSync();
    void TestReadFileInPackage();

private:
    void MakeStorageRoot(std::shared_ptr<IFileSystem> ifs, FSStorageType storageType, const std::string& rootPath);
    std::shared_ptr<IFileSystem> CreateFileSystem();
    void ResetFileSystem(std::shared_ptr<IFileSystem>& ifs);
    void CheckStorageMetrics(const std::shared_ptr<IFileSystem>& ifs, FSStorageType storageType, FSFileType fileType,
                             int64_t fileCount, int64_t fileLength, int lineNo);
    void CheckFileCacheMetrics(const std::shared_ptr<IFileSystem>& ifs, FSStorageType storageType, int64_t missCount,
                               int64_t hitCount, int64_t replaceCount, int lineNO);

    void CheckInnerFile(const std::string& fileContent, const std::shared_ptr<FileReader>& fileReader,
                        FSOpenType openType);

private:
    std::shared_ptr<Directory> _directory;
    std::shared_ptr<IFileSystem> _fileSystem;

private:
    AUTIL_LOG_DECLARE();
};
AUTIL_LOG_SETUP(indexlib.file_system, FileSystemFileTest);

INDEXLIB_UNIT_TEST_CASE(FileSystemFileTest, TestForDisableCache);
INDEXLIB_UNIT_TEST_CASE(FileSystemFileTest, TestCreateFileWriterForSync);
INDEXLIB_UNIT_TEST_CASE(FileSystemFileTest, TestCacheHitWhenFileInCacheForInMemStorage);

// Test File Read Write
INDEXLIB_INSTANTIATE_PARAM_NAME(FileSystemFileTestFileWriteRead, FileSystemFileTest);
INSTANTIATE_TEST_CASE_P(InMemStorage, FileSystemFileTestFileWriteRead,
                        testing::Combine(testing::Values(FSST_MEM), testing::Values(FSOT_MEM)));
INSTANTIATE_TEST_CASE_P(LocalStorage, FileSystemFileTestFileWriteRead,
                        testing::Combine(testing::Values(FSST_DISK), testing::Values(FSOT_MEM, FSOT_MMAP, FSOT_CACHE,
                                                                                     FSOT_BUFFERED, FSOT_LOAD_CONFIG)));

INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(FileSystemFileTestFileWriteRead, TestCreateFileWriter);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(FileSystemFileTestFileWriteRead, TestCreateFileReader);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(FileSystemFileTestFileWriteRead, TestCreateFileReaderWithEmpthFile);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(FileSystemFileTestFileWriteRead, TestGetBaseAddressForFileReader);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(FileSystemFileTestFileWriteRead, TestGetBaseAddressForFileReaderWithEmptyFile);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(FileSystemFileTestFileWriteRead, TestReadByteSliceForFileReader);

INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(FileSystemFileTestFileWriteRead, TestReadFileInPackage);

INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(FileSystemFileTestFileWriteRead, TestStorageMetrics);

INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(FileSystemFileTestFileWriteRead, TestRemoveFileInCache);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(FileSystemFileTestFileWriteRead, TestRemoveFileOnDisk);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(FileSystemFileTestFileWriteRead, TestRemoveFileInFlushQueue);

// Test Cache Hit
INDEXLIB_INSTANTIATE_PARAM_NAME(FileSystemFileTestCacheHit, FileSystemFileTest);
INSTANTIATE_TEST_CASE_P(InMemStorage, FileSystemFileTestCacheHit,
                        testing::Combine(testing::Values(FSST_MEM), testing::Values(FSOT_MEM)));

INSTANTIATE_TEST_CASE_P(LocalStorage, FileSystemFileTestCacheHit,
                        testing::Combine(testing::Values(FSST_DISK), testing::Values(FSOT_MEM, FSOT_MMAP, FSOT_CACHE,
                                                                                     FSOT_BUFFERED, FSOT_LOAD_CONFIG)));

INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(FileSystemFileTestCacheHit, TestCacheHitWhenFileOnDisk);

// Test Cache Hit Replace or Exception
INDEXLIB_INSTANTIATE_TEST_CASE_ONE_P(FileSystemFileTestCacheHitReplaceOrException, FileSystemFileTest,
                                     testing::Combine(testing::Values(FSST_DISK),
                                                      testing::Values(FSOT_MEM, FSOT_MMAP, FSOT_CACHE, FSOT_BUFFERED,
                                                                      FSOT_LOAD_CONFIG)));

INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(FileSystemFileTestCacheHitReplaceOrException, TestCacheHitReplace);

// Test Not Support
INDEXLIB_INSTANTIATE_TEST_CASE_ONE_P(FileSystemFileTestNotSupport, FileSystemFileTest,
                                     testing::Combine(testing::Values(FSST_MEM), testing::Values(FSOT_CACHE)));

// INDEXLIB_INSTANTIATE_TEST_CASE_ONE_P(FileSystemFileTestNotSupport, FileSystemFileTest,
//                                      testing::Combine(testing::Values(FSST_MEM),
//                                                       testing::Values(FSOT_MMAP, FSOT_CACHE, FSOT_BUFFERED,
//                                                                       FSOT_LOAD_CONFIG, FSOT_SLICE)));

INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(FileSystemFileTestNotSupport, TestCreateFileReaderNotSupport);

//////////////////////////////////////////////////////////////////////

#define STORAGE_TYPE GET_PARAM_VALUE(0)
#define OPEN_TYPE GET_PARAM_VALUE(1)

#define COMPILE 0

FileSystemFileTest::FileSystemFileTest() {}
FileSystemFileTest::~FileSystemFileTest() {}

void FileSystemFileTest::CaseSetUp()
{
    _fileSystem = FileSystemCreator::Create("", GET_TEMP_DATA_PATH()).GetOrThrow();
    _directory = Directory::Get(_fileSystem);
}

void FileSystemFileTest::CaseTearDown() {}

void FileSystemFileTest::TestCreateFileWriter()
{
    std::shared_ptr<IFileSystem> fileSystem = _fileSystem;

    string dirPath = "writer_dir_path";
    string filePath = PathUtil::JoinPath(dirPath, "writer_file");
    WriterOption writerOption;
    writerOption.atomicDump = true;
    std::shared_ptr<FileWriter> fileWriter =
        TestFileCreator::CreateWriter(fileSystem, STORAGE_TYPE, OPEN_TYPE, "", filePath, writerOption);
    ASSERT_EQ((size_t)0, fileWriter->GetLength());
    ASSERT_EQ(filePath, fileWriter->GetLogicalPath());

    string fileContext = "hello";
    ASSERT_EQ(FSEC_OK, fileWriter->Write(fileContext.c_str(), fileContext.size()).Code());
    ASSERT_EQ(fileContext.size(), fileWriter->GetLength());

    // ASSERT_FALSE(fileSystem->IsExist(filePath));
    ASSERT_EQ(FSEC_OK, fileWriter->Close());
    ASSERT_TRUE(fileSystem->IsExist(filePath).GetOrThrow());

    if (STORAGE_TYPE == FSST_MEM) {
        ASSERT_TRUE(FileSystemTestUtil::CheckFileStat(fileSystem, filePath, STORAGE_TYPE, FSFT_MEM, FSOT_MEM, 5, true,
                                                      false, false));
    } else {
        ASSERT_TRUE(FileSystemTestUtil::CheckFileStat(fileSystem, filePath, STORAGE_TYPE, FSFT_UNKNOWN, FSOT_UNKNOWN, 5,
                                                      false, true, false));
    }
}

void FileSystemFileTest::TestCreateFileReader()
{
    std::shared_ptr<IFileSystem> fileSystem = CreateFileSystem();

    string fileContext = "hello";
    ASSERT_NE(OPEN_TYPE, FSOT_SLICE);
    std::shared_ptr<FileReader> fileReader =
        TestFileCreator::CreateReader(fileSystem, STORAGE_TYPE, OPEN_TYPE, fileContext);
    ASSERT_EQ(fileContext.size(), fileReader->GetLength());

    // read without offset
    uint8_t buffer[1024];
    size_t readSize = fileReader->Read(buffer, fileContext.size()).GetOrThrow();
    ASSERT_EQ(fileContext.size(), readSize);
    ASSERT_TRUE(memcmp(fileContext.data(), buffer, readSize) == 0);

    // read with offset
    ASSERT_EQ((size_t)3, fileReader->Read(buffer, 3, 2).GetOrThrow());
    ASSERT_TRUE(memcmp("llo", buffer, 3) == 0);

    // read with slicelist
    if (OPEN_TYPE != FSOT_BUFFERED) {
        ByteSliceList* byteSliceList = fileReader->ReadToByteSliceList((size_t)3, 1, ReadOption());
        ASSERT_TRUE(byteSliceList != NULL);
        ByteSliceReader sliceListReader(byteSliceList);
        void* data;
        ASSERT_EQ(3u, sliceListReader.ReadMayCopy(data, 3));
        ASSERT_EQ(0, memcmp(data, "ell", 3));
        delete byteSliceList;
    }

    ASSERT_EQ(FSEC_OK, fileReader->Close());
}

void FileSystemFileTest::TestReadFileInPackage()
{
#if COMPILE
    std::shared_ptr<IFileSystem> fileSystem = CreateFileSystem();
    ASSERT_NE(OPEN_TYPE, FSOT_SLICE);

    vector<string> innerFileContents;
    innerFileContents.push_back("hello");
    innerFileContents.push_back("world");
    innerFileContents.push_back("123456");
    innerFileContents.push_back("");

    vector<std::shared_ptr<FileReader>> fileReaders =
        TestFileCreator::CreateInPackageFileReader(fileSystem, STORAGE_TYPE, OPEN_TYPE, innerFileContents);
    for (size_t i = 0; i < innerFileContents.size(); i++) {
        CheckInnerFile(innerFileContents[i], fileReaders[i], OPEN_TYPE);
        fileReaders[i].reset();
    }

    // check reader after cleanCache and reload from disk
    fileSystem->Sync(true).GetOrThrow();
    fileSystem->CleanCache();
    StorageMetrics storageMetrics = fileSystem->GetFileSystemMetrics().TEST_GetStorageMetrics(STORAGE_TYPE);
    ASSERT_EQ((int64_t)0, storageMetrics.GetTotalFileLength());

    fileReaders = TestFileCreator::CreateInPackageFileReader(fileSystem, STORAGE_TYPE, OPEN_TYPE, innerFileContents);
    for (size_t i = 0; i < innerFileContents.size(); i++) {
        CheckInnerFile(innerFileContents[i], fileReaders[i], OPEN_TYPE);
        fileReaders[i].reset();
    }
#endif
}

void FileSystemFileTest::CheckInnerFile(const string& fileContent, const std::shared_ptr<FileReader>& fileReader,
                                        FSOpenType openType)
{
    ASSERT_EQ(openType, fileReader->GetOpenType());
    ASSERT_EQ(fileContent.size(), fileReader->GetLength());
    ASSERT_TRUE(fileReader->GetFileNode()->IsInPackage());

    // read without offset
    uint8_t buffer[1024];
    size_t readSize = fileReader->Read(buffer, fileContent.size()).GetOrThrow();
    ASSERT_EQ(fileContent.size(), readSize);
    ASSERT_EQ(fileContent, string((char*)buffer, readSize));

    // read with offset
    if (fileContent.size() > 3) {
        size_t remainLen = fileContent.size() - 3;
        string expectValue = fileContent.substr(3);
        ASSERT_EQ(remainLen, fileReader->Read(buffer, remainLen, 3).GetOrThrow());
        ASSERT_EQ(expectValue, string((char*)buffer, remainLen));

        if (openType != FSOT_BUFFERED) {
            // read with slicelist
            ByteSliceList* byteSliceList = fileReader->ReadToByteSliceList((size_t)remainLen, 3, ReadOption());
            ByteSliceReader reader(byteSliceList);
            vector<char> data(' ', remainLen);
            ASSERT_EQ(remainLen, reader.Read(data.data(), remainLen));
            ASSERT_EQ(expectValue, string(data.data(), remainLen));
            delete byteSliceList;
        }
    }
    ASSERT_EQ(FSEC_OK, fileReader->Close());
}

void FileSystemFileTest::TestCreateFileReaderWithEmpthFile()
{
    std::shared_ptr<IFileSystem> fileSystem = CreateFileSystem();

    ASSERT_NE(OPEN_TYPE, FSOT_SLICE);
    std::shared_ptr<FileReader> fileReader = TestFileCreator::CreateReader(fileSystem, STORAGE_TYPE, OPEN_TYPE, "");
    ASSERT_EQ((size_t)0, fileReader->GetLength());

    // read without offset
    uint8_t buffer[1024];
    ASSERT_EQ((size_t)0, fileReader->Read(buffer, 0).GetOrThrow());

    // read with offset
    ASSERT_EQ((size_t)0, fileReader->Read(buffer, 0, 0).GetOrThrow());

    // read with slicelist
    if (OPEN_TYPE != FSOT_BUFFERED) {
        ByteSliceListPtr byteSliceList(fileReader->ReadToByteSliceList((size_t)0, 0, ReadOption()));
        ASSERT_TRUE(byteSliceList != NULL);
        ASSERT_EQ(0u, byteSliceList->GetTotalSize());
    }

    ASSERT_EQ(FSEC_OK, fileReader->Close());
}

void FileSystemFileTest::TestReadByteSliceForFileReader()
{
    std::shared_ptr<IFileSystem> fileSystem = CreateFileSystem();

    string fileContext = "hello";
    std::shared_ptr<FileReader> fileReader =
        TestFileCreator::CreateReader(fileSystem, STORAGE_TYPE, OPEN_TYPE, fileContext);
    if (OPEN_TYPE != FSOT_BUFFERED) {
        uint8_t buffer[1024];
        size_t readSize = fileReader->Read(buffer, fileContext.size(), 0).GetOrThrow();
        ASSERT_EQ(fileContext.size(), readSize);
        ASSERT_EQ(0, memcmp(fileContext.data(), buffer, readSize));

        ByteSliceList* byteSliceList = fileReader->ReadToByteSliceList(fileContext.size(), 0, ReadOption());
        ASSERT_TRUE(byteSliceList);
        ByteSliceReader reader(byteSliceList);
        ASSERT_EQ(fileContext.size(), reader.Read(buffer, fileContext.size()));
        ASSERT_EQ(0, memcmp(fileContext.data(), buffer, fileContext.size()));
        delete byteSliceList;
    } else {
        ASSERT_EQ(nullptr, fileReader->ReadToByteSliceList(fileContext.size(), 0, ReadOption()));
    }
}

void FileSystemFileTest::TestGetBaseAddressForFileReader()
{
    std::shared_ptr<IFileSystem> fileSystem = CreateFileSystem();

    string fileContext = "hello";
    std::shared_ptr<FileReader> fileReader =
        TestFileCreator::CreateReader(fileSystem, STORAGE_TYPE, OPEN_TYPE, fileContext);
    if (OPEN_TYPE == FSOT_MEM || OPEN_TYPE == FSOT_MMAP) {
        char* data = (char*)fileReader->GetBaseAddress();
        ASSERT_TRUE(data);
        ASSERT_EQ(0, memcmp(fileContext.data(), data, fileContext.size()));
    } else {
        ASSERT_FALSE(fileReader->GetBaseAddress());
    }
}

void FileSystemFileTest::TestGetBaseAddressForFileReaderWithEmptyFile()
{
    std::shared_ptr<IFileSystem> fileSystem = CreateFileSystem();
    std::shared_ptr<FileReader> fileReader = TestFileCreator::CreateReader(fileSystem, STORAGE_TYPE, OPEN_TYPE, "");
    ASSERT_FALSE(fileReader->GetBaseAddress());
}

void FileSystemFileTest::TestStorageMetrics()
{
#if COMPILE
    std::shared_ptr<IFileSystem> fileSystem = CreateFileSystem();

    // may be we need CreateReader with FSFileType in test file creator
    FSFileType fileType;
    switch (OPEN_TYPE) {
    case FSOT_MEM:
        fileType = FSFT_MEM;
        break;
    case FSOT_MMAP:
        fileType = FSFT_MMAP;
        break;
    case FSOT_CACHE:
        fileType = FSFT_BLOCK;
        break;
    case FSOT_BUFFERED:
        fileType = FSFT_BUFFERED;
        break;
    case FSOT_LOAD_CONFIG:
        fileType = FSFT_BLOCK;
        break;
    default:
        fileType = FSFT_UNKNOWN;
        break;
    }

    CheckStorageMetrics(fileSystem, STORAGE_TYPE, fileType, 0, 0, __LINE__);
    std::shared_ptr<FileReader> fileReader =
        TestFileCreator::CreateReader(fileSystem, STORAGE_TYPE, OPEN_TYPE, "hello");
    ASSERT_EQ(OPEN_TYPE, fileReader->GetOpenType());
    CheckStorageMetrics(fileSystem, STORAGE_TYPE, fileType, 1, 5, __LINE__);
    fileSystem->Sync(true).GetOrThrow();
    fileReader.reset();
    fileSystem->CleanCache();
    CheckStorageMetrics(fileSystem, STORAGE_TYPE, fileType, 0, 0, __LINE__);
#endif
}

void FileSystemFileTest::TestRemoveFileInCache()
{
    std::shared_ptr<IFileSystem> fileSystem = _fileSystem;

    ASSERT_NE(OPEN_TYPE, FSOT_SLICE);
    WriterOption writerOption;
    writerOption.atomicDump = true;
    std::shared_ptr<FileWriter> fileWriter =
        TestFileCreator::CreateWriter(fileSystem, STORAGE_TYPE, OPEN_TYPE, "", "", writerOption);
    string filePath = fileWriter->GetLogicalPath();

    // ASSERT_FALSE(fileSystem->IsExist(filePath));
    ASSERT_EQ(FSEC_OK, fileWriter->Close());
    fileWriter.reset();

    ASSERT_TRUE(fileSystem->IsExist(filePath).GetOrThrow());
    ASSERT_EQ(FSEC_OK, fileSystem->RemoveFile(filePath, RemoveOption()));
    ASSERT_FALSE(fileSystem->IsExist(filePath).GetOrThrow());
}

void FileSystemFileTest::TestRemoveFileOnDisk()
{
    std::shared_ptr<IFileSystem> fileSystem = CreateFileSystem();
    ASSERT_NE(OPEN_TYPE, FSOT_SLICE);
    std::shared_ptr<FileReader> fileReader =
        TestFileCreator::CreateReader(fileSystem, STORAGE_TYPE, OPEN_TYPE, "hello");
    string filePath = fileReader->GetLogicalPath();
    fileReader.reset();
    fileSystem->Sync(true).GetOrThrow();
    fileSystem->CleanCache();

    // FileStat fileStat;
    // fileSystem->TEST_GetFileStat(filePath, fileStat);
    // ASSERT_TRUE(fileStat.onDisk);
    // ASSERT_FALSE(fileStat.inCache);
    ASSERT_TRUE(fileSystem->IsExist(filePath).GetOrThrow());
    ASSERT_EQ(FSEC_OK, fileSystem->RemoveFile(filePath, RemoveOption()));
    ASSERT_FALSE(fileSystem->IsExist(filePath).GetOrThrow());
}

void FileSystemFileTest::TestRemoveFileInFlushQueue()
{
    std::shared_ptr<IFileSystem> fileSystem = CreateFileSystem();
    ASSERT_NE(OPEN_TYPE, FSOT_SLICE);
    // normal file
    std::shared_ptr<FileReader> fileReader =
        TestFileCreator::CreateReader(fileSystem, STORAGE_TYPE, OPEN_TYPE, "hello");
    string filePath = fileReader->GetLogicalPath();
    fileReader.reset();

    // FileStat fileStat;
    // fileSystem->TEST_GetFileStat(filePath, fileStat);
    // ASSERT_EQ(STORAGE_TYPE != FSST_MEM, fileStat.onDisk);
    // ASSERT_TRUE(fileStat.inCache);
    ASSERT_TRUE(fileSystem->IsExist(filePath).GetOrThrow());

    // in mem file
    string inMemPath = "inmem";
    std::shared_ptr<FileWriter> inMemFile = fileSystem->CreateFileWriter(inMemPath, WriterOption::Mem(10)).GetOrThrow();

    // fileSystem->TEST_GetFileStat(inMemPath, fileStat);
    // ASSERT_FALSE(fileStat.onDisk);
    // ASSERT_TRUE(fileStat.inCache);
    ASSERT_TRUE(fileSystem->IsExist(inMemPath).GetOrThrow());

    fileSystem->CleanCache();

    ASSERT_EQ(FSEC_OK, inMemFile->Close());
    inMemFile.reset();
    ASSERT_EQ(FSEC_OK, fileSystem->RemoveFile(inMemPath, RemoveOption()));
    ASSERT_TRUE(fileSystem->IsExist(filePath).GetOrThrow());
    ASSERT_FALSE(fileSystem->IsExist(inMemPath).GetOrThrow());

    // remove does not trigger sync
    // fileSystem->TEST_GetFileStat(filePath, fileStat);
    // ASSERT_EQ(STORAGE_TYPE != FSST_MEM, fileStat.onDisk);

    fileSystem->Sync(true).GetOrThrow();
    // fileSystem->TEST_GetFileStat(filePath, fileStat);
    // ASSERT_TRUE(fileStat.onDisk);
}

void FileSystemFileTest::TestCacheHitWhenFileOnDisk()
{
    std::shared_ptr<IFileSystem> fileSystem = CreateFileSystem();

    std::shared_ptr<FileWriter> writer = TestFileCreator::CreateWriter(fileSystem, STORAGE_TYPE, OPEN_TYPE, "hello");
    ASSERT_EQ(FSEC_OK, writer->Close());
    // flush file to disk
    fileSystem->Sync(true).GetOrThrow();
    fileSystem->CleanCache();

    std::shared_ptr<FileReader> fileReader1 = TestFileCreator::CreateReader(fileSystem, STORAGE_TYPE, OPEN_TYPE, "");
    std::shared_ptr<FileReader> fileReader2 = TestFileCreator::CreateReader(fileSystem, STORAGE_TYPE, OPEN_TYPE, "");
    ASSERT_EQ(OPEN_TYPE, fileReader1->GetOpenType());
    ASSERT_EQ(fileReader1->GetFileNode(), fileReader2->GetFileNode());

    CheckFileCacheMetrics(fileSystem, STORAGE_TYPE, 1, 1, 0, __LINE__);
}

void FileSystemFileTest::TestCacheHitWhenFileInCacheForInMemStorage()
{
    std::shared_ptr<IFileSystem> fileSystem = CreateFileSystem();
    std::shared_ptr<FileReader> fileReader1 = TestFileCreator::CreateReader(fileSystem, FSST_MEM, FSOT_MEM, "hello");
    std::shared_ptr<FileReader> fileReader2 = TestFileCreator::CreateReader(fileSystem, FSST_MEM, FSOT_MEM, "");
    ASSERT_EQ(FSOT_MEM, fileReader1->GetOpenType());
    ASSERT_EQ(fileReader1->GetFileNode(), fileReader2->GetFileNode());
    CheckFileCacheMetrics(fileSystem, FSST_MEM, 0, 2, 0, __LINE__);
}

void FileSystemFileTest::TestForDisableCache()
{
    FileSystemOptions options;
    options.enableAsyncFlush = false;
    options.useCache = false;
    _fileSystem = FileSystemCreator::Create("ut", GET_TEMP_DATA_PATH(), options).GetOrThrow();
    std::shared_ptr<IFileSystem> fileSystem = _fileSystem;

    std::shared_ptr<FileReader> fileReader1 = TestFileCreator::CreateReader(fileSystem, FSST_DISK, FSOT_MEM, "hello");
    // FileStat fileStat;
    // fileSystem->TEST_GetFileStat(fileReader1->GetLogicalPath(), fileStat);
    // ASSERT_FALSE(fileStat.inCache);

    std::shared_ptr<FileReader> fileReader2 = TestFileCreator::CreateReader(fileSystem, FSST_DISK, FSOT_MEM, "");
    // fileSystem->TEST_GetFileStat(fileReader2->GetLogicalPath(), fileStat);
    // ASSERT_FALSE(fileStat.inCache);

    ASSERT_EQ(FSOT_MEM, fileReader1->GetOpenType());
    ASSERT_NE(fileReader1->GetFileNode(), fileReader2->GetFileNode());
}

void FileSystemFileTest::TestCacheHitReplace()
{
    std::shared_ptr<IFileSystem> fileSystem = CreateFileSystem();

    for (int type = 0; type < FSOT_SLICE; ++type) {
        if (type == FSOT_BUFFERED) {
            continue;
        }
        ResetFileSystem(fileSystem);
        std::shared_ptr<FileReader> fileReader =
            TestFileCreator::CreateReader(fileSystem, STORAGE_TYPE, OPEN_TYPE, "hello");
        ASSERT_EQ(OPEN_TYPE, fileReader->GetOpenType());
        FSOpenType newOpenType = (FSOpenType)type;
        fileReader.reset();
        fileReader = TestFileCreator::CreateReader(fileSystem, STORAGE_TYPE, newOpenType, "");
        ASSERT_EQ(newOpenType, fileReader->GetOpenType());
        if (newOpenType == OPEN_TYPE) {
            continue;
        }
        CheckFileCacheMetrics(fileSystem, STORAGE_TYPE, 1, 0, 1, __LINE__);
    }
}

void FileSystemFileTest::TestCreateFileWriterForSync()
{
    std::shared_ptr<IFileSystem> fileSystem = CreateFileSystem();

    string filePath = "inmem/file";
    TestFileCreator::CreateFiles(fileSystem, FSST_MEM, FSOT_MEM, "hello", filePath);
    ASSERT_TRUE(fileSystem->IsExist(filePath).GetOrThrow());

    std::shared_ptr<FileWriter> fileWriter = fileSystem->CreateFileWriter("disk", WriterOption()).GetOrThrow();
    ASSERT_EQ(FSEC_OK, fileWriter->Close());
    ASSERT_TRUE(fileSystem->IsExist(filePath).GetOrThrow());
}

void FileSystemFileTest::TestCreateFileReaderNotSupport()
{
    std::shared_ptr<IFileSystem> fileSystem = _fileSystem;

    ASSERT_THROW(TestFileCreator::CreateReader(fileSystem, STORAGE_TYPE, OPEN_TYPE, "hello"), UnSupportedException);
}

void FileSystemFileTest::CheckStorageMetrics(const std::shared_ptr<IFileSystem>& ifs, FSStorageType storageType,
                                             FSFileType fileType, int64_t fileCount, int64_t fileLength, int lineNo)
{
#if COMPILE
    SCOPED_TRACE(string("see line:") + StringUtil::toString(lineNo) + "]");

    StorageMetrics metrics = ifs->GetFileSystemMetrics().TEST_GetStorageMetrics(storageType);
    ASSERT_EQ(fileCount, metrics.GetFileCount(fileType));
    ASSERT_EQ(fileLength, metrics.GetFileLength(fileType));
#endif
}

void FileSystemFileTest::CheckFileCacheMetrics(const std::shared_ptr<IFileSystem>& ifs, FSStorageType storageType,
                                               int64_t missCount, int64_t hitCount, int64_t replaceCount, int lineNo)
{
#if COMPILE
    SCOPED_TRACE(string("see line:") + StringUtil::toString(lineNo) + "]");

    StorageMetrics metrics = ifs->GetFileSystemMetrics().TEST_GetStorageMetrics(storageType);
    ASSERT_EQ(missCount, metrics.GetFileCacheMetrics().missCount.getValue());
    ASSERT_EQ(hitCount, metrics.GetFileCacheMetrics().hitCount.getValue());
    ASSERT_EQ(replaceCount, metrics.GetFileCacheMetrics().replaceCount.getValue());
#endif
}

void FileSystemFileTest::MakeStorageRoot(std::shared_ptr<IFileSystem> ifs, FSStorageType storageType,
                                         const string& rootPath)
{
    if (storageType == FSST_MEM) {
        // ifs->MountInMemStorage(rootPath);
    } else {
        ASSERT_EQ(FSEC_OK, ifs->MakeDirectory(rootPath, DirectoryOption()));
    }
}

std::shared_ptr<IFileSystem> FileSystemFileTest::CreateFileSystem()
{
    // cache has not default creator when create file reader
    FileSystemOptions fsOptions;
    fsOptions.loadConfigList = LoadConfigListCreator::CreateLoadConfigList(READ_MODE_CACHE);
    fsOptions.needFlush = true;
    return FileSystemCreator::Create("FileSystemFileTest", GET_TEMP_DATA_PATH(), fsOptions).GetOrThrow();
}

void FileSystemFileTest::ResetFileSystem(std::shared_ptr<IFileSystem>& fileSystem)
{
    if (fileSystem) {
        fileSystem->Sync(true).GetOrThrow();
    }
    fileSystem.reset();
    testDataPathTearDown();
    testDataPathSetup();
    fileSystem = CreateFileSystem();
}
}} // namespace indexlib::file_system
