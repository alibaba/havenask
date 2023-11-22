#include "gtest/gtest.h"
#include <cstddef>
#include <memory>
#include <stdint.h>
#include <string>

#include "alog/Logger.h"
#include "autil/CommonMacros.h"
#include "autil/Log.h"
#include "autil/StringUtil.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/FileSystemCreator.h"
#include "indexlib/file_system/FileSystemDefine.h"
#include "indexlib/file_system/FileSystemMetrics.h"
#include "indexlib/file_system/IFileSystem.h"
#include "indexlib/file_system/LogicalFileSystem.h"
#include "indexlib/file_system/RemoveOption.h"
#include "indexlib/file_system/Storage.h"
#include "indexlib/file_system/StorageMetrics.h"
#include "indexlib/file_system/file/FileReader.h"
#include "indexlib/file_system/file/FileWriter.h"
#include "indexlib/file_system/file/SliceFileReader.h"
#include "indexlib/file_system/load_config/LoadConfigList.h"
#include "indexlib/file_system/test/FileSystemTestUtil.h"
#include "indexlib/file_system/test/TestFileCreator.h"
#include "indexlib/util/PathUtil.h"
#include "indexlib/util/byte_slice_list/ByteSliceList.h"
#include "indexlib/util/slice_array/BytesAlignedSliceArray.h"
#include "indexlib/util/testutil/unittest.h"

namespace indexlib { namespace util {
class InconsistentStateException;
}} // namespace indexlib::util
using namespace std;
using namespace autil;
using namespace indexlib::util;

namespace indexlib { namespace file_system {

class SliceFileTest : public INDEXLIB_TESTBASE_WITH_PARAM<FSStorageType>
{
public:
    SliceFileTest();
    ~SliceFileTest();

    DECLARE_CLASS_NAME(SliceFileTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;

    void TestExistInCacheBeforeClose();
    void TestMetricsIncrease();
    void TestMetricsDecrease();
    void TestCreateSliceFileWriter();
    void TestCreateSliceFileReader();
    void TestCacheHit();
    void TestRemoveSliceFile();
    void TestDisableCache();

private:
    void CheckSliceFileMetrics(const StorageMetrics& metrics, int64_t fileCount, int64_t fileLength,
                               int64_t totalFileLength, int lineNo);

private:
    std::string _rootDir;
    std::shared_ptr<IFileSystem> _fileSystem;
    std::shared_ptr<Directory> _directory;

private:
    AUTIL_LOG_DECLARE();
};
AUTIL_LOG_SETUP(indexlib.file_system, SliceFileTest);

INSTANTIATE_TEST_CASE_P(p1, SliceFileTest, testing::Values(FSST_DISK, FSST_MEM));
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(SliceFileTest, TestExistInCacheBeforeClose);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(SliceFileTest, TestMetricsIncrease);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(SliceFileTest, TestMetricsDecrease);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(SliceFileTest, TestCreateSliceFileWriter);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(SliceFileTest, TestCreateSliceFileReader);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(SliceFileTest, TestCacheHit);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(SliceFileTest, TestRemoveSliceFile);
INDEXLIB_UNIT_TEST_CASE(SliceFileTest, TestDisableCache);
//////////////////////////////////////////////////////////////////////

SliceFileTest::SliceFileTest() {}

SliceFileTest::~SliceFileTest() {}

void SliceFileTest::CaseSetUp()
{
    _fileSystem = FileSystemCreator::Create("", GET_TEMP_DATA_PATH()).GetOrThrow();
    _directory = Directory::Get(_fileSystem);
}

void SliceFileTest::CaseTearDown() {}

void SliceFileTest::TestExistInCacheBeforeClose()
{
    FSStorageType st = GET_CASE_PARAM();
    AUTIL_LOG(INFO, "st: %d", st);
    auto fileSystem = std::dynamic_pointer_cast<LogicalFileSystem>(_fileSystem);
    std::shared_ptr<FileWriter> fileWriter = TestFileCreator::CreateWriter(fileSystem, st, FSOT_SLICE, "abc", "f");
    ASSERT_TRUE(fileSystem->_outputStorage->IsExistInCache(fileWriter->GetLogicalPath()));
    ASSERT_EQ(FSEC_OK, fileWriter->Close());
}

#define CHECK_SLICEFILE_METRICS(metrics, fileCnt, fileLen, totalLen)                                                   \
    CheckSliceFileMetrics((metrics), (fileCnt), (fileLen), (totalLen), __LINE__)
void SliceFileTest::TestMetricsIncrease()
{
#if 0 // TODO(qingran)
    FSStorageType storageType = GET_CASE_PARAM();
    std::shared_ptr<IFileSystem> fileSystem = _fileSystem;
    StorageMetrics metrics = fileSystem->GetFileSystemMetrics().TEST_GetStorageMetrics(storageType);
    std::shared_ptr<FileWriter> fileWriter = TestFileCreator::CreateWriter(fileSystem, storageType, FSOT_SLICE, "abc");
    metrics = fileSystem->GetFileSystemMetrics().TEST_GetStorageMetrics(storageType);
    CHECK_SLICEFILE_METRICS(metrics, 1, 3, 3);
    fileWriter->Close();
#endif
}

void SliceFileTest::TestMetricsDecrease()
{
    return; // TODO
    FSStorageType storageType = GET_CASE_PARAM();
    std::shared_ptr<IFileSystem> fileSystem = _fileSystem;
    std::shared_ptr<FileWriter> fileWriter = TestFileCreator::CreateWriter(fileSystem, storageType, FSOT_SLICE, "abc");

    StorageMetrics metrics = fileSystem->GetFileSystemMetrics().TEST_GetStorageMetrics(storageType);
    CHECK_SLICEFILE_METRICS(metrics, 1, 3, 3);

    string filePath = fileWriter->GetLogicalPath();
    ASSERT_EQ(FSEC_OK, fileWriter->Close());
    fileWriter.reset();
    ASSERT_EQ(FSEC_OK, fileSystem->RemoveFile(filePath, RemoveOption()));
    metrics = fileSystem->GetFileSystemMetrics().TEST_GetStorageMetrics(storageType);
    CHECK_SLICEFILE_METRICS(metrics, 0, 0, 0);
}

void SliceFileTest::TestCreateSliceFileWriter()
{
    std::shared_ptr<IFileSystem> fileSystem = _fileSystem;
    FSStorageType storageType = GET_CASE_PARAM();

    string dirPath = "writer_dir_path";
    string filePath = PathUtil::JoinPath(dirPath, "writer_file");
    std::shared_ptr<FileWriter> fileWriter =
        TestFileCreator::CreateWriter(fileSystem, storageType, FSOT_SLICE, "", filePath);
    ASSERT_EQ((size_t)0, fileWriter->GetLength());
    ASSERT_EQ(filePath, fileWriter->GetLogicalPath());

    string fileContext = "hello";
    ASSERT_EQ(FSEC_OK, fileWriter->Write(fileContext.c_str(), fileContext.size()).Code());
    ASSERT_EQ(fileContext.size(), fileWriter->GetLength());

    ASSERT_TRUE(fileSystem->IsExist(filePath).GetOrThrow());
    ASSERT_EQ(FSEC_OK, fileWriter->Close());

    ASSERT_TRUE(FileSystemTestUtil::CheckFileStat(fileSystem, filePath, storageType, FSFT_SLICE, FSOT_SLICE, 5, true,
                                                  false, false));
}

void SliceFileTest::TestCreateSliceFileReader()
{
    std::shared_ptr<IFileSystem> fileSystem = _fileSystem;
    FSStorageType storageType = GET_CASE_PARAM();

    string fileContext = "hello";
    std::shared_ptr<SliceFileReader> sliceFileReader =
        TestFileCreator::CreateSliceFileReader(fileSystem, storageType, fileContext);
    ASSERT_EQ(fileContext.size(), sliceFileReader->GetLength());
    ASSERT_EQ(FSEC_OK, sliceFileReader->Close());

    // array
    auto sliceFile = fileSystem->CreateFileWriter("slicefile", WriterOption::Slice(8, 2)).GetOrThrow();
    ASSERT_EQ(FSEC_OK, sliceFile->Close());
    sliceFileReader =
        std::dynamic_pointer_cast<SliceFileReader>(fileSystem->CreateFileReader("slicefile", FSOT_SLICE).GetOrThrow());
    const BytesAlignedSliceArrayPtr& array = sliceFileReader->GetBytesAlignedSliceArray();
    ASSERT_FALSE(sliceFileReader->GetBaseAddress());
    ASSERT_EQ((size_t)0, sliceFileReader->GetLength());
    ASSERT_EQ((int64_t)0, array->Insert("abc", 3));
    ASSERT_EQ((size_t)3, sliceFileReader->GetLength());

    // can not be open with other type
    sliceFileReader.reset();
    ASSERT_THROW(TestFileCreator::CreateReader(fileSystem, storageType, FSOT_MEM, ""), FileIOException);
}

void SliceFileTest::TestCacheHit()
{
    std::shared_ptr<IFileSystem> fileSystem = _fileSystem;

    FSStorageType storageType = GET_CASE_PARAM();

    std::shared_ptr<FileReader> fileReader1 = TestFileCreator::CreateSliceFileReader(fileSystem, storageType, "hello");
    std::shared_ptr<FileReader> fileReader2 = TestFileCreator::CreateSliceFileReader(fileSystem, storageType, "");

    ASSERT_EQ(FSOT_SLICE, fileReader1->GetOpenType());
    ASSERT_EQ(fileReader1->GetFileNode(), fileReader2->GetFileNode());
    ASSERT_EQ(fileReader1->GetBaseAddress(), fileReader2->GetBaseAddress());
}

void SliceFileTest::TestDisableCache()
{
    FileSystemOptions options;
    options.enableAsyncFlush = false;
    options.useCache = false;
    _fileSystem = FileSystemCreator::Create("ut", GET_TEMP_DATA_PATH(), options).GetOrThrow();
    _directory = Directory::Get(_fileSystem);

    std::shared_ptr<IFileSystem> fileSystem = _fileSystem;
    std::shared_ptr<FileWriter> fileWriter =
        fileSystem->CreateFileWriter("test_file", WriterOption::Slice(10, 1)).GetOrThrow();
    std::shared_ptr<FileReader> fileReader = fileSystem->CreateFileReader("test_file", FSOT_SLICE).GetOrThrow();
    ASSERT_EQ(FSEC_OK, fileWriter->Close());
    // ASSERT_FALSE(fileSystem->IsExist(fileReader->GetLogicalPath()));
    ASSERT_TRUE(fileSystem->IsExist(fileReader->GetLogicalPath()).GetOrThrow());
}

void SliceFileTest::TestRemoveSliceFile()
{
    std::shared_ptr<IFileSystem> fileSystem = _fileSystem;
    FSStorageType storageType = GET_CASE_PARAM();

    std::shared_ptr<FileReader> fileReader = TestFileCreator::CreateSliceFileReader(fileSystem, storageType, "hello");
    string filePath = fileReader->GetLogicalPath();
    fileReader.reset();
    ASSERT_TRUE(fileSystem->IsExist(filePath).GetOrThrow());
    ASSERT_EQ(FSEC_OK, fileSystem->RemoveFile(filePath, RemoveOption()));
    ASSERT_FALSE(fileSystem->IsExist(filePath).GetOrThrow());
}

void SliceFileTest::CheckSliceFileMetrics(const StorageMetrics& metrics, int64_t fileCount, int64_t fileLength,
                                          int64_t totalFileLength, int lineNo)
{
    SCOPED_TRACE(string("line number[") + StringUtil::toString(lineNo) + string("]"));

    ASSERT_EQ(fileCount, metrics.GetFileCount(FSMG_LOCAL, FSFT_SLICE));
    ASSERT_EQ(fileLength, metrics.GetFileLength(FSMG_LOCAL, FSFT_SLICE));
    ASSERT_EQ(totalFileLength, metrics.GetTotalFileLength());
}
}} // namespace indexlib::file_system
