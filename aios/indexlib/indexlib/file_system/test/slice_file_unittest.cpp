#include <autil/StringUtil.h>
#include "indexlib/file_system/test/slice_file_unittest.h"
#include "indexlib/file_system/test/test_file_creator.h"
#include "indexlib/file_system/test/file_system_test_util.h"
#include "indexlib/util/path_util.h"

using namespace std;
using namespace autil;
IE_NAMESPACE_USE(util);
IE_NAMESPACE_USE(misc);

IE_NAMESPACE_BEGIN(file_system);
IE_LOG_SETUP(file_system, SliceFileTest);

SliceFileTest::SliceFileTest()
{
}

SliceFileTest::~SliceFileTest()
{
}

void SliceFileTest::CaseSetUp()
{
}

void SliceFileTest::CaseTearDown()
{
}

void SliceFileTest::TestExistInCacheBeforeClose()
{
    FSStorageType st = GET_CASE_PARAM();
    IE_LOG(INFO, "st: %d", st);
    IndexlibFileSystemPtr fileSystem = GET_FILE_SYSTEM();
    FileWriterPtr fileWriter = TestFileCreator::CreateWriter(
            fileSystem, st, FSOT_SLICE);
    FileStat fileStat;
    fileSystem->GetFileStat(fileWriter->GetPath(), fileStat);
    INDEXLIB_TEST_TRUE(fileStat.inCache);
    fileWriter->Close();
}

#define CHECK_SLICEFILE_METRICS(metrics, fileCnt, fileLen, totalLen)    \
    CheckSliceFileMetrics((metrics), (fileCnt), (fileLen), (totalLen), __LINE__)
void SliceFileTest::TestMetricsIncrease()
{
    FSStorageType storageType = GET_CASE_PARAM();
    IndexlibFileSystemPtr fileSystem = GET_FILE_SYSTEM();
    StorageMetrics metrics = fileSystem->GetStorageMetrics(storageType);
    FileWriterPtr fileWriter = TestFileCreator::CreateWriter(
            fileSystem, storageType, FSOT_SLICE, "abc");
    metrics = fileSystem->GetStorageMetrics(storageType);
    CHECK_SLICEFILE_METRICS(metrics, 1, 3, 3);
    fileWriter->Close();
}

void SliceFileTest::TestMetricsDecrease()
{
    FSStorageType storageType = GET_CASE_PARAM();
    IndexlibFileSystemPtr fileSystem = GET_FILE_SYSTEM();
    FileWriterPtr fileWriter = TestFileCreator::CreateWriter(
            fileSystem, storageType, FSOT_SLICE, "abc");

    StorageMetrics metrics = fileSystem->GetStorageMetrics(storageType);
    CHECK_SLICEFILE_METRICS(metrics, 1, 3, 3);

    string filePath = fileWriter->GetPath();
    fileWriter->Close();
    fileWriter.reset();
    fileSystem->RemoveFile(filePath);
    metrics = fileSystem->GetStorageMetrics(storageType);
    CHECK_SLICEFILE_METRICS(metrics, 0, 0, 0);
}

void SliceFileTest::TestCreateSliceFileWriter()
{
    IndexlibFileSystemPtr fileSystem = GET_FILE_SYSTEM();
    FSStorageType storageType = GET_CASE_PARAM();

    string dirPath = PathUtil::JoinPath(fileSystem->GetRootPath(), 
            "writer_dir_path");
    string filePath = PathUtil::JoinPath(dirPath, "writer_file");
    FileWriterPtr fileWriter = TestFileCreator::CreateWriter(
            fileSystem, storageType, FSOT_SLICE, "", filePath);
    ASSERT_EQ((size_t)0, fileWriter->GetLength());
    ASSERT_EQ(filePath, fileWriter->GetPath());

    string fileContext = "hello";
    fileWriter->Write(fileContext.c_str(), fileContext.size());
    ASSERT_EQ(fileContext.size(), fileWriter->GetLength());
    
    ASSERT_TRUE(fileSystem->IsExist(filePath));
    fileWriter->Close();

    ASSERT_TRUE(FileSystemTestUtil::CheckFileStat(fileSystem,
                    filePath, storageType, FSFT_SLICE, FSOT_SLICE,
                    5, true, false, false));
}

void SliceFileTest::TestCreateSliceFileReader()
{
    IndexlibFileSystemPtr fileSystem = GET_FILE_SYSTEM();
    FSStorageType storageType = GET_CASE_PARAM();

    string fileContext = "hello";
    SliceFileReaderPtr sliceFileReader = 
        TestFileCreator::CreateSliceFileReader(
                fileSystem, storageType, fileContext);
    ASSERT_EQ(fileContext.size(), sliceFileReader->GetLength());
    sliceFileReader->Close();

    // array
    SliceFilePtr sliceFile = fileSystem->CreateSliceFile(
            GET_ABS_PATH("slicefile"), 8, 2);
    sliceFileReader = sliceFile->CreateSliceFileReader();
    const BytesAlignedSliceArrayPtr& array = 
        sliceFileReader->GetBytesAlignedSliceArray();
    ASSERT_FALSE(sliceFileReader->GetBaseAddress());
    ASSERT_EQ((size_t)0, sliceFileReader->GetLength());
    ASSERT_EQ((int64_t)0, array->Insert("abc", 3));
    ASSERT_EQ((size_t)3, sliceFileReader->GetLength());

    // can not be open with other type
    sliceFileReader.reset();
    ASSERT_THROW(TestFileCreator::CreateReader(fileSystem, storageType, 
                    FSOT_IN_MEM, ""), InconsistentStateException);

}

void SliceFileTest::TestCacheHit()
{
    IndexlibFileSystemPtr fileSystem = GET_FILE_SYSTEM();

    FSStorageType storageType = GET_CASE_PARAM();

    FileReaderPtr fileReader1 = TestFileCreator::CreateSliceFileReader(
            fileSystem, storageType, "hello");
    FileReaderPtr fileReader2 = TestFileCreator::CreateSliceFileReader(
            fileSystem, storageType, "");        

    ASSERT_EQ(FSOT_SLICE, fileReader1->GetOpenType());
    ASSERT_EQ(fileReader1->GetFileNode(), fileReader2->GetFileNode());
    ASSERT_EQ(fileReader1->GetBaseAddress(), fileReader2->GetBaseAddress());
}

void SliceFileTest::TestDisableCache()
{
    RESET_FILE_SYSTEM(LoadConfigList(), false, false);
    IndexlibFileSystemPtr fileSystem = GET_FILE_SYSTEM();
    SliceFilePtr sliceFile = fileSystem->CreateSliceFile(
            GET_ABS_PATH("test_file"), 10, 1);
    FileReaderPtr fileReader = sliceFile->CreateSliceFileReader();
    ASSERT_FALSE(fileSystem->IsExist(fileReader->GetPath()));
}

void SliceFileTest::TestRemoveSliceFile()
{
    IndexlibFileSystemPtr fileSystem = GET_FILE_SYSTEM();
    FSStorageType storageType = GET_CASE_PARAM();

    FileReaderPtr fileReader = TestFileCreator::CreateSliceFileReader(
            fileSystem, storageType, "hello");
    string filePath = fileReader->GetPath();
    fileReader.reset();
    ASSERT_TRUE(fileSystem->IsExist(filePath));
    fileSystem->RemoveFile(filePath);
    ASSERT_FALSE(fileSystem->IsExist(filePath));
}

void SliceFileTest::CheckSliceFileMetrics(const StorageMetrics& metrics,
        int64_t fileCount, int64_t fileLength, int64_t totalFileLength, 
        int lineNo)
{
    SCOPED_TRACE(string("line number[") 
                 + StringUtil::toString(lineNo) + string("]"));

    ASSERT_EQ(fileCount, metrics.GetFileCount(FSFT_SLICE));
    ASSERT_EQ(fileLength, metrics.GetFileLength(FSFT_SLICE));
    ASSERT_EQ(totalFileLength, metrics.GetTotalFileLength());
}

IE_NAMESPACE_END(file_system);

