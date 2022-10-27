#include "indexlib/file_system/test/compress_file_writer_unittest.h"
#include "indexlib/file_system/compress_file_reader.h"

using namespace std;

IE_NAMESPACE_BEGIN(file_system);
IE_LOG_SETUP(file_system, CompressFileWriterTest);

CompressFileWriterTest::CompressFileWriterTest()
{
}

CompressFileWriterTest::~CompressFileWriterTest()
{
}

void CompressFileWriterTest::CaseSetUp()
{
}

void CompressFileWriterTest::CaseTearDown()
{
}

void CompressFileWriterTest::TestSimpleProcess()
{
    InnerTestSimpleProcess(false);
    InnerTestSimpleProcess(true);
}

void CompressFileWriterTest::InnerTestSimpleProcess(bool isMemDirectory)
{
    DirectoryPtr segDir = 
        isMemDirectory ? GET_SEGMENT_DIRECTORY() : GET_PARTITION_DIRECTORY();

    string filePath = "tmp";
    CompressFileWriterPtr compressWriter =
        segDir->CreateCompressFileWriter(filePath, "lz4", 1024);
    FileWriterPtr fileWriter = compressWriter->GetDataFileWriter();

    size_t dataLen = 73 * 1024; // 73KB
    string oriData = MakeOriginData(dataLen);
    compressWriter->Write(oriData.data(), oriData.size());
    ASSERT_EQ(fileWriter->GetPath(), compressWriter->GetPath());
    ASSERT_EQ(fileWriter->GetLength(), compressWriter->GetLength());
    ASSERT_TRUE(compressWriter->GetLength() < dataLen);
    compressWriter->Close();

    CompressFileReaderPtr compressReader = segDir->CreateCompressFileReader(
            filePath, FSOT_BUFFERED);
    FileReaderPtr fileReader = compressReader->GetDataFileReader();

    ASSERT_EQ(fileReader->GetLength(), compressReader->GetLength());

    char* readBuffer = new char[dataLen + 1];
    size_t decompressLen = compressReader->Read(readBuffer, dataLen);
    ASSERT_EQ(dataLen, decompressLen);

    string decompressData(readBuffer, dataLen);
    ASSERT_TRUE(oriData == decompressData);
    delete [] readBuffer;

    CheckData(oriData, compressReader, 1);
    CheckData(oriData, compressReader, 10);
    CheckData(oriData, compressReader, 128);
    CheckData(oriData, compressReader, 512 + 1);
    CheckData(oriData, compressReader, 1024);
    CheckData(oriData, compressReader, 2 * 1024);
}

void CompressFileWriterTest::TestCompressAddressMapperMemoryStatistic()
{
    {
        DirectoryPtr segDir = GET_PARTITION_DIRECTORY();
        string filePath = "tmp";
        CompressFileWriterPtr compressWriter =
            segDir->CreateCompressFileWriter(filePath, "lz4", 128);
    
        size_t dataLen = 73 * 1024; // 73KB
        string oriData = MakeOriginData(dataLen);
        compressWriter->Write(oriData.data(), oriData.size());
        compressWriter->Close();

        CompressFileReaderPtr compressReader =
            segDir->CreateCompressFileReader(filePath, FSOT_MMAP);

        GET_FILE_SYSTEM()->CleanCache();
        const StorageMetrics& localStorageMetrics =
            GET_FILE_SYSTEM()->GetStorageMetrics(FSST_LOCAL);
        size_t expectLen = compressReader->GetLength();
        ASSERT_EQ(expectLen, localStorageMetrics.GetTotalFileLength());
    }
    
    {
        TearDown();
        SetUp();
        
        DirectoryPtr segDir = GET_PARTITION_DIRECTORY();
        string filePath = "tmp";
        CompressFileWriterPtr compressWriter =
            segDir->CreateCompressFileWriter(filePath, "lz4", 128);
    
        size_t dataLen = 73 * 1024; // 73KB
        string oriData = MakeOriginData(dataLen);
        compressWriter->Write(oriData.data(), oriData.size());
        compressWriter->Close();

        CompressFileReaderPtr compressReader =
            segDir->CreateCompressFileReader(filePath, FSOT_BUFFERED);

        GET_FILE_SYSTEM()->CleanCache();
        const StorageMetrics& localStorageMetrics =
            GET_FILE_SYSTEM()->GetStorageMetrics(FSST_LOCAL);

        const CompressFileInfo& compressInfo = compressReader->GetCompressInfo();
        size_t addressMapperLen = (compressInfo.blockCount + 1) * sizeof(size_t);
        size_t expectLen = compressReader->GetLength() + addressMapperLen;
        ASSERT_EQ(expectLen, localStorageMetrics.GetTotalFileLength());

        CompressFileReaderPtr compressReaderExist =
            segDir->CreateCompressFileReader(filePath, FSOT_BUFFERED);
        GET_FILE_SYSTEM()->CleanCache();
        const StorageMetrics& localStorageMetricsExist =
            GET_FILE_SYSTEM()->GetStorageMetrics(FSST_LOCAL);
        const CompressFileInfo& compressInfoExist = compressReaderExist->GetCompressInfo();
        size_t addressMapperLenExist = (compressInfoExist.blockCount + 1) * sizeof(size_t);
        size_t expectLenExist = compressReaderExist->GetLength() + addressMapperLenExist;
        ASSERT_EQ(expectLenExist, localStorageMetricsExist.GetTotalFileLength());
    }
}

string CompressFileWriterTest::MakeOriginData(size_t dataLen)
{
    string orgData(dataLen, '\0');
    for (size_t i = 0; i < dataLen; ++i)
    {
        orgData[i] = (char)(i * 3 % 127);
    }
    return orgData;
}

void CompressFileWriterTest::CheckData(
        const string &oriDataStr,
        const CompressFileReaderPtr &compressReader,
        size_t readStep)
{
    vector<char> buffer(readStep);
    
    size_t lastOffset = 0;
    string lastExpectStr;
    for (size_t i = 0; i < oriDataStr.size(); i += readStep)
    {
        string expectStr = oriDataStr.substr(i, readStep);
        size_t readLen = compressReader->Read(buffer.data(), readStep, i);
        ASSERT_TRUE(expectStr.compare(0, readLen, buffer.data(), readLen) == 0);
        
        if (i > 0)
        {
            // read backward
            readLen = compressReader->Read(buffer.data(), readStep, lastOffset);
            ASSERT_TRUE(lastExpectStr.compare(0, readLen, buffer.data(), readLen) == 0) << i;
        }
        lastExpectStr = expectStr;
        lastOffset = i;
    }
}

IE_NAMESPACE_END(file_system);

