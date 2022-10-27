#include "indexlib/storage/test/buffered_file_wrapper_unittest.h"

using namespace std;
using namespace fslib;
using namespace fslib::fs;

IE_NAMESPACE_BEGIN(storage);
IE_LOG_SETUP(storage, BufferedFileWrapperTest);

INDEXLIB_UNIT_TEST_CASE(BufferedFileWrapperTest, TestCaseForRead);
INDEXLIB_UNIT_TEST_CASE(BufferedFileWrapperTest, TestCaseForSeek);
INDEXLIB_UNIT_TEST_CASE(BufferedFileWrapperTest, TestCaseForReadBigFile);
INDEXLIB_UNIT_TEST_CASE(BufferedFileWrapperTest, TestCaseForWrite);
INDEXLIB_UNIT_TEST_CASE(BufferedFileWrapperTest, TestCaseForWriteMultiTimes);

void BufferedFileWrapperTest::CaseSetUp()
{
    string dir = GET_TEST_DATA_PATH();
    mFilePath = dir + "test_file";
    FileSystemWrapper::Reset();
}

void BufferedFileWrapperTest::CaseTearDown()
{
    if (FileSystem::isExist(mFilePath) == EC_TRUE)
    {
        FileSystem::remove(mFilePath);
    }
}

void BufferedFileWrapperTest::TestCaseForRead()
{
    TestForRead(20, 100);
    TestForRead(31, 100);
    TestForRead(50, 50);
    TestForRead(100, 80);
}

void BufferedFileWrapperTest::TestCaseForSeek()
{
    string answer;
    uint32_t dataLength = 1000;
    uint32_t bufferSize = 27;
    MakeFileData(dataLength, answer);

    uint32_t stepLens[] = {10, 43, 400, 1000};
    for (uint32_t i = 0; i < sizeof(stepLens) / sizeof(stepLens[0]); ++i)
    {
        CheckSeek(stepLens[i], bufferSize, answer);
    }
}

void BufferedFileWrapperTest::TestCaseForWrite()
{
    TestForWrite(20, 7);
    TestForWrite(50, 50);
    TestForWrite(10, 50);
}

void BufferedFileWrapperTest::TestCaseForWriteMultiTimes()
{
    uint32_t bufferSize = 30;
    BufferedFileWrapper *writer = FileSystemWrapper::OpenBufferedFile(
            mFilePath, WRITE, bufferSize, mAsync);
    string answer;
    uint32_t cursor = 0;
    uint32_t writeLens[] = {10, 24, 44, 16};
    uint32_t writeTimes = sizeof(writeLens) / sizeof (writeLens[0]);

    for (uint32_t i = 0 ; i < writeTimes; ++i)
    {
        MakeData(writeLens[i], answer);
        writer->Write(answer.data() + cursor, writeLens[i]);
        cursor += writeLens[i];
        INDEXLIB_TEST_EQUAL((int64_t)cursor, writer->Tell());
    }

    writer->Close();
    delete writer;

    CheckAnswer(answer);
}

void BufferedFileWrapperTest::TestCaseForReadBigFile()
{
    size_t fileLength = 4L * 1024 * 1024 * 1024;
    uint32_t bufferSize = 2 * 1024 * 1024;
    MockBufferedFileWrapper fileReader(NULL, READ, fileLength, bufferSize);
    char* buf = new char[fileLength];

    EXPECT_CALL(fileReader, LoadBuffer(_)).Times(fileLength / bufferSize);
    INDEXLIB_TEST_EQUAL(fileLength, (size_t)fileReader.Read(buf, fileLength));
    for (size_t i = 0; i < fileLength; i += 133331)
    {
        INDEXLIB_TEST_EQUAL(buf[i], (char)(i % 128));
    }
    delete []buf;
}

void BufferedFileWrapperTest::TestForRead(uint32_t bufferSize, uint32_t dataLength)
{
    string answer;
    MakeFileData(dataLength, answer);
    BufferedFileWrapper *reader = FileSystemWrapper::OpenBufferedFile(
            mFilePath, READ, bufferSize, mAsync);
    INDEXLIB_TEST_EQUAL(mAsync, FileSystemWrapper::GetReadThreadPool() != NULL);
    CheckFileReader(*reader, answer);
    reader->Close();
    delete reader;
}

void BufferedFileWrapperTest::TestForWrite(uint32_t lengthToWrite, uint32_t bufferSize)
{
    BufferedFileWrapper *writer = FileSystemWrapper::OpenBufferedFile(
            mFilePath, WRITE, bufferSize, mAsync);
    INDEXLIB_TEST_EQUAL(mAsync, FileSystemWrapper::GetWriteThreadPool() != NULL);
    string answer;
    MakeData(lengthToWrite, answer);
    writer->Write(answer.data(), lengthToWrite);
    writer->Close();

    CheckAnswer(answer);
    delete writer;
}

void BufferedFileWrapperTest::CheckAnswer(const string& answer)
{
    INDEXLIB_TEST_EQUAL(answer.length(), FileSystemWrapper::GetFileLength(mFilePath));

    File* file = FileSystem::openFile(mFilePath, READ);
    INDEXLIB_TEST_TRUE(file->isOpened());

    char* data = new char[answer.length()];
    file->read((void*)data, answer.length());
    string strData(data, answer.length());
    INDEXLIB_TEST_EQUAL(strData, answer);

    delete []data;
    delete file;
}


void BufferedFileWrapperTest::MakeFileData(uint32_t dataLength, string& answer)
{
    MakeData(dataLength, answer);
    File* file = FileSystem::openFile(mFilePath, WRITE);
    INDEXLIB_TEST_TRUE(file->isOpened());
    file->write(answer.c_str(), answer.length());
    file->close();
    delete file;
}

void BufferedFileWrapperTest::MakeData(uint32_t dataLength, string& answer)
{
    answer.reserve(dataLength + 1);
    for (uint32_t i = 0; i < dataLength; ++i)
    {
        char word = i % 256;
        answer.append(1, word);
    }
}

void BufferedFileWrapperTest::CheckSeek(uint32_t stepLen, uint32_t bufferSize,
                                       const string& answer)
{
    uint32_t skipLens[] = {0, 9};

    BufferedFileWrapper *reader = FileSystemWrapper::OpenBufferedFile(
            mFilePath, fslib::READ, bufferSize, mAsync);

    for (uint32_t i = 0; i < sizeof(skipLens) / sizeof(skipLens[0]); ++i)
    {
        for (uint32_t offset = 0;
             offset < answer.size(); 
             offset += stepLen + skipLens[i])
        {
            reader->SetOffset(offset);
            CheckFileReader(*reader, answer.substr(offset, stepLen));
        }
    }
    reader->Close();
    delete reader;
}

void BufferedFileWrapperTest::CheckFileReader(BufferedFileWrapper& reader, 
        const string& answer)
{
    char* data = new char[answer.length()];
    reader.Read((void*)data, answer.length());

    string strData(data, answer.length());
    INDEXLIB_TEST_EQUAL(strData, answer);

    delete []data;
}

IE_NAMESPACE_END(storage);

