#include "indexlib/file_system/file/BufferedFileReader.h"

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>

#include "autil/Log.h"
#include "fslib/common/common_type.h"
#include "fslib/fs/File.h"
#include "fslib/fs/FileSystem.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/FileSystemCreator.h"
#include "indexlib/file_system/FileSystemDefine.h"
#include "indexlib/file_system/ReaderOption.h"
#include "indexlib/file_system/file/BufferedFileNode.h"
#include "indexlib/file_system/file/FileBuffer.h"
#include "indexlib/file_system/file/FileNode.h"
#include "indexlib/file_system/file/FileReader.h"
#include "indexlib/file_system/file/ReadOption.h"
#include "indexlib/file_system/test/TestFileCreator.h"
#include "indexlib/util/PathUtil.h"
#include "indexlib/util/testutil/unittest.h"
#include "unittest/unittest.h"

using namespace std;

namespace indexlib { namespace file_system {

class BufferedFileReaderTest : public INDEXLIB_TESTBASE
{
public:
    BufferedFileReaderTest();
    ~BufferedFileReaderTest();

    DECLARE_CLASS_NAME(BufferedFileReaderTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestCaseForRead();
    void TestCaseForOffsetWithinOneBuffer();
    void TestCaseForSkipForwardThenBackward();
    void TestCaseForRevertReadAllFileWithOffset();
    void TestCaseForSeqReadAllFileWithOffset();
    void TestCaseForReadBigFile();

private:
    void ForReadCase(uint32_t bufferSize, uint32_t dataLength, bool async);
    void MakeFileData(uint32_t dataLength, std::string& answer);
    void MakeData(uint32_t dataLength, std::string& answer);
    void CheckFileReader(BufferedFileReader& reader, const std::string& answer);

    std::shared_ptr<FileNode> PrepareFileNode();
    std::shared_ptr<FileReader> PrepareFileReader(std::string& answer, size_t fileLength);

private:
    std::string _filePath;
    std::shared_ptr<IFileSystem> _fileSystem;

private:
    AUTIL_LOG_DECLARE();
};
AUTIL_LOG_SETUP(indexlib.file_system, BufferedFileReaderTest);

INDEXLIB_UNIT_TEST_CASE(BufferedFileReaderTest, TestCaseForRead);
INDEXLIB_UNIT_TEST_CASE(BufferedFileReaderTest, TestCaseForOffsetWithinOneBuffer);
INDEXLIB_UNIT_TEST_CASE(BufferedFileReaderTest, TestCaseForSkipForwardThenBackward);
INDEXLIB_UNIT_TEST_CASE(BufferedFileReaderTest, TestCaseForRevertReadAllFileWithOffset);
INDEXLIB_UNIT_TEST_CASE(BufferedFileReaderTest, TestCaseForSeqReadAllFileWithOffset);
INDEXLIB_UNIT_TEST_CASE(BufferedFileReaderTest, TestCaseForReadBigFile);
//////////////////////////////////////////////////////////////////////

class MockBufferedFileReader : public BufferedFileReader
{
public:
    MockBufferedFileReader(int64_t fileLength, const std::shared_ptr<FileNode>& fileNode,
                           uint32_t bufferSize = ReaderOption::DEFAULT_BUFFER_SIZE)
        : BufferedFileReader(fileNode, bufferSize)
    {
        _fileLength = fileLength;
        char* baseAddr = _buffer->GetBaseAddr();
        for (uint32_t i = 0; i < bufferSize; i++) {
            baseAddr[i] = i % 128;
        }
    }

    MOCK_METHOD(FSResult<size_t>, DoRead, (void*, size_t, size_t, ReadOption), (noexcept, override));
};

BufferedFileReaderTest::BufferedFileReaderTest() {}

BufferedFileReaderTest::~BufferedFileReaderTest() {}

void BufferedFileReaderTest::CaseSetUp()
{
    _filePath = util::PathUtil::NormalizePath(GET_TEMP_DATA_PATH() + "test_file");
    _fileSystem = FileSystemCreator::Create("ut", GET_TEMP_DATA_PATH()).GetOrThrow();
}

void BufferedFileReaderTest::CaseTearDown()
{
    if (fslib::fs::FileSystem::isExist(_filePath) == fslib::EC_TRUE) {
        fslib::fs::FileSystem::remove(_filePath);
    }
}

void BufferedFileReaderTest::TestCaseForRead()
{
    ForReadCase(20, 100, false);
    ForReadCase(31, 100, false);
    ForReadCase(50, 50, false);
    ForReadCase(100, 80, false);

    ForReadCase(20, 100, true);
    ForReadCase(31, 100, true);
    ForReadCase(50, 50, true);
    ForReadCase(100, 80, true);
}

#define CALL_READ(func) static_cast<FSResult<size_t> (FileNode::*)(void*, size_t, size_t, ReadOption)>(func)

void BufferedFileReaderTest::TestCaseForOffsetWithinOneBuffer()
{
    // 3 buffer(5, 5, 1)
    std::shared_ptr<FileNode> fileNode = PrepareFileNode();
    MockBufferedFileReader reader(11, fileNode, 5);

    char readBuffer[2];
    EXPECT_CALL(reader, DoRead(_, _, _, _)).Times(1).WillRepeatedly(Invoke(fileNode.get(), CALL_READ(&FileNode::Read)));

    ASSERT_EQ((size_t)2, reader.Read(readBuffer, 2, 0, ReadOption()).GetOrThrow());
    ASSERT_EQ(string("ab"), string(readBuffer, 2));

    ASSERT_EQ((size_t)2, reader.Read(readBuffer, 2, 2, ReadOption()).GetOrThrow());
    ASSERT_EQ(string("cd"), string(readBuffer, 2));

    ASSERT_EQ((size_t)1, reader.Read(readBuffer, 1, 4, ReadOption()).GetOrThrow());
    ASSERT_EQ(string("e"), string(readBuffer, 1));
}

void BufferedFileReaderTest::TestCaseForSkipForwardThenBackward()
{
    // 3 buffer(5, 5, 1)
    std::shared_ptr<FileNode> fileNode = PrepareFileNode();
    MockBufferedFileReader reader(11, fileNode, 5);

    char readBuffer[2];
    EXPECT_CALL(reader, DoRead(_, _, _, _)).Times(2).WillRepeatedly(Invoke(fileNode.get(), CALL_READ(&FileNode::Read)));

    // forward
    ASSERT_EQ((size_t)2, reader.Read(readBuffer, 2, 7, ReadOption()).GetOrThrow());
    // TODO: inline temp to EQUAL will be core
    // string temp(readBuffer,2);
    // ASSERT_EQ(string("hi"), temp);
    ASSERT_EQ('h', readBuffer[0]);
    ASSERT_EQ('i', readBuffer[1]);

    // backward
    ASSERT_EQ((size_t)2, reader.Read(readBuffer, 2, 2, ReadOption()).GetOrThrow());
    ASSERT_EQ(string("cd"), string(readBuffer, 2));
}

void BufferedFileReaderTest::TestCaseForRevertReadAllFileWithOffset()
{
    size_t fileLength = 30 * 1024 * 1024;
    string answer;
    std::shared_ptr<FileReader> reader = PrepareFileReader(answer, fileLength);

    int stepSize = 1536 * 1024;
    char* data = new char[fileLength];
    for (int i = fileLength; i > 0; i -= stepSize) {
        ASSERT_EQ(FSEC_OK, reader->Read((void*)(data + i - stepSize), stepSize, i - stepSize).Code());
    }

    string strData(data, fileLength);
    ASSERT_EQ(strData, answer);

    delete[] data;
}

void BufferedFileReaderTest::TestCaseForSeqReadAllFileWithOffset()
{
    size_t fileLength = 30 * 1024 * 1024;
    string answer;
    std::shared_ptr<FileReader> reader = PrepareFileReader(answer, fileLength);

    int stepSize = 1536 * 1024;
    char* data = new char[fileLength];
    for (size_t i = 0; i < fileLength; i += stepSize) {
        ASSERT_EQ(FSEC_OK, reader->Read((void*)(data + i), stepSize, i).Code());
    }

    string strData(data, fileLength);
    ASSERT_EQ(strData, answer);

    delete[] data;
}

void BufferedFileReaderTest::TestCaseForReadBigFile()
{
    size_t fileLength = 4L * 1024 * 1024 * 1024;
    uint32_t bufferSize = 2 * 1024 * 1024;
    std::shared_ptr<BufferedFileNode> fileNode(new BufferedFileNode());
    MockBufferedFileReader fileReader(fileLength, fileNode, bufferSize);
    char* buf = new char[fileLength];

    EXPECT_CALL(fileReader, DoRead(_, _, _, _))
        .Times(fileLength / bufferSize)
        .WillRepeatedly(Return(FSResult<size_t>(FSEC_OK, bufferSize)));
    ASSERT_EQ(fileLength, (size_t)fileReader.Read(buf, fileLength, ReadOption()).GetOrThrow());
    for (size_t i = 0; i < fileLength; i += 133331) {
        ASSERT_EQ(buf[i], (char)(i % 128));
    }
    delete[] buf;
}

void BufferedFileReaderTest::ForReadCase(uint32_t bufferSize, uint32_t dataLength, bool async)
{
    string answer;
    MakeFileData(dataLength, answer);

    std::shared_ptr<BufferedFileNode> fileNode(new BufferedFileNode());
    ASSERT_EQ(FSEC_OK, fileNode->Open("LOGICAL_PATH", _filePath, FSOT_BUFFERED, -1));
    BufferedFileReader reader(fileNode, bufferSize);
    if (async) {
        reader.ResetBufferParam(8, true);
    }
    CheckFileReader(reader, answer);
}

void BufferedFileReaderTest::MakeFileData(uint32_t dataLength, string& answer)
{
    MakeData(dataLength, answer);
    fslib::fs::FilePtr file(fslib::fs::FileSystem::openFile(_filePath, fslib::WRITE));
    ASSERT_TRUE(file->isOpened());
    file->write(answer.c_str(), answer.length());
    file->close();
}

void BufferedFileReaderTest::MakeData(uint32_t dataLength, string& answer)
{
    answer.reserve(dataLength + 1);
    for (uint32_t i = 0; i < dataLength; ++i) {
        char word = i % 256;
        answer.append(1, word);
    }
}

void BufferedFileReaderTest::CheckFileReader(BufferedFileReader& reader, const string& answer)
{
    char* data = new char[answer.length()];
    ASSERT_EQ(FSEC_OK, reader.Read((void*)data, answer.length(), ReadOption()).Code());

    string strData(data, answer.length());
    ASSERT_EQ(strData, answer);

    delete[] data;
}

std::shared_ptr<FileNode> BufferedFileReaderTest::PrepareFileNode()
{
    char data[128];
    size_t bufferSize = sizeof(data);
    for (uint32_t i = 0; i < bufferSize; i++) {
        data[i] = i % 26 + 'a';
    }
    std::shared_ptr<FileReader> fileReader =
        TestFileCreator::CreateReader(_fileSystem, FSST_DISK, FSOT_BUFFERED, string(data, bufferSize));
    return fileReader->GetFileNode();
}

std::shared_ptr<FileReader> BufferedFileReaderTest::PrepareFileReader(string& answer, size_t fileLength)
{
    MakeFileData(fileLength, answer);
    BufferedFileReaderPtr reader(new BufferedFileReader());
    EXPECT_EQ(FSEC_OK, reader->Open(_filePath));
    return reader;
}
}} // namespace indexlib::file_system
