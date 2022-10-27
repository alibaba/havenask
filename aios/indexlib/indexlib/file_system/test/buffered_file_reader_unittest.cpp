#include <fslib/fslib.h>
#include "indexlib/file_system/buffered_file_node.h"
#include "indexlib/file_system/test/buffered_file_reader_unittest.h"
#include "indexlib/file_system/test/test_file_creator.h"

using namespace std;
using namespace fslib;
using namespace fslib::fs;

IE_NAMESPACE_BEGIN(file_system);
IE_LOG_SETUP(file_system, BufferedFileReaderTest);

class MockBufferedFileReader : public BufferedFileReader
{
public:
    MockBufferedFileReader(int64_t fileLength, const FileNodePtr& fileNode, 
                           uint32_t bufferSize = FSReaderParam::DEFAULT_BUFFER_SIZE)
        : BufferedFileReader(fileNode, bufferSize)
    {
        mFileLength = fileLength;
        char *baseAddr = mBuffer->GetBaseAddr();
        for (uint32_t i = 0; i < bufferSize; i++)
        {
            baseAddr[i] = i % 128;
        }
    }

    MOCK_METHOD4(DoRead, size_t(void*, size_t, size_t, ReadOption));
};


BufferedFileReaderTest::BufferedFileReaderTest()
{
}

BufferedFileReaderTest::~BufferedFileReaderTest()
{
}

void BufferedFileReaderTest::CaseSetUp()
{
    string dir = GET_TEST_DATA_PATH();
    mFilePath = dir + "test_file";
}

void BufferedFileReaderTest::CaseTearDown()
{
    if (FileSystem::isExist(mFilePath) == EC_TRUE)
    {
        FileSystem::remove(mFilePath);
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

#define CALL_READ(func)                                                 \
    static_cast<size_t (FileNode::*)(void*, size_t, size_t, ReadOption)>(func)

void BufferedFileReaderTest::TestCaseForOffsetWithinOneBuffer()
{
    //3 buffer(5, 5, 1)
    FileNodePtr fileNode = PrepareFileNode();
    MockBufferedFileReader reader(11, fileNode, 5);

    char readBuffer[2];
    EXPECT_CALL(reader, DoRead(_, _, _, _))
        .Times(1)
        .WillRepeatedly(Invoke(fileNode.get(), CALL_READ(&FileNode::Read)));

    INDEXLIB_TEST_EQUAL((size_t)2, reader.Read(readBuffer, 2, 0));
    INDEXLIB_TEST_EQUAL(string("ab"), string(readBuffer, 2));

    INDEXLIB_TEST_EQUAL((size_t)2, reader.Read(readBuffer, 2, 2));
    INDEXLIB_TEST_EQUAL(string("cd"), string(readBuffer, 2));

    INDEXLIB_TEST_EQUAL((size_t)1, reader.Read(readBuffer, 1, 4));
    INDEXLIB_TEST_EQUAL(string("e"), string(readBuffer, 1));
}

void BufferedFileReaderTest::TestCaseForSkipForwardThenBackward()
{
    //3 buffer(5, 5, 1)
    FileNodePtr fileNode = PrepareFileNode();
    MockBufferedFileReader reader(11, fileNode, 5);

    char readBuffer[2];
    EXPECT_CALL(reader, DoRead(_, _, _, _))
        .Times(2)
        .WillRepeatedly(Invoke(fileNode.get(), CALL_READ(&FileNode::Read)));
                        
    //forward
    INDEXLIB_TEST_EQUAL((size_t)2, reader.Read(readBuffer, 2, 7));
    // TODO: inline temp to EQUAL will be core
    // string temp(readBuffer,2);
    // INDEXLIB_TEST_EQUAL(string("hi"), temp);
    ASSERT_EQ('h', readBuffer[0]);
    ASSERT_EQ('i', readBuffer[1]);

    //backward
    INDEXLIB_TEST_EQUAL((size_t)2, reader.Read(readBuffer, 2, 2));
    INDEXLIB_TEST_EQUAL(string("cd"), string(readBuffer, 2));
}

void BufferedFileReaderTest::TestCaseForRevertReadAllFileWithOffset()
{
    size_t fileLength = 30 * 1024 * 1024;
    string answer;
    FileReaderPtr reader = PrepareFileReader(answer, fileLength);

    int stepSize = 1536 * 1024;
    char* data = new char[fileLength];
    for (int i = fileLength; i > 0; i -= stepSize)
    {
        reader->Read((void*)(data + i - stepSize), stepSize, i - stepSize);
    }

    string strData(data, fileLength);
    INDEXLIB_TEST_EQUAL(strData, answer);

    delete []data;
}

void BufferedFileReaderTest::TestCaseForSeqReadAllFileWithOffset()
{
    size_t fileLength = 30 * 1024 * 1024;
    string answer;
    FileReaderPtr reader = PrepareFileReader(answer, fileLength);

    int stepSize = 1536 * 1024;
    char* data = new char[fileLength];
    for (size_t i = 0; i < fileLength; i += stepSize)
    {
        reader->Read((void*)(data + i), stepSize, i);
    }

    string strData(data, fileLength);
    INDEXLIB_TEST_EQUAL(strData, answer);

    delete []data;
}

void BufferedFileReaderTest::TestCaseForReadBigFile()
{
    size_t fileLength = 4L * 1024 * 1024 * 1024;
    uint32_t bufferSize = 2 * 1024 * 1024;
    BufferedFileNodePtr fileNode(new BufferedFileNode());
    MockBufferedFileReader fileReader(fileLength, fileNode, bufferSize);
    char* buf = new char[fileLength];

    EXPECT_CALL(fileReader, DoRead(_, _, _, _))
        .Times(fileLength / bufferSize)
        .WillRepeatedly(Return(bufferSize));
    INDEXLIB_TEST_EQUAL(fileLength, (size_t)fileReader.Read(buf, fileLength));
    for (size_t i = 0; i < fileLength; i += 133331)
    {
        INDEXLIB_TEST_EQUAL(buf[i], (char)(i % 128));
    }
    delete []buf;
}

void BufferedFileReaderTest::ForReadCase(uint32_t bufferSize, 
        uint32_t dataLength, bool async)
{
    string answer;
    MakeFileData(dataLength, answer);

    BufferedFileNodePtr fileNode(new BufferedFileNode());
    fileNode->Open(mFilePath, FSOT_BUFFERED);
    BufferedFileReader reader(fileNode, bufferSize);
    if (async)
    {
        reader.ResetBufferParam(8, true);
    }
    CheckFileReader(reader, answer);
}

void BufferedFileReaderTest::MakeFileData(uint32_t dataLength, string& answer)
{
    MakeData(dataLength, answer);
    FilePtr file(FileSystem::openFile(mFilePath, WRITE));
    INDEXLIB_TEST_TRUE(file->isOpened());
    file->write(answer.c_str(), answer.length());
    file->close();
}

void BufferedFileReaderTest::MakeData(uint32_t dataLength, string& answer)
{
    answer.reserve(dataLength + 1);
    for (uint32_t i = 0; i < dataLength; ++i)
    {
        char word = i % 256;
        answer.append(1, word);
    }
}

void BufferedFileReaderTest::CheckFileReader(BufferedFileReader& reader, 
        const string& answer)
{
    char* data = new char[answer.length()];
    reader.Read((void*)data, answer.length());

    string strData(data, answer.length());
    INDEXLIB_TEST_EQUAL(strData, answer);

    delete []data;
}

FileNodePtr BufferedFileReaderTest::PrepareFileNode()
{
    char data[128];
    size_t bufferSize = sizeof(data);
    for (uint32_t i = 0; i < bufferSize; i++)
    {
        data[i] = i % 26 + 'a';
    }
    FileReaderPtr fileReader = TestFileCreator::CreateReader(
            GET_FILE_SYSTEM(), FSST_LOCAL, FSOT_BUFFERED, 
            string(data, bufferSize));
    return fileReader->GetFileNode();
}

FileReaderPtr BufferedFileReaderTest::PrepareFileReader(string& answer, 
        size_t fileLength)
{
    MakeFileData(fileLength, answer);
    IndexlibFileSystemPtr fileSystem = GET_FILE_SYSTEM();
    return fileSystem->CreateFileReader(mFilePath, FSOT_BUFFERED);    
}

IE_NAMESPACE_END(file_system);

