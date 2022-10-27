#include <fstream>
#include <fslib/fslib.h>
#include "indexlib/util/path_util.h"
#include "indexlib/misc/exception.h"
#include "indexlib/file_system/test/buffered_file_writer_unittest.h"
#include "indexlib/file_system/buffered_file_output_stream.h"
#include "indexlib/file_system/buffered_file_node.h"
#include "indexlib/storage/file_system_wrapper.h"
#include "indexlib/file_system/test/mock_fslib_file.h"
#include "indexlib/storage/exception_trigger.h"
#include "indexlib/file_system/buffered_file_node.h"

using namespace std;
using namespace fslib;
using namespace fslib::fs;
IE_NAMESPACE_USE(util);
IE_NAMESPACE_USE(storage);
IE_NAMESPACE_USE(misc);

IE_NAMESPACE_BEGIN(file_system);
IE_LOG_SETUP(file_system, BufferedFileWriterTest);

namespace {
    class MockStream : public BufferedFileOutputStream
    {
    public:
        MockStream()
            : BufferedFileOutputStream(storage::RaidConfigPtr())
        {
        }
        MOCK_METHOD0(Close, void());
    };
    DEFINE_SHARED_PTR(MockStream);
}

BufferedFileWriterTest::BufferedFileWriterTest()
{
}

BufferedFileWriterTest::~BufferedFileWriterTest()
{
}

void BufferedFileWriterTest::CaseSetUp()
{
}

void BufferedFileWriterTest::CaseTearDown()
{
}

void BufferedFileWriterTest::TestSimpleProcess()
{
    string fileName = PathUtil::JoinPath(GET_TEST_DATA_PATH(), "test");
    BufferedFileWriterPtr writer(new BufferedFileWriter());
    writer->Open(fileName);
    writer->Write("abc", 3);
    writer->Close();
    writer.reset();
    CheckFile(fileName, "abc");
}

void BufferedFileWriterTest::TestCloseException()
{
    string fileName = PathUtil::JoinPath(GET_TEST_DATA_PATH(), "test");
    FSWriterParam param(true, false);
    param.bufferSize = 2;
    BufferedFileWriterPtr writer(new BufferedFileWriter(param));
    writer->Open(fileName);    
    auto mockStream = new MockStream();
    mockStream->Open(writer->mStream->mPath);
    writer->mStream.reset(mockStream);
    EXPECT_CALL(*mockStream, Close())
        .WillOnce(Throw(FileIOException()));
    writer->Write("abc", 3);
    ASSERT_THROW(writer->Close(), FileIOException);
    writer.reset();
    ASSERT_FALSE(FileSystemWrapper::IsExist(fileName));
    string tmpFileName = fileName + TEMP_FILE_SUFFIX;
    ASSERT_TRUE(FileSystemWrapper::IsExist(tmpFileName));
}


void BufferedFileWriterTest::CheckFile(const string& fileName, const string& content)
{
    ifstream in(fileName.c_str());
    ASSERT_TRUE(static_cast<bool>(in));

    in.seekg(0, in.end);
    size_t size = in.tellg();
    in.seekg(0, in.beg);
    ASSERT_EQ(content.size(), size);

    char* buffer = new char[size];
    in.read(buffer, size);
    in.close();
    ASSERT_EQ(content, string(buffer, size));
    delete [] buffer;
}

void BufferedFileWriterTest::TestBufferedFileException()
{
    string fileName = PathUtil::JoinPath(GET_TEST_DATA_PATH(), "test");
    FSWriterParam param(true, false);
    param.bufferSize = 1;
    BufferedFileWriterPtr writer(new BufferedFileWriter(param));
    writer->Open(fileName);

    BufferedFileOutputStreamPtr stream(new BufferedFileOutputStream(storage::RaidConfigPtr()));

    MockFslibFile* mockFile = new MockFslibFile(fileName);
    fslib::fs::FilePtr file(mockFile);
    auto fileNode = new BufferedFileNode(false);
    fileNode->mFile = std::move(file);
    stream->mFileNode.reset(fileNode);
    writer->mStream = stream;
    EXPECT_CALL(*mockFile, write(_,_))
        .WillRepeatedly(Return(0));
    EXPECT_CALL(*mockFile, isOpened())
        .WillRepeatedly(Return(true));
    EXPECT_CALL(*mockFile, close())
        .WillRepeatedly(Return(fslib::EC_OK));
    ASSERT_THROW(writer->Write("abc", 3), misc::FileIOException);
    ASSERT_THROW(writer->Close(), misc::FileIOException);
    writer.reset();
}

IE_NAMESPACE_END(file_system);

