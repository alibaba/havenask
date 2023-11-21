#include "indexlib/file_system/file/BufferedFileWriter.h"

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include <fstream>
#include <memory>
#include <stddef.h>
#include <stdint.h>
#include <string>
#include <utility>

#include "autil/Log.h"
#include "fslib/common/common_type.h"
#include "fslib/fs/File.h"
#include "indexlib/file_system/ErrorCode.h"
#include "indexlib/file_system/FileSystemDefine.h"
#include "indexlib/file_system/WriterOption.h"
#include "indexlib/file_system/file/BufferedFileNode.h"
#include "indexlib/file_system/file/BufferedFileOutputStream.h"
#include "indexlib/file_system/file/FileWriter.h"
#include "indexlib/file_system/file/test/MockFslibFile.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/util/Exception.h"
#include "indexlib/util/PathUtil.h"
#include "indexlib/util/byte_slice_list/ByteSliceList.h"
#include "indexlib/util/testutil/unittest.h"
#include "unittest/unittest.h"

using namespace std;
using namespace indexlib::util;

namespace indexlib { namespace file_system {

class BufferedFileWriterTest : public INDEXLIB_TESTBASE
{
public:
    BufferedFileWriterTest();
    ~BufferedFileWriterTest();

    DECLARE_CLASS_NAME(BufferedFileWriterTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSimpleProcess();
    void TestCloseException();
    void TestBufferedFileException();

private:
    void CheckFile(const std::string& fileName, const std::string& content);

private:
    AUTIL_LOG_DECLARE();
};
AUTIL_LOG_SETUP(indexlib.file_system, BufferedFileWriterTest);

INDEXLIB_UNIT_TEST_CASE(BufferedFileWriterTest, TestSimpleProcess);
INDEXLIB_UNIT_TEST_CASE(BufferedFileWriterTest, TestCloseException);
INDEXLIB_UNIT_TEST_CASE(BufferedFileWriterTest, TestBufferedFileException);
//////////////////////////////////////////////////////////////////////

#define EMPTY_UPDATE_FILE_SIZE_FUNC [](int64_t size) {}

namespace {
class MockStream : public BufferedFileOutputStream
{
public:
    MockStream() : BufferedFileOutputStream(false) {}
    MOCK_METHOD(FSResult<void>, Close, (), (noexcept, override));
};
typedef std::shared_ptr<MockStream> MockStreamPtr;
} // namespace

BufferedFileWriterTest::BufferedFileWriterTest() {}

BufferedFileWriterTest::~BufferedFileWriterTest() {}

void BufferedFileWriterTest::CaseSetUp() {}

void BufferedFileWriterTest::CaseTearDown() {}

void BufferedFileWriterTest::TestSimpleProcess()
{
    string fileName = PathUtil::JoinPath(GET_TEMP_DATA_PATH(), "test");
    WriterOption option;
    BufferedFileWriterPtr writer(new BufferedFileWriter(option, EMPTY_UPDATE_FILE_SIZE_FUNC));
    ASSERT_EQ(FSEC_OK, writer->Open(fileName, fileName));
    ASSERT_EQ(FSEC_OK, writer->Write("abc", 3).Code());
    ASSERT_EQ(FSEC_OK, writer->Close());
    writer.reset();
    CheckFile(fileName, "abc");
}

void BufferedFileWriterTest::TestCloseException()
{
    string fileName = PathUtil::JoinPath(GET_TEMP_DATA_PATH(), "test");
    WriterOption option;
    option.atomicDump = true;
    option.copyOnDump = false;
    option.bufferSize = 2;
    BufferedFileWriterPtr writer(new BufferedFileWriter(option, EMPTY_UPDATE_FILE_SIZE_FUNC));
    ASSERT_EQ(FSEC_OK, writer->Open(fileName, fileName));
    auto mockStream = new MockStream();
    ASSERT_EQ(FSEC_OK, mockStream->Open(writer->_stream->_path, -1));
    writer->_stream.reset(mockStream);
    EXPECT_CALL(*mockStream, Close()).WillOnce(Return(FSEC_ERROR));
    ASSERT_EQ(FSEC_OK, writer->Write("abc", 3).Code());
    ASSERT_EQ(FSEC_ERROR, writer->Close());
    writer.reset();
    ASSERT_FALSE(FslibWrapper::IsExist(fileName).GetOrThrow());
    string tmpFileName = fileName + TEMP_FILE_SUFFIX;
    ASSERT_TRUE(FslibWrapper::IsExist(tmpFileName).GetOrThrow());
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
    delete[] buffer;
}

void BufferedFileWriterTest::TestBufferedFileException()
{
    string fileName = PathUtil::JoinPath(GET_TEMP_DATA_PATH(), "test");
    WriterOption option;
    option.atomicDump = true;
    option.copyOnDump = false;
    option.bufferSize = 1;
    BufferedFileWriterPtr writer(new BufferedFileWriter(option, EMPTY_UPDATE_FILE_SIZE_FUNC));
    ASSERT_EQ(FSEC_OK, writer->Open(fileName, fileName));

    std::shared_ptr<BufferedFileOutputStream> stream(new BufferedFileOutputStream(false));

    MockFslibFile* mockFile = new MockFslibFile(fileName);
    fslib::fs::FilePtr file(mockFile);
    auto fileNode = new BufferedFileNode(fslib::WRITE);
    fileNode->_file = std::move(file);
    stream->_fileNode.reset(fileNode);
    writer->_stream = stream;
    EXPECT_CALL(*mockFile, write(_, _)).WillRepeatedly(Return(0));
    EXPECT_CALL(*mockFile, isOpened()).WillRepeatedly(Return(true));
    EXPECT_CALL(*mockFile, close()).WillRepeatedly(Return(fslib::EC_OK));
    ASSERT_EQ(FSEC_ERROR, writer->Write("abc", 3).Code());
    ASSERT_EQ(FSEC_ERROR, writer->Close());
    writer.reset();
}

#undef EMPTY_UPDATE_FILE_SIZE_FUNC
}} // namespace indexlib::file_system
