#include "indexlib/file_system/file/BufferedFileOutputStream.h"

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include <iosfwd>

#include "indexlib/util/testutil/unittest.h"

using namespace std;

namespace indexlib { namespace file_system {

struct MockBufferedFileNode : public BufferedFileNode {
    MockBufferedFileNode() {}

    MOCK_METHOD(ErrorCode, DoOpen, (const std::string&, FSOpenType, int64_t), (noexcept, override));
    MOCK_METHOD(FSResult<void>, Close, (), (noexcept, override));

    FSResult<size_t> Write(const void* data, size_t length) noexcept override
    {
        dataBuffer += std::string((const char*)data, length);
        _length += length;
        return {FSEC_OK, length};
    }

    std::string dataBuffer;
};
typedef std::shared_ptr<MockBufferedFileNode> MockBufferedFileNodePtr;

class BufferedFileOutputStreamTest : public INDEXLIB_TESTBASE
{
public:
    BufferedFileOutputStreamTest();
    ~BufferedFileOutputStreamTest();

    DECLARE_CLASS_NAME(BufferedFileOutputStreamTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSimpleProcess();

private:
    AUTIL_LOG_DECLARE();
};
AUTIL_LOG_SETUP(indexlib.file_system, BufferedFileOutputStreamTest);

INDEXLIB_UNIT_TEST_CASE(BufferedFileOutputStreamTest, TestSimpleProcess);
//////////////////////////////////////////////////////////////////////

BufferedFileOutputStreamTest::BufferedFileOutputStreamTest() {}

BufferedFileOutputStreamTest::~BufferedFileOutputStreamTest() {}

void BufferedFileOutputStreamTest::CaseSetUp() {}

void BufferedFileOutputStreamTest::CaseTearDown() {}

struct FakeBufferedFileOutputStream : public BufferedFileOutputStream {
    FakeBufferedFileOutputStream() : BufferedFileOutputStream(false) {}
    BufferedFileNode* CreateFileNode() const noexcept override { return mockNode; }

    MockBufferedFileNode* mockNode;
};

void BufferedFileOutputStreamTest::TestSimpleProcess()
{
    {
        FakeBufferedFileOutputStream stream;
        auto fileNode = new MockBufferedFileNode();
        stream.mockNode = fileNode;
        string path = "pangu://cluster1/a/b";
        string content = "helloworld";
        string expectedOpenPath = "pangu://cluster1/a/b";

        EXPECT_CALL(*fileNode, DoOpen(expectedOpenPath, FSOT_BUFFERED, _)).Times(1);
        EXPECT_CALL(*fileNode, Close()).WillOnce(Return(FSEC_OK));

        ASSERT_EQ(FSEC_OK, stream.Open(path, content.size()));
        ASSERT_EQ(FSEC_OK, stream.Write(content.data(), content.size()).Code());
        ASSERT_EQ(FSEC_OK, stream.Close());

        EXPECT_EQ(content, fileNode->dataBuffer);
    }
    {
        FakeBufferedFileOutputStream stream;
        auto fileNode = new MockBufferedFileNode();
        stream.mockNode = fileNode;
        string path = "pangu://cluster1/a/b";
        string content = "helloworld";
        string expectedOpenPath = "pangu://cluster1/a/b";

        EXPECT_CALL(*fileNode, DoOpen(expectedOpenPath, FSOT_BUFFERED, _)).Times(1);
        EXPECT_CALL(*fileNode, Close()).WillOnce(Return(FSEC_OK));

        ASSERT_EQ(FSEC_OK, stream.Open(path, -1));
        ASSERT_EQ(FSEC_OK, stream.Write(content.data(), content.size()).Code());
        ASSERT_EQ(FSEC_OK, stream.Close());

        EXPECT_EQ(content, fileNode->dataBuffer);
    }
    {
        FakeBufferedFileOutputStream stream;
        auto fileNode = new MockBufferedFileNode();
        stream.mockNode = fileNode;
        string path = "pangu://cluster1/a/b";
        string content = "helloworld";
        string expectedOpenPath = "pangu://cluster1/a/b";

        EXPECT_CALL(*fileNode, DoOpen(expectedOpenPath, FSOT_BUFFERED, _)).Times(1);
        EXPECT_CALL(*fileNode, Close()).WillOnce(Return(FSEC_OK));

        ASSERT_EQ(FSEC_OK, stream.Open(path, -1));
        ASSERT_EQ(FSEC_OK, stream.Write(content.data(), 3).Code());
        EXPECT_EQ(3u, stream.GetLength());
        ASSERT_EQ(FSEC_OK, stream.Write(content.data() + 3, content.size() - 3).Code());
        EXPECT_EQ(content.size(), stream.GetLength());
        ASSERT_EQ(FSEC_OK, stream.Close());

        EXPECT_EQ(content, fileNode->dataBuffer);
    }
    {
        FakeBufferedFileOutputStream stream;
        auto fileNode = new MockBufferedFileNode();
        stream.mockNode = fileNode;
        string path = "pangu://cluster1/a/b";
        string content = "abc";
        string expectedOpenPath = "pangu://cluster1/a/b";

        EXPECT_CALL(*fileNode, DoOpen(expectedOpenPath, FSOT_BUFFERED, _)).Times(1);
        EXPECT_CALL(*fileNode, Close()).WillOnce(Return(FSEC_OK));

        ASSERT_EQ(FSEC_OK, stream.Open(path, -1));
        ASSERT_EQ(FSEC_OK, stream.Write(content.data(), content.size()).Code());
        ASSERT_EQ(FSEC_OK, stream.Close());

        EXPECT_EQ(content, fileNode->dataBuffer);
    }
    {
        FakeBufferedFileOutputStream stream;
        auto fileNode = new MockBufferedFileNode();
        stream.mockNode = fileNode;
        string path = "pangu://cluster1/a/b";
        string content = "helloworld";
        string expectedOpenPath = "pangu://cluster1/a/b";

        EXPECT_CALL(*fileNode, DoOpen(expectedOpenPath, FSOT_BUFFERED, _)).Times(1);
        EXPECT_CALL(*fileNode, Close()).WillOnce(Return(FSEC_OK));

        ASSERT_EQ(FSEC_OK, stream.Open(path, -1));
        ASSERT_EQ(FSEC_OK, stream.Write(content.data(), content.size()).Code());
        ASSERT_EQ(FSEC_OK, stream.Close());

        EXPECT_EQ(content, fileNode->dataBuffer);
    }
    {
        FakeBufferedFileOutputStream stream;
        auto fileNode = new MockBufferedFileNode();
        stream.mockNode = fileNode;
        string path = "pangu://cluster1/a/b";
        string content = "a";
        string expectedOpenPath = "pangu://cluster1/a/b";

        EXPECT_CALL(*fileNode, DoOpen(expectedOpenPath, FSOT_BUFFERED, _)).Times(1);
        EXPECT_CALL(*fileNode, Close()).WillOnce(Return(FSEC_OK));

        ASSERT_EQ(FSEC_OK, stream.Open(path, -1));
        ASSERT_EQ(FSEC_OK, stream.Write(content.data(), content.size()).Code());
        ASSERT_EQ(FSEC_OK, stream.Close());

        EXPECT_EQ(content, fileNode->dataBuffer);
    }
}
}} // namespace indexlib::file_system
