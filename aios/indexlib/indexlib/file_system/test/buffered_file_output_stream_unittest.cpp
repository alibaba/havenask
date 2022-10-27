#include "indexlib/file_system/test/buffered_file_output_stream_unittest.h"

using namespace std;


IE_NAMESPACE_BEGIN(file_system);
IE_LOG_SETUP(file_system, BufferedFileOutputStreamTest);

BufferedFileOutputStreamTest::BufferedFileOutputStreamTest()
{
}

BufferedFileOutputStreamTest::~BufferedFileOutputStreamTest()
{
}

void BufferedFileOutputStreamTest::CaseSetUp()
{
}

void BufferedFileOutputStreamTest::CaseTearDown()
{
}

struct FakeBufferedFileOutputStream : public BufferedFileOutputStream
{
    FakeBufferedFileOutputStream(const storage::RaidConfigPtr& raidConfig)
        : BufferedFileOutputStream(raidConfig)
    {
    }
    BufferedFileNode* CreateFileNode() const override { return mockNode; }

    MockBufferedFileNode* mockNode;
};


namespace
{

storage::RaidConfigPtr makeRaidConfig(bool useRaid, size_t bufferSizeThreshold)
{
    storage::RaidConfigPtr raidConfig(new storage::RaidConfig);
    raidConfig->useRaid = useRaid;
    raidConfig->bufferSizeThreshold = bufferSizeThreshold;
    return raidConfig;
}

}

void BufferedFileOutputStreamTest::TestSimpleProcess()
{
    {
        auto raidConfig = makeRaidConfig(true, 4);
        FakeBufferedFileOutputStream stream(raidConfig);
        auto fileNode = new MockBufferedFileNode();
        stream.mockNode = fileNode;
        string path = "pangu://cluster1/a/b";
        string content = "helloworld";
        string expectedOpenPath = "pangu://cluster1?use_raid=true/a/b";

        EXPECT_CALL(*fileNode, DoOpen(expectedOpenPath, FSOT_BUFFERED)).Times(1);
        EXPECT_CALL(*fileNode, Close()).Times(1);

        stream.Open(path);
        stream.Write(content.data(), content.size());
        stream.Close();

        EXPECT_EQ(content, fileNode->dataBuffer);
    }
    {
        auto raidConfig = makeRaidConfig(true, 100);
        FakeBufferedFileOutputStream stream(raidConfig);
        auto fileNode = new MockBufferedFileNode();
        stream.mockNode = fileNode;
        string path = "pangu://cluster1/a/b";
        string content = "helloworld";
        string expectedOpenPath = "pangu://cluster1/a/b";

        EXPECT_CALL(*fileNode, DoOpen(expectedOpenPath, FSOT_BUFFERED)).Times(1);
        EXPECT_CALL(*fileNode, Close()).Times(1);

        stream.Open(path);
        stream.Write(content.data(), content.size());
        stream.Close();

        EXPECT_EQ(content, fileNode->dataBuffer);
    }
    {
        auto raidConfig = makeRaidConfig(true, 4);
        FakeBufferedFileOutputStream stream(raidConfig);
        auto fileNode = new MockBufferedFileNode();
        stream.mockNode = fileNode;
        string path = "pangu://cluster1/a/b";
        string content = "helloworld";
        string expectedOpenPath = "pangu://cluster1?use_raid=true/a/b";

        EXPECT_CALL(*fileNode, DoOpen(expectedOpenPath, FSOT_BUFFERED)).Times(1);
        EXPECT_CALL(*fileNode, Close()).Times(1);

        stream.Open(path);
        stream.Write(content.data(), 3);
        EXPECT_EQ(3u, stream.GetLength());
        stream.Write(content.data() + 3, content.size() - 3);
        EXPECT_EQ(content.size(), stream.GetLength());
        stream.Close();

        EXPECT_EQ(content, fileNode->dataBuffer);
    }
    {
        auto raidConfig = makeRaidConfig(true, 4);
        FakeBufferedFileOutputStream stream(raidConfig);
        auto fileNode = new MockBufferedFileNode();
        stream.mockNode = fileNode;
        string path = "pangu://cluster1/a/b";
        string content = "abc";
        string expectedOpenPath = "pangu://cluster1/a/b";

        EXPECT_CALL(*fileNode, DoOpen(expectedOpenPath, FSOT_BUFFERED)).Times(1);
        EXPECT_CALL(*fileNode, Close()).Times(1);

        stream.Open(path);
        stream.Write(content.data(), content.size());
        stream.Close();

        EXPECT_EQ(content, fileNode->dataBuffer);
    }
    {
        auto raidConfig = makeRaidConfig(false, 4);
        FakeBufferedFileOutputStream stream(raidConfig);
        auto fileNode = new MockBufferedFileNode();
        stream.mockNode = fileNode;
        string path = "pangu://cluster1/a/b";
        string content = "helloworld";
        string expectedOpenPath = "pangu://cluster1/a/b";

        EXPECT_CALL(*fileNode, DoOpen(expectedOpenPath, FSOT_BUFFERED)).Times(1);
        EXPECT_CALL(*fileNode, Close()).Times(1);

        stream.Open(path);
        stream.Write(content.data(), content.size());
        stream.Close();

        EXPECT_EQ(content, fileNode->dataBuffer);
    }
    {
        auto raidConfig = makeRaidConfig(true, 0);
        FakeBufferedFileOutputStream stream(raidConfig);
        auto fileNode = new MockBufferedFileNode();
        stream.mockNode = fileNode;
        string path = "pangu://cluster1/a/b";
        string content = "a";
        string expectedOpenPath = "pangu://cluster1?use_raid=true/a/b";

        EXPECT_CALL(*fileNode, DoOpen(expectedOpenPath, FSOT_BUFFERED)).Times(1);
        EXPECT_CALL(*fileNode, Close()).Times(1);

        stream.Open(path);
        stream.Write(content.data(), content.size());
        stream.Close();

        EXPECT_EQ(content, fileNode->dataBuffer);
    }                
}

IE_NAMESPACE_END(file_system);

