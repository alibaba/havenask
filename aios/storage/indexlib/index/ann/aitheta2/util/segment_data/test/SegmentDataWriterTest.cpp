#include "indexlib/index/ann/aitheta2/util/segment_data/SegmentDataWriter.h"

#include "indexlib/file_system/FileSystemCreator.h"
#include "indexlib/file_system/file/FileReader.h"
#include "unittest/unittest.h"

using namespace autil::legacy;
using namespace std;
using namespace indexlib::file_system;
namespace indexlibv2::index::ann {

class SegmentDataWriterTest : public TESTBASE
{
public:
    SegmentDataWriterTest() = default;
    ~SegmentDataWriterTest() = default;

public:
    void setUp() override
    {
        auto testRoot = GET_TEMP_DATA_PATH();
        _fs = indexlib::file_system::FileSystemCreator::Create("SegmentDataWriterTest", testRoot).GetOrThrow();
        _dir = indexlib::file_system::Directory::Get(_fs);
    }

public:
    void testEmpty();
    void testOneIndexId();
    void testIndexIds();
    void testReentrance();

private:
    std::shared_ptr<IFileSystem> _fs;
    std::shared_ptr<Directory> _dir;
    AUTIL_LOG_DECLARE();
};
AUTIL_LOG_SETUP(indexlib.index, SegmentDataWriterTest);

// INDEXLIB_UNIT_TEST_CASE(SegmentDataWriterTest, testEmpty);
// INDEXLIB_UNIT_TEST_CASE(SegmentDataWriterTest, testOneIndexId);
// INDEXLIB_UNIT_TEST_CASE(SegmentDataWriterTest, testIndexIds);
// INDEXLIB_UNIT_TEST_CASE(SegmentDataWriterTest, testReentrance);

TEST_F(SegmentDataWriterTest, testEmpty)
{
    auto testDir = _dir->MakeDirectory("SegmentDataWriterTest");

    SegmentDataWriter segmentDataWriter;
    ASSERT_TRUE(segmentDataWriter.Init(testDir));

    index_id_t id = 0;
    auto indexDataWriter = segmentDataWriter.GetIndexDataWriter(id);
    ASSERT_TRUE(indexDataWriter);
    ASSERT_FALSE(segmentDataWriter.GetIndexDataWriter(id));
    ASSERT_FALSE(segmentDataWriter.Close());
    indexDataWriter->Close();
    ASSERT_TRUE(segmentDataWriter.Close());

    auto metaReader = testDir->CreateFileReader(INDEX_ADDR_FILE, FSOT_MMAP);
    string metaContent((char*)metaReader->GetBaseAddress(), metaReader->GetLength());
    IndexDataAddrHolder packIndexAddrHolder;
    autil::legacy::FromJsonString(packIndexAddrHolder, metaContent);

    auto indexDataMap = packIndexAddrHolder.TEST_GetIndexDataMap();
    ASSERT_NE(indexDataMap.end(), indexDataMap.find(id));
    ASSERT_EQ(0, indexDataMap[id].offset);
    ASSERT_EQ(0, indexDataMap[id].length);
}

TEST_F(SegmentDataWriterTest, testOneIndexId)
{
    auto testDir = _dir->MakeDirectory("SegmentDataWriterTest");

    SegmentDataWriter segmentDataWriter;
    ASSERT_TRUE(segmentDataWriter.Init(testDir));

    index_id_t id0 = 0;
    auto indexDataWriter0 = segmentDataWriter.GetIndexDataWriter(id0);
    ASSERT_TRUE(indexDataWriter0);

    string content0 = "0123456789";
    indexDataWriter0->Write(content0.data(), content0.size());
    ASSERT_EQ(10, indexDataWriter0->GetLength());
    ASSERT_FALSE(segmentDataWriter.Close());
    indexDataWriter0->Close();

    ASSERT_FALSE(segmentDataWriter.GetIndexDataWriter(id0));
    ASSERT_TRUE(segmentDataWriter.Close());

    auto metaReader = testDir->CreateFileReader(INDEX_ADDR_FILE, FSOT_MMAP);
    string metaContent((char*)metaReader->GetBaseAddress(), metaReader->GetLength());
    IndexDataAddrHolder packIndexAddrHolder;
    autil::legacy::FromJsonString(packIndexAddrHolder, metaContent);

    ASSERT_NE(packIndexAddrHolder.TEST_GetIndexDataMap().end(), packIndexAddrHolder.TEST_GetIndexDataMap().find(id0));
    ASSERT_EQ(0, packIndexAddrHolder.TEST_GetIndexDataMap()[id0].offset);
    ASSERT_EQ(10, packIndexAddrHolder.TEST_GetIndexDataMap()[id0].length);

    auto fileReader = testDir->CreateFileReader(INDEX_FILE, FSOT_MMAP);
    string fileContent((char*)fileReader->GetBaseAddress(), fileReader->GetLength());
    ASSERT_EQ(10, fileContent.size());
    for (size_t i = 0; i < 10; ++i) {
        ASSERT_EQ(content0[i], fileContent[i]) << i;
    }
}

TEST_F(SegmentDataWriterTest, testIndexIds)
{
    auto testDir = _dir->MakeDirectory("SegmentDataWriterTest");

    SegmentDataWriter segmentDataWriter;
    ASSERT_TRUE(segmentDataWriter.Init(testDir));

    index_id_t id0 = 0;
    auto indexDataWriter0 = segmentDataWriter.GetIndexDataWriter(id0);
    ASSERT_TRUE(indexDataWriter0);

    string content0 = "0123456789";
    indexDataWriter0->Write(content0.data(), content0.size());
    ASSERT_EQ(10, indexDataWriter0->GetLength());
    ASSERT_FALSE(segmentDataWriter.Close());
    indexDataWriter0->Close();

    ASSERT_FALSE(segmentDataWriter.GetIndexDataWriter(id0));

    index_id_t id1 = 1;
    auto indexDataWriter1 = segmentDataWriter.GetIndexDataWriter(id1);
    ASSERT_TRUE(indexDataWriter1);

    string content1 = "abcdefgh";
    indexDataWriter1->Write(content1.data(), content1.size());
    ASSERT_EQ(8, indexDataWriter1->GetLength());
    ASSERT_FALSE(segmentDataWriter.Close());
    indexDataWriter1->Close();
    ASSERT_TRUE(segmentDataWriter.Close());

    auto metaReader = testDir->CreateFileReader(INDEX_ADDR_FILE, FSOT_MMAP);
    string metaContent((char*)metaReader->GetBaseAddress(), metaReader->GetLength());
    IndexDataAddrHolder packIndexAddrHolder;
    autil::legacy::FromJsonString(packIndexAddrHolder, metaContent);

    auto indexDataMap = packIndexAddrHolder.TEST_GetIndexDataMap();
    ASSERT_NE(indexDataMap.end(), indexDataMap.find(id0));
    ASSERT_NE(indexDataMap.end(), indexDataMap.find(id1));
    ASSERT_EQ(0, indexDataMap[id0].offset);
    ASSERT_EQ(10, indexDataMap[id0].length);
    ASSERT_EQ(10, indexDataMap[id1].offset);
    ASSERT_EQ(8, indexDataMap[id1].length);

    auto fileReader = testDir->CreateFileReader(INDEX_FILE, FSOT_MMAP);
    string fileContent((char*)fileReader->GetBaseAddress(), fileReader->GetLength());
    ASSERT_EQ(18, fileContent.size());
    for (size_t i = 0; i < 10; ++i) {
        ASSERT_EQ(content0[i], fileContent[i]) << i;
    }
    for (size_t i = 0; i < 8; ++i) {
        ASSERT_EQ(content1[i], fileContent[i + 10]) << i;
    }
}

TEST_F(SegmentDataWriterTest, testReentrance)
{
    auto testDir = _dir->MakeDirectory("SegmentDataWriterTest");
    SegmentDataWriter segmentDataWriter;
    ASSERT_TRUE(segmentDataWriter.Init(testDir));
    segmentDataWriter.Close();
}
} // namespace indexlibv2::index::ann
