#include "indexlib/index/ann/aitheta2/util/segment_data/SegmentDataReader.h"

#include "indexlib/file_system/FileSystemCreator.h"
#include "indexlib/file_system/file/FileWriter.h"
#include "unittest/unittest.h"

using namespace autil::legacy;
using namespace std;
using namespace indexlib::file_system;

namespace indexlibv2::index::ann {

class SegmentDataReaderTest : public TESTBASE
{
public:
    SegmentDataReaderTest() = default;
    ~SegmentDataReaderTest() = default;

public:
    void setUp() override
    {
        auto testRoot = GET_TEMP_DATA_PATH();
        _fs = indexlib::file_system::FileSystemCreator::Create("SegmentDataReaderTest", testRoot).GetOrThrow();
        _dir = indexlib::file_system::Directory::Get(_fs);
    }

public:
    void testEmpty();
    void testOneIndexId();
    void testIndexIds();

private:
    std::shared_ptr<IFileSystem> _fs;
    std::shared_ptr<Directory> _dir;
    AUTIL_LOG_DECLARE();
};

AUTIL_LOG_SETUP(indexlib.index, SegmentDataReaderTest);

// INDEXLIB_UNIT_TEST_CASE(SegmentDataReaderTest, testEmpty);
// INDEXLIB_UNIT_TEST_CASE(SegmentDataReaderTest, testOneIndexId);
// INDEXLIB_UNIT_TEST_CASE(SegmentDataReaderTest, testIndexIds);

TEST_F(SegmentDataReaderTest, testEmpty)
{
    IndexDataAddrHolder packIndexMeta;
    string metaContent = ToJsonString(packIndexMeta);
    auto testDir = _dir->MakeDirectory("SegmentDataReaderTest_testEmpty");
    auto packFileMetaWriter = testDir->CreateFileWriter(INDEX_ADDR_FILE);
    packFileMetaWriter->Write(metaContent.data(), metaContent.size()).GetOrThrow();
    packFileMetaWriter->Close().GetOrThrow();
    auto packFileWriter = testDir->CreateFileWriter(INDEX_FILE);
    packFileWriter->Close().GetOrThrow();

    SegmentDataReader segmentDataReader;
    ASSERT_TRUE(segmentDataReader.Init(testDir));
    auto indexDataMap = segmentDataReader.TEST_GetIndexDataAddrHolder().TEST_GetIndexDataMap();
    ASSERT_TRUE(indexDataMap.empty());
}

TEST_F(SegmentDataReaderTest, testOneIndexId)
{
    IndexDataAddrHolder packIndexMeta;
    IndexDataAddr meta;
    meta.offset = 0;
    meta.length = 8;
    index_id_t id = 0;
    packIndexMeta.TEST_GetIndexDataMap()[id] = meta;

    string metaContent = ToJsonString(packIndexMeta);
    auto testDir = _dir->MakeDirectory("SegmentDataReaderTest_testOneIndexId");
    auto packFileMetaWriter = testDir->CreateFileWriter(INDEX_ADDR_FILE);
    packFileMetaWriter->Write(metaContent.data(), metaContent.size()).GetOrThrow();
    packFileMetaWriter->Close().GetOrThrow();
    auto packFileWriter = testDir->CreateFileWriter(INDEX_FILE);

    string dumpContent = "01234567";
    packFileWriter->Write(dumpContent.data(), dumpContent.size()).GetOrThrow();
    packFileWriter->Close().GetOrThrow();

    SegmentDataReader segmentDataReader;
    ASSERT_TRUE(segmentDataReader.Init(testDir));
    auto indexDataMap = segmentDataReader.TEST_GetIndexDataAddrHolder().TEST_GetIndexDataMap();
    ASSERT_EQ(1, indexDataMap.size());
    ASSERT_EQ(0, indexDataMap[id].offset);
    ASSERT_EQ(8, indexDataMap[id].length);

    ASSERT_TRUE(!segmentDataReader.GetIndexDataReader(1));

    auto indexReader = segmentDataReader.GetIndexDataReader(0);
    ASSERT_TRUE(indexReader);
    ASSERT_EQ(8, indexReader->GetLength());

    void* buf = 0;
    ASSERT_EQ(8, indexReader->Read(&buf, 8, 0));
    for (size_t i = 0; i < 8; ++i) {
        ASSERT_EQ(dumpContent[i], ((char*)buf)[i]) << i;
    }
    ASSERT_EQ(8, indexReader->Read(&buf, 8, 0));
    // length out of range
    ASSERT_EQ(8, indexReader->Read(&buf, 9, 0));
    for (size_t i = 0; i < 8; ++i) {
        ASSERT_EQ(dumpContent[i], ((char*)buf)[i]) << i;
    }
    // offset out of range
    ASSERT_EQ(0, indexReader->Read(&buf, 8, 8));
    // offset & length out of range
    ASSERT_EQ(0, indexReader->Read(&buf, 9, 9));
    // (offset + length) out of range
    ASSERT_EQ(2, indexReader->Read(&buf, 4, 6));
    ASSERT_EQ('6', ((char*)buf)[0]);
    ASSERT_EQ('7', ((char*)buf)[1]);
}

TEST_F(SegmentDataReaderTest, testIndexIds)
{
    IndexDataAddrHolder packIndexMeta;
    IndexDataAddr meta0;
    meta0.offset = 0;
    meta0.length = 8;
    index_id_t id0 = 0;
    packIndexMeta.TEST_GetIndexDataMap()[id0] = meta0;

    IndexDataAddr meta1;
    meta1.offset = 8;
    meta1.length = 4;
    index_id_t id1 = 1;
    packIndexMeta.TEST_GetIndexDataMap()[id1] = meta1;

    string metaContent = ToJsonString(packIndexMeta);
    auto testDir = _dir->MakeDirectory("SegmentDataReaderTest_testIndexIds");
    auto packFileMetaWriter = testDir->CreateFileWriter(INDEX_ADDR_FILE);
    packFileMetaWriter->Write(metaContent.data(), metaContent.size()).GetOrThrow();
    packFileMetaWriter->Close().GetOrThrow();
    auto packFileWriter = testDir->CreateFileWriter(INDEX_FILE);

    string dumpContent = "01234567ABCD";
    packFileWriter->Write(dumpContent.data(), dumpContent.size()).GetOrThrow();
    packFileWriter->Close().GetOrThrow();

    SegmentDataReader segmentDataReader;
    ASSERT_TRUE(segmentDataReader.Init(testDir));
    auto indexDataMap = segmentDataReader.TEST_GetIndexDataAddrHolder().TEST_GetIndexDataMap();
    ASSERT_EQ(2, indexDataMap.size());
    ASSERT_EQ(0, indexDataMap[id0].offset);
    ASSERT_EQ(8, indexDataMap[id0].length);
    ASSERT_EQ(8, indexDataMap[id1].offset);
    ASSERT_EQ(4, indexDataMap[id1].length);

    auto aiThetaFileReader0 = segmentDataReader.GetIndexDataReader(0);
    ASSERT_TRUE(aiThetaFileReader0);
    ASSERT_EQ(8, aiThetaFileReader0->GetLength());

    void* buf0 = 0;
    ASSERT_EQ(8, aiThetaFileReader0->Read(&buf0, 8, 0));
    for (size_t i = 0; i < 8; ++i) {
        ASSERT_EQ(dumpContent[i], ((char*)buf0)[i]) << i;
    }

    // length out of range
    ASSERT_EQ(8, aiThetaFileReader0->Read(&buf0, 9, 0));
    for (size_t i = 0; i < 8; ++i) {
        ASSERT_EQ(dumpContent[i], ((char*)buf0)[i]) << i;
    }
    // offset out of range
    ASSERT_EQ(0, aiThetaFileReader0->Read(&buf0, 8, 8));
    // offset & length out of range
    ASSERT_EQ(0, aiThetaFileReader0->Read(&buf0, 9, 8));
    // (offset + length) out of range
    ASSERT_EQ(2, aiThetaFileReader0->Read(&buf0, 4, 6));
    ASSERT_EQ('6', ((char*)buf0)[0]);
    ASSERT_EQ('7', ((char*)buf0)[1]);

    auto aiThetaFileReader1 = segmentDataReader.GetIndexDataReader(1);
    ASSERT_TRUE(aiThetaFileReader1);
    ASSERT_EQ(4, aiThetaFileReader1->GetLength());

    void* buf1 = 0;
    ASSERT_EQ(4, aiThetaFileReader1->Read(&buf1, 4, 0));
    for (size_t i = 0; i < 4; ++i) {
        ASSERT_EQ(dumpContent[i + 8], ((char*)buf1)[i]) << i;
    }
    // length out of range
    ASSERT_EQ(4, aiThetaFileReader1->Read(&buf1, 5, 0));
    for (size_t i = 0; i < 4; ++i) {
        ASSERT_EQ(dumpContent[i + 8], ((char*)buf1)[i]) << i;
    }
    // offset out of range
    ASSERT_EQ(0, aiThetaFileReader1->Read(&buf1, 4, 4));
    // offset & length out of range
    ASSERT_EQ(0, aiThetaFileReader1->Read(&buf1, 5, 4));
    // (offset + length) out of range
    ASSERT_EQ(2, aiThetaFileReader1->Read(&buf1, 4, 2));
    ASSERT_EQ('C', ((char*)buf1)[0]);
    ASSERT_EQ('D', ((char*)buf1)[1]);
}

} // namespace indexlibv2::index::ann
