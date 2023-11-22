#include "indexlib/index/ann/aitheta2/util/CustomizedAithetaContainer.h"

#include "indexlib/file_system/FileSystemCreator.h"
#include "indexlib/file_system/file/FileReader.h"
#include "indexlib/file_system/file/FileWriter.h"
#include "indexlib/index/ann/aitheta2/util/BufferedAithetaSegment.h"
#include "indexlib/index/ann/aitheta2/util/InMemoryAithetaSegment.h"
#include "indexlib/util/testutil/unittest.h"

using namespace indexlib::file_system;
using namespace std;
using namespace aitheta2;

namespace indexlibv2::index::ann {

class AiThetaSegmentTest : public TESTBASE
{
public:
    AiThetaSegmentTest() {}
    ~AiThetaSegmentTest() = default;

public:
    void setUp() override
    {
        auto testRoot = GET_TEMP_DATA_PATH();
        _fs = indexlib::file_system::FileSystemCreator::Create("SegmentDataReaderTest", testRoot).GetOrThrow();
        _dir = indexlib::file_system::Directory::Get(_fs);
    }

    void TestFetch();

private:
    std::shared_ptr<IFileSystem> _fs;
    std::shared_ptr<Directory> _dir;
    AUTIL_LOG_DECLARE();
};

TEST_F(AiThetaSegmentTest, TestInMemoryAiThetaSegment)
{
    size_t data_size = 12;
    size_t padding_size = 4;
    uint32_t data_crc = 1;
    size_t region_size = data_size + padding_size;
    std::shared_ptr<char> data(new (std::nothrow) char[region_size], [](char* p) { delete[] p; });
    for (size_t i = 0; i < region_size; ++i) {
        data.get()[i] = i;
    }

    auto segment = std::make_shared<aitheta2::InMemoryAiThetaSegment>(data, data_size, padding_size, data_crc);

    EXPECT_EQ(12, segment->data_size());
    EXPECT_EQ(4, segment->padding_size());
    EXPECT_EQ(1, segment->data_crc());
    const void* actualData {nullptr};
    EXPECT_EQ(region_size, segment->read(0, &actualData, region_size));
    for (size_t i = 0; i < region_size; ++i) {
        EXPECT_EQ(((const int8_t*)actualData)[i], data.get()[i]);
    }
}

TEST_F(AiThetaSegmentTest, TestBufferedAiThetaSegment)
{
    size_t data_size = 12;
    size_t padding_size = 4;
    uint32_t data_crc = 1;
    size_t region_size = data_size + padding_size;
    std::shared_ptr<char> data(new (std::nothrow) char[region_size], [](char* p) { delete[] p; });
    for (size_t i = 0; i < region_size; ++i) {
        data.get()[i] = i;
    }

    auto testDir = _dir->MakeDirectory("CustomizedAiThetaContainerTest_TestGetBufferedAiThetaSegment");
    auto fileWriter = testDir->CreateFileWriter(INDEX_ADDR_FILE);
    fileWriter->Write(data.get(), region_size).GetOrThrow();
    fileWriter->Close().GetOrThrow();

    indexlib::file_system::ReaderOption readerOption(FSOT_BUFFERED);
    auto fileReader = testDir->CreateFileReader(INDEX_ADDR_FILE, readerOption);

    IndexDataAddr indexDataAddr;
    indexDataAddr.length = 16;
    auto indexDataReader = std::make_shared<IndexDataReader>(fileReader, indexDataAddr);

    aitheta2::IndexUnpacker::SegmentMeta meta(0, data_size, padding_size, data_crc);

    auto segment = std::make_shared<aitheta2::BufferedAiThetaSegment>(indexDataReader, meta);

    EXPECT_EQ(12, segment->data_size());
    EXPECT_EQ(4, segment->padding_size());
    EXPECT_EQ(1, segment->data_crc());
    const void* actualData {nullptr};
    EXPECT_EQ(region_size, segment->read(0, &actualData, region_size));
    for (size_t i = 0; i < region_size; ++i) {
        EXPECT_EQ(((const int8_t*)actualData)[i], data.get()[i]);
    }
    std::vector<char> actualData1(region_size);
    EXPECT_EQ(region_size, segment->fetch(0, actualData1.data(), region_size));
    for (size_t i = 0; i < region_size; ++i) {
        EXPECT_EQ(actualData1[i], data.get()[i]);
    }
}

AUTIL_LOG_SETUP(indexlib.index, AiThetaSegmentTest);
} // namespace indexlibv2::index::ann
