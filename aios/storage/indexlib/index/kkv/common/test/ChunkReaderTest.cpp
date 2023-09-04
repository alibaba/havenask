#include "indexlib/index/kkv/common/ChunkReader.h"

#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/FileSystemCreator.h"
#include "indexlib/file_system/IDirectory.h"
#include "indexlib/file_system/ReaderOption.h"
#include "indexlib/file_system/file/FileReader.h"
#include "indexlib/index/kkv/common/ChunkWriter.h"
#include "unittest/unittest.h"

using namespace std;
using namespace autil;
using namespace indexlib::file_system;

namespace indexlibv2::index {

class ChunkReaderTest : public TESTBASE
{
public:
    void setUp() override
    {
        auto testRoot = GET_TEMP_DATA_PATH();
        auto fs = FileSystemCreator::Create("test", testRoot).GetOrThrow();
        auto rootDiretory = Directory::Get(fs);
        _directory = rootDiretory->MakeDirectory("ChunkReaderTest", DirectoryOption())->GetIDirectory();
        ASSERT_NE(nullptr, _directory);
    }

    void tearDown() override {}

protected:
    void DoTestChunkRead(uint32_t chunkOffsetAlignBit);

protected:
    std::shared_ptr<IDirectory> _directory;
};

TEST_F(ChunkReaderTest, TestCalculateNextChunkOffset)
{
    {
        uint32_t chunkOffsetAlignBit = 0;
        ChunkReader reader(ReadOption::LowLatency(), chunkOffsetAlignBit, true, nullptr);
        uint32_t chunkDataLen = 11;
        uint64_t chunkOffset = 0;
        ASSERT_EQ(chunkOffset + chunkDataLen + sizeof(ChunkMeta),
                  reader.CalcNextChunkOffset(chunkOffset, chunkDataLen));
    }
    {
        uint32_t chunkOffsetAlignBit = 0;
        ChunkReader reader(ReadOption::LowLatency(), chunkOffsetAlignBit, true, nullptr);
        uint32_t chunkDataLen = 11;
        uint64_t chunkOffset = 1ul << 32;
        ASSERT_EQ(chunkOffset + chunkDataLen + sizeof(ChunkMeta),
                  reader.CalcNextChunkOffset(chunkOffset, chunkDataLen));
    }
    {
        uint32_t chunkOffsetAlignBit = 3;
        ChunkReader reader(ReadOption::LowLatency(), chunkOffsetAlignBit, true, nullptr);
        uint32_t chunkDataLen = 11;
        uint64_t chunkOffset = 1ul << 32;
        uint32_t paddingSize = 1;
        ASSERT_EQ(chunkOffset + chunkDataLen + sizeof(ChunkMeta) + paddingSize,
                  reader.CalcNextChunkOffset(chunkOffset, chunkDataLen));
    }
    {
        uint32_t chunkOffsetAlignBit = 3;
        ChunkReader reader(ReadOption::LowLatency(), chunkOffsetAlignBit, true, nullptr);
        uint32_t chunkDataLen = 12;
        uint64_t chunkOffset = 1ul << 32;
        uint32_t paddingSize = 0;
        ASSERT_EQ(chunkOffset + chunkDataLen + sizeof(ChunkMeta) + paddingSize,
                  reader.CalcNextChunkOffset(chunkOffset, chunkDataLen));
    }
}

void ChunkReaderTest::DoTestChunkRead(uint32_t chunkOffsetAlignBit)
{
    string fileName = "file";
    ChunkWriter chunkWriter(chunkOffsetAlignBit);
    auto writerOption = WriterOption::Buffer();
    ASSERT_TRUE(chunkWriter.Open(_directory, writerOption, fileName).IsOK());

    size_t len0 = 4096;
    string expectChunkData0(len0, '0');
    chunkWriter.InsertItem(expectChunkData0);

    size_t len1 = 1024;
    string expectChunkData1(len1, '1');
    chunkWriter.InsertItem(expectChunkData1);

    ASSERT_TRUE(chunkWriter.Close().IsOK());

    ReaderOption readerOption(FSOT_MMAP);
    readerOption.isSwapMmapFile = false;
    auto [status, fileReader] = _directory->CreateFileReader(fileName, readerOption).StatusWith();
    ASSERT_TRUE(status.IsOK());

    autil::mem_pool::Pool pool;
    ChunkReader chunkReader(ReadOption::LowLatency(), chunkOffsetAlignBit, true, &pool);
    chunkReader.Init(fileReader.get(), false);

    auto fsResult = future_lite::interface::syncAwait(chunkReader.Prefetch(0, len0 + len1));
    ASSERT_TRUE(fsResult.IsOK());

    size_t chunkOffset0 = 0;
    auto chunkData0 = future_lite::interface::syncAwait(chunkReader.Read(chunkOffset0));
    ASSERT_NE(nullptr, chunkData0.data);
    ASSERT_EQ(4096, chunkData0.length);
    ASSERT_EQ(expectChunkData0, string(chunkData0.data, chunkData0.length));

    size_t chunkOffset1 = chunkReader.CalcNextChunkOffset(chunkOffset0, chunkData0.length);
    auto chunkData1 = future_lite::interface::syncAwait(chunkReader.Read(chunkOffset1));
    ASSERT_NE(nullptr, chunkData1.data);
    ASSERT_EQ(1024, chunkData1.length);
    ASSERT_EQ(expectChunkData1, string(chunkData1.data, chunkData1.length));

    ASSERT_EQ(2, chunkReader.GetReadCount());
}

TEST_F(ChunkReaderTest, TestNotAlign) { DoTestChunkRead(0); }

TEST_F(ChunkReaderTest, TestAlign) { DoTestChunkRead(3); }
// TODO(richard.sy)
TEST_F(ChunkReaderTest, TestFileDirectIoRead) {}
// TODO(richard.sy)
TEST_F(ChunkReaderTest, TestFileBufferRead) {}
// TODO(richard.sy)
TEST_F(ChunkReaderTest, TestFileReadException) {}
} // namespace indexlibv2::index
