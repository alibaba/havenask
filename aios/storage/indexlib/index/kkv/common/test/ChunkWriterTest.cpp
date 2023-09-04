#include "indexlib/index/kkv/common/ChunkWriter.h"

#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/FileSystemCreator.h"
#include "indexlib/file_system/IDirectory.h"
#include "indexlib/file_system/ReaderOption.h"
#include "indexlib/file_system/file/FileReader.h"
#include "indexlib/index/kkv/Constant.h"
#include "unittest/unittest.h"

using namespace std;
using namespace autil;
using namespace indexlib::file_system;

namespace indexlibv2::index {

class ChunkWriterTest : public TESTBASE
{
public:
    void setUp() override
    {
        auto testRoot = GET_TEMP_DATA_PATH();
        auto fs = FileSystemCreator::Create("test", testRoot).GetOrThrow();
        auto rootDiretory = Directory::Get(fs);
        _directory = rootDiretory->MakeDirectory("ChunkWriterTest", DirectoryOption())->GetIDirectory();
        ASSERT_NE(nullptr, _directory);
    }

    void DoTestAlignment(size_t alignBit);

    size_t AlignValue(size_t offset, size_t alignBit)
    {
        if ((offset >> alignBit << alignBit) == offset) {
            return offset;
        }
        return (offset + (1 << alignBit)) >> alignBit << alignBit;
    }

protected:
    std::shared_ptr<IDirectory> _directory;
};

TEST_F(ChunkWriterTest, TestNoInsert)
{
    string fileName = "value";
    ChunkWriter chunkWriter(3);
    auto writerOption = WriterOption::Buffer();
    ASSERT_TRUE(chunkWriter.Open(_directory, writerOption, fileName).IsOK());
    ASSERT_TRUE(chunkWriter.Close().IsOK());

    ReaderOption readerOption(FSOT_MMAP);
    readerOption.isSwapMmapFile = false;
    auto [status, fileReader] = _directory->CreateFileReader(fileName, readerOption).StatusWith();
    ASSERT_EQ(0, fileReader->GetLength());
}

TEST_F(ChunkWriterTest, TestNotAlign) { DoTestAlignment(0); }
TEST_F(ChunkWriterTest, TestAlign1Bit) { DoTestAlignment(1); }
TEST_F(ChunkWriterTest, TestAlign2Bit) { DoTestAlignment(2); }
TEST_F(ChunkWriterTest, TestAlign3Bit) { DoTestAlignment(3); }

void ChunkWriterTest::DoTestAlignment(size_t alignBit)
{
    {
        string fileName = "value";
        ChunkWriter chunkWriter(alignBit);
        auto writerOption = WriterOption::Buffer();
        ASSERT_TRUE(chunkWriter.Open(_directory, writerOption, fileName).IsOK());

        string expectChunkData0;
        size_t len0 = 8;
        {
            string item(len0, '0');
            ChunkItemOffset offset = chunkWriter.InsertItem(item);
            ASSERT_TRUE(offset.IsValidOffset());
            ASSERT_EQ(0, offset.chunkOffset);
            ASSERT_EQ(0, offset.inChunkOffset);
            expectChunkData0 += item;
        }

        size_t len1 = 10;
        {
            size_t len1_0 = 7;
            ASSERT_TRUE(len1_0 < len1);
            string item(len1_0, '1');
            ChunkItemOffset offset = chunkWriter.InsertItem(item);
            ASSERT_TRUE(offset.IsValidOffset());
            ASSERT_EQ(0, offset.chunkOffset);
            ASSERT_EQ(len0, offset.inChunkOffset);
            expectChunkData0 += item;

            string extra(len1 - len1_0, '2');
            chunkWriter.AppendData(extra);
            expectChunkData0 += extra;
        }

        size_t len2 = 4096;
        {
            string item(len2, '3');
            ChunkItemOffset offset = chunkWriter.InsertItem(item);
            ASSERT_TRUE(offset.IsValidOffset());
            ASSERT_EQ(0, offset.chunkOffset);
            ASSERT_EQ(len0 + len1, offset.inChunkOffset);
            expectChunkData0 += item;
        }

        size_t len3 = 10;
        size_t expectChunkOffset1 = AlignValue(sizeof(ChunkMeta) + len0 + len1 + len2, alignBit);
        string expectChunkData1;
        {
            string item(len3, '4');
            // 触发chunk flush
            ChunkItemOffset offset = chunkWriter.InsertItem(item);
            ASSERT_TRUE(offset.IsValidOffset());

            ASSERT_EQ(expectChunkOffset1, offset.chunkOffset);
            ASSERT_EQ(0, offset.inChunkOffset);
            expectChunkData1 += item;
        }

        // 触发chunk flush
        ASSERT_TRUE(chunkWriter.Close().IsOK());

        ReaderOption readerOption(FSOT_MMAP);
        readerOption.isSwapMmapFile = false;
        auto [status, fileReader] = _directory->CreateFileReader(fileName, readerOption).StatusWith();
        ASSERT_TRUE(status.IsOK());
        const char* data = reinterpret_cast<const char*>(fileReader->GetBaseAddress());
        ASSERT_TRUE(data != nullptr);

        size_t fileLength = fileReader->GetLength();
        size_t expectFileLength = expectChunkOffset1 + sizeof(ChunkMeta) + len3;
        ASSERT_EQ(expectFileLength, fileLength);

        ChunkMeta chunkMeta0;
        ASSERT_TRUE(fileReader->Read(&chunkMeta0, sizeof(chunkMeta0)).OK());
        ASSERT_EQ(len0 + len1 + len2, chunkMeta0.length);
        string chunkData0(chunkMeta0.length, 0);
        ASSERT_TRUE(fileReader->Read(chunkData0.data(), chunkMeta0.length).OK());
        ASSERT_EQ(expectChunkData0, chunkData0);

        string padding0(expectChunkOffset1 - chunkMeta0.length - sizeof(chunkMeta0), '-');
        ASSERT_TRUE(fileReader->Read(padding0.data(), padding0.size()).OK());
        ASSERT_EQ(string(padding0.size(), 0), padding0);

        ChunkMeta chunkMeta1;
        ASSERT_TRUE(fileReader->Read(&chunkMeta1, sizeof(chunkMeta1)).OK());
        ASSERT_EQ(len3, chunkMeta1.length);
        string chunkData1(chunkMeta1.length, 0);
        ASSERT_TRUE(fileReader->Read(chunkData1.data(), chunkData1.size()).OK());
        ASSERT_EQ(expectChunkData1, chunkData1);
    }
}
} // namespace indexlibv2::index
