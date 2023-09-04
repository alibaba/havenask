#include "indexlib/index/kkv/built/KKVBuiltValueReader.h"

#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/FileSystemCreator.h"
#include "indexlib/file_system/IDirectory.h"
#include "indexlib/file_system/ReaderOption.h"
#include "indexlib/index/kkv/common/ChunkWriter.h"
#include "unittest/unittest.h"

using namespace std;
using namespace autil;
using namespace autil::mem_pool;
using namespace indexlib::util;
using namespace indexlib::file_system;

namespace indexlibv2::index {

class KKVBuiltValueReaderTest : public TESTBASE
{
public:
    void setUp() override
    {
        auto testRoot = GET_TEMP_DATA_PATH();
        auto fs = FileSystemCreator::Create("test", testRoot).GetOrThrow();
        auto rootDiretory = Directory::Get(fs);
        _directory = rootDiretory->MakeDirectory("KKVBuiltValueReaderTest", DirectoryOption())->GetIDirectory();
        ASSERT_NE(nullptr, _directory);
    }

    void tearDown() override {}

protected:
    autil::mem_pool::Pool _pool;
    std::shared_ptr<indexlib::file_system::IDirectory> _directory;
};

TEST_F(KKVBuiltValueReaderTest, TestFixedLen)
{
    int32_t chunkOffsetAlignBit = 3;
    string fileName = "file";
    ChunkWriter chunkWriter(chunkOffsetAlignBit);
    auto writerOption = WriterOption::Buffer();
    ASSERT_TRUE(chunkWriter.Open(_directory, writerOption, fileName).IsOK());

    size_t fixedLen = 4096;
    string expectChunkData0(fixedLen, '0');
    auto chunkItemOffset0 = chunkWriter.InsertItem(expectChunkData0);
    string expectChunkData1(fixedLen, '1');
    auto chunkItemOffset1 = chunkWriter.InsertItem(expectChunkData1);
    ASSERT_TRUE(chunkWriter.Close().IsOK());

    ReaderOption readerOption(FSOT_MMAP);
    readerOption.isSwapMmapFile = false;
    auto [status, fileReader] = _directory->CreateFileReader(fileName, readerOption).StatusWith();
    ASSERT_TRUE(status.IsOK());

    KKVBuiltValueReader valueReader(fixedLen, true, &_pool, indexlib::file_system::ReadOption::LowLatency());
    valueReader.Init(fileReader.get(), false);

    ASSERT_TRUE(valueReader.NeedSwitchChunk(chunkItemOffset0.chunkOffset));
    auto strView0 = valueReader.Read(chunkItemOffset0);
    ASSERT_EQ(strView0, expectChunkData0);

    ASSERT_TRUE(valueReader.NeedSwitchChunk(chunkItemOffset1.chunkOffset));
    auto strView1 = valueReader.Read(chunkItemOffset1);
    ASSERT_EQ(strView1, expectChunkData1);
}

TEST_F(KKVBuiltValueReaderTest, TestVarLen)
{
    return;
    int32_t chunkOffsetAlignBit = 3;
    string fileName = "file";
    ChunkWriter chunkWriter(chunkOffsetAlignBit);
    auto writerOption = WriterOption::Buffer();
    ASSERT_TRUE(chunkWriter.Open(_directory, writerOption, fileName).IsOK());

    size_t len0 = 4096;
    string expectChunkData0(len0, '0');
    char head0[4];
    size_t headSize0 = autil::MultiValueFormatter::encodeCount(len0, head0, 4);
    auto chunkItemOffset0 = chunkWriter.InsertItem({head0, headSize0});
    chunkWriter.AppendData(expectChunkData0);

    size_t len1 = 1024;
    string expectChunkData1(len1, '1');
    char head1[4];
    size_t headSize1 = autil::MultiValueFormatter::encodeCount(len1, head1, 4);
    auto chunkItemOffset1 = chunkWriter.InsertItem({head1, headSize1});
    chunkWriter.AppendData(expectChunkData1);
    ASSERT_TRUE(chunkWriter.Close().IsOK());

    ReaderOption readerOption(FSOT_MMAP);
    readerOption.isSwapMmapFile = false;
    auto [status, fileReader] = _directory->CreateFileReader(fileName, readerOption).StatusWith();
    ASSERT_TRUE(status.IsOK());

    KKVBuiltValueReader valueReader(-1, true, &_pool, indexlib::file_system::ReadOption::LowLatency());
    valueReader.Init(fileReader.get(), false);

    ASSERT_TRUE(valueReader.NeedSwitchChunk(chunkItemOffset0.chunkOffset));
    auto strView0 = valueReader.Read(chunkItemOffset0);
    ASSERT_EQ(strView0, expectChunkData0);

    ASSERT_TRUE(valueReader.NeedSwitchChunk(chunkItemOffset1.chunkOffset));
    auto strView1 = valueReader.Read(chunkItemOffset1);
    ASSERT_EQ(strView1, expectChunkData1);
}

} // namespace indexlibv2::index
