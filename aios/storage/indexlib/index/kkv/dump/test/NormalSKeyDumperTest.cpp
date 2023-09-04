#include "indexlib/index/kkv/dump/NormalSKeyDumper.h"

#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/FileSystemCreator.h"
#include "indexlib/file_system/file/FileReader.h"
#include "indexlib/index/kkv/dump/KKVValueDumper.h"
#include "unittest/unittest.h"

using namespace std;
using namespace autil;
using namespace indexlib::file_system;

namespace indexlibv2::index {

class NormalSKeyDumperTest : public TESTBASE
{
public:
    void setUp() override
    {
        auto testRoot = GET_TEMP_DATA_PATH();
        auto fs = indexlib::file_system::FileSystemCreator::Create("test", testRoot).GetOrThrow();
        auto rootDiretory = indexlib::file_system::Directory::Get(fs);
        _directory = rootDiretory->MakeDirectory("NormalSKeyDumperTest", indexlib::file_system::DirectoryOption())
                         ->GetIDirectory();
        ASSERT_NE(nullptr, _directory);

        _valueDumper = std::make_unique<KKVValueDumper>(-1);
        auto writerOption = indexlib::file_system::WriterOption::Buffer();
        ASSERT_TRUE(_valueDumper->Init(_directory, writerOption).IsOK());

        bool storeTs = true;
        bool storeExpireTime = false;
        _skeyDumper = std::make_unique<NormalSKeyDumper<int64_t>>(storeTs, storeExpireTime);
        ASSERT_TRUE(_skeyDumper->Init(_directory, writerOption).IsOK());
    }

    void tearDown() override
    {
        ASSERT_TRUE(_valueDumper->Close().IsOK());
        ASSERT_TRUE(_skeyDumper->Close().IsOK());
    }

protected:
    void BuildDoc(bool isDeletedPkey, bool isDeletedSkey, bool isLastNode, uint64_t skey, uint32_t ts,
                  const std::string& value, bool forceFlush, ChunkItemOffset expectedSkeyOffset);
    void CheckSkeyFile(const string& skeyFileContent);

protected:
    std::shared_ptr<indexlib::file_system::IDirectory> _directory;
    std::unique_ptr<KKVValueDumper> _valueDumper;
    std::unique_ptr<NormalSKeyDumper<int64_t>> _skeyDumper;
};

TEST_F(NormalSKeyDumperTest, testLegacyUT)
{
    size_t skeyItemSize = sizeof(NormalOnDiskSKeyNode<uint64_t>) + 4;

    auto expectedSkeyOffset = ChunkItemOffset::Of(0, 0);
    // isDeletedPkey,isDeletedSkey,isLastNode,skey,ts,value,forceFlush
    BuildDoc(true, false, false, 0, 0, "", false, expectedSkeyOffset);

    expectedSkeyOffset.inChunkOffset += skeyItemSize;
    BuildDoc(false, false, true, 11, 1, "abc", false, expectedSkeyOffset);

    expectedSkeyOffset.inChunkOffset += skeyItemSize;
    BuildDoc(false, true, true, 21, 2, "", false, expectedSkeyOffset);

    expectedSkeyOffset.inChunkOffset += skeyItemSize;
    BuildDoc(false, false, false, 31, 3, "efg", true, expectedSkeyOffset);

    expectedSkeyOffset.inChunkOffset += skeyItemSize;
    BuildDoc(false, false, true, 32, 4, "higk", false, expectedSkeyOffset);

    // NODE_DELETE_PKEY_OFFSET = 34359738365
    // NODE_DELETE_SKEY_OFFSET = 34359738366

    // isLastNode,skey,ts,chunkOffset,inChunkOffset
    string expectedSkeyFileData = "false,0,0,34359738365,0;"
                                  "true,11,1,0,0;"
                                  "true,21,2,34359738366,0;"
                                  "false,31,3,0,4;"
                                  "true,32,4,2,0";
    CheckSkeyFile(expectedSkeyFileData);
}

void NormalSKeyDumperTest::BuildDoc(bool isDeletedPkey, bool isDeletedSkey, bool isLastNode, uint64_t skey, uint32_t ts,
                                    const std::string& value, bool forceFlush, ChunkItemOffset expectedSkeyOffset)
{
    if (isDeletedPkey || isDeletedSkey) {
        auto [status, skeyOffset] = _skeyDumper->Dump(isDeletedPkey, isDeletedSkey, isLastNode, skey, ts,
                                                      indexlib::UNINITIALIZED_EXPIRE_TIME, ChunkItemOffset::Invalid());
        ASSERT_TRUE(status.IsOK());
        ASSERT_EQ(skeyOffset.inChunkOffset, expectedSkeyOffset.inChunkOffset);
        ASSERT_EQ(skeyOffset.chunkOffset, expectedSkeyOffset.chunkOffset);
        return;
    }

    auto [status, valueOffset] = _valueDumper->Dump(value);
    if (forceFlush) {
        bool ret = _valueDumper->_chunkWriter->TEST_ForceFlush();
        ASSERT_TRUE(ret);
    }
    ASSERT_TRUE(status.IsOK());
    auto [status1, skeyOffset] = _skeyDumper->Dump(isDeletedPkey, isDeletedSkey, isLastNode, skey, ts,
                                                   indexlib::UNINITIALIZED_EXPIRE_TIME, valueOffset);
    ASSERT_TRUE(status1.IsOK());
    ASSERT_EQ(skeyOffset.inChunkOffset, expectedSkeyOffset.inChunkOffset);
    ASSERT_EQ(skeyOffset.chunkOffset, expectedSkeyOffset.chunkOffset);
}

void NormalSKeyDumperTest::CheckSkeyFile(const string& expectedSkeyFileData)
{
    ASSERT_TRUE(_skeyDumper->Close().IsOK());

    ReaderOption readerOption(FSOT_MMAP);
    readerOption.isSwapMmapFile = false;
    auto [status, fileReader] = _directory->CreateFileReader("skey", readerOption).StatusWith();
    ASSERT_TRUE(status.IsOK());

    ChunkMeta chunkMeta;
    ASSERT_TRUE(fileReader->Read(&(chunkMeta), sizeof(chunkMeta)).OK());

    vector<vector<string>> expectedSkeyInfos;
    // isLastNode,skey,ts,chunkOffset,inChunkOffset
    StringUtil::fromString(expectedSkeyFileData, expectedSkeyInfos, ",", ";");
    ASSERT_TRUE(expectedSkeyInfos.size() > 0);

    NormalOnDiskSKeyNode<uint64_t> skeyNode;
    for (size_t i = 0; i < expectedSkeyInfos.size(); i++) {
        const auto& expectedSkeyInfo = expectedSkeyInfos[i];
        ASSERT_EQ(5ul, expectedSkeyInfo.size());

        ASSERT_TRUE(fileReader->Read(&(skeyNode), sizeof(skeyNode)).OK());
        ASSERT_EQ(expectedSkeyInfo[0], skeyNode.IsLastNode() ? string("true") : string("false"));
        ASSERT_EQ(StringUtil::fromString<uint64_t>(expectedSkeyInfo[1]), skeyNode.skey);
        int32_t ts = 0;
        ASSERT_TRUE(fileReader->Read(&(ts), sizeof(ts)).OK());
        ASSERT_EQ(StringUtil::fromString<uint32_t>(expectedSkeyInfo[2]), ts);
        ASSERT_EQ(StringUtil::fromString<uint64_t>(expectedSkeyInfo[3]), skeyNode.meta.chunkOffset);
        ASSERT_EQ(StringUtil::fromString<uint64_t>(expectedSkeyInfo[4]), skeyNode.meta.inChunkOffset);
    }
}

} // namespace indexlibv2::index
