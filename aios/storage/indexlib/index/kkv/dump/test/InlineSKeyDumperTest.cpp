#include "indexlib/index/kkv/dump/InlineSKeyDumper.h"

#include "autil/MultiValueType.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/FileSystemCreator.h"
#include "indexlib/file_system/file/FileReader.h"
#include "indexlib/index/kkv/common/Trait.h"
#include "unittest/unittest.h"

using namespace std;
using namespace autil;

namespace indexlibv2::index {

class InlineSKeyDumperTest : public TESTBASE
{
public:
    void setUp() override
    {
        auto testRoot = GET_TEMP_DATA_PATH();
        auto fs = indexlib::file_system::FileSystemCreator::Create("test", testRoot).GetOrThrow();
        auto rootDiretory = indexlib::file_system::Directory::Get(fs);
        _dumpDirectory = rootDiretory->MakeDirectory("InlineSKeyDumperTest", indexlib::file_system::DirectoryOption())
                             ->GetIDirectory();
        ASSERT_NE(nullptr, _dumpDirectory);
    }

protected:
    void DoTest(bool storeTs, bool storeExpireTime, bool isValueFixedLen);

protected:
    std::shared_ptr<indexlib::file_system::IDirectory> _dumpDirectory;
};

void InlineSKeyDumperTest::DoTest(bool storeTs, bool storeExpireTime, bool isValueFixedLen)
{
    int32_t valueFixedLen = -1;
    if (isValueFixedLen) {
        valueFixedLen = 4;
    }
    string expectedValue;
    if (valueFixedLen > 0) {
        expectedValue = string(valueFixedLen, '1');
    } else {
        expectedValue = "1";
    }

    InlineSKeyDumper<int64_t> dumper(storeTs, storeExpireTime, valueFixedLen);
    auto writerOption = indexlib::file_system::WriterOption::Buffer();
    ASSERT_TRUE(dumper.Init(_dumpDirectory, writerOption).IsOK());

    Status status;
    ChunkItemOffset offset0;
    {
        bool isDeletedPkey = true;
        bool isDeletedSkey = false;
        bool isLastSkey = false;
        int64_t skey = 0;
        uint32_t ts = 0;
        uint32_t expireTime = 0;
        autil::StringView value;
        std::tie(status, offset0) = dumper.Dump(isDeletedPkey, isDeletedSkey, isLastSkey, skey, ts, expireTime, value);
        ASSERT_TRUE(status.IsOK());
    }
    ASSERT_EQ(0, offset0.inChunkOffset);

    ChunkItemOffset offset1;
    {
        bool isDeletedPkey = false;
        bool isDeletedSkey = true;
        bool isLastSkey = false;
        int64_t skey = 1;
        uint32_t ts = 1;
        uint32_t expireTime = 1;
        autil::StringView value;
        std::tie(status, offset1) = dumper.Dump(isDeletedPkey, isDeletedSkey, isLastSkey, skey, ts, expireTime, value);
        ASSERT_TRUE(status.IsOK());
        ASSERT_EQ(dumper.GetMaxSKeyCount(), 0);
        ASSERT_EQ(dumper.GetTotalSKeyCount(), 2);
        ASSERT_EQ(dumper.GetMaxValueLen(), isValueFixedLen ? 4 : 0);
    }
    ChunkItemOffset offset2;
    {
        bool isDeletedPkey = false;
        bool isDeletedSkey = false;
        bool isLastSkey = true;
        int64_t skey = 2;
        uint32_t ts = 2;
        uint32_t expireTime = 2;
        std::tie(status, offset2) =
            dumper.Dump(isDeletedPkey, isDeletedSkey, isLastSkey, skey, ts, expireTime, expectedValue);
        ASSERT_TRUE(status.IsOK());
        ASSERT_EQ(dumper.GetMaxSKeyCount(), 3);
        ASSERT_EQ(dumper.GetTotalSKeyCount(), 3);
        ASSERT_EQ(dumper.GetMaxValueLen(), isValueFixedLen ? 4 : 2);
    }
    ASSERT_TRUE(dumper.Close().IsOK());

    indexlib::file_system::ReaderOption readerOption(indexlib::file_system::FSOT_MMAP);
    readerOption.isSwapMmapFile = false;
    auto [status1, fileReader] = _dumpDirectory->CreateFileReader("skey", readerOption).StatusWith();
    ASSERT_TRUE(status1.IsOK());

    uint64_t inChunkOffset = 0;
    const char* baseAddr = (const char*)fileReader->GetBaseAddress() + sizeof(ChunkMeta);
    using SKeyNode = typename SKeyNodeTraits<int64_t, true>::SKeyNode;
    auto skeyNode = *reinterpret_cast<const SKeyNode*>(baseAddr);
    inChunkOffset += sizeof(SKeyNode);

    ASSERT_FALSE(skeyNode.IsLastNode());
    ASSERT_TRUE(skeyNode.IsPKeyDeleted());

    if (storeTs) {
        const char* tsAddr = baseAddr + inChunkOffset;
        uint32_t ts = 0u;
        ::memcpy(&ts, tsAddr, sizeof(ts));
        ASSERT_EQ(0, ts);
        inChunkOffset += sizeof(uint32_t);
    }

    if (storeExpireTime) {
        const char* expireTimeAddr = baseAddr + inChunkOffset;
        uint32_t expireTime = 0u;
        ::memcpy(&expireTime, expireTimeAddr, sizeof(expireTime));
        ASSERT_EQ(0, expireTime);
        inChunkOffset += sizeof(uint32_t);
    }
    ASSERT_EQ(inChunkOffset, offset1.inChunkOffset);

    skeyNode = *reinterpret_cast<const SKeyNode*>(baseAddr + inChunkOffset);
    inChunkOffset += sizeof(SKeyNode);

    ASSERT_FALSE(skeyNode.IsLastNode());
    ASSERT_FALSE(skeyNode.IsPKeyDeleted());
    ASSERT_EQ(1, skeyNode.skey);
    ASSERT_FALSE(skeyNode.HasValue());

    if (storeTs) {
        const char* tsAddr = baseAddr + inChunkOffset;
        uint32_t ts = 0u;
        ::memcpy(&ts, tsAddr, sizeof(ts));
        ASSERT_EQ(1, ts);
        inChunkOffset += sizeof(uint32_t);
    }

    if (storeExpireTime) {
        const char* expireTimeAddr = baseAddr + inChunkOffset;
        uint32_t expireTime = 0u;
        ::memcpy(&expireTime, expireTimeAddr, sizeof(expireTime));
        ASSERT_EQ(1, expireTime);
        inChunkOffset += sizeof(uint32_t);
    }

    ASSERT_EQ(inChunkOffset, offset2.inChunkOffset);
    skeyNode = *reinterpret_cast<const SKeyNode*>(baseAddr + inChunkOffset);
    inChunkOffset += sizeof(SKeyNode);

    ASSERT_TRUE(skeyNode.IsLastNode());
    ASSERT_FALSE(skeyNode.IsPKeyDeleted());
    ASSERT_EQ(2, skeyNode.skey);
    ASSERT_TRUE(skeyNode.HasValue());

    if (storeTs) {
        const char* tsAddr = baseAddr + inChunkOffset;
        uint32_t ts = 0u;
        ::memcpy(&ts, tsAddr, sizeof(ts));
        ASSERT_EQ(2, ts);
        inChunkOffset += sizeof(uint32_t);
    }

    if (storeExpireTime) {
        const char* expireTimeAddr = baseAddr + inChunkOffset;
        uint32_t expireTime = 0u;
        ::memcpy(&expireTime, expireTimeAddr, sizeof(expireTime));
        ASSERT_EQ(2, expireTime);
        inChunkOffset += sizeof(uint32_t);
    }

    const char* valueAddr = baseAddr + inChunkOffset;
    if (valueFixedLen > 0) {
        auto value = autil::StringView(valueAddr, valueFixedLen);
        ASSERT_EQ(autil::StringView(expectedValue.data(), expectedValue.size()), value);
    } else {
        autil::MultiChar mc;
        mc.init(valueAddr);
        auto value = autil::StringView(mc.data(), mc.size());
        ASSERT_EQ(autil::StringView(expectedValue.data(), expectedValue.size()), value);
    }
}

TEST_F(InlineSKeyDumperTest, Test111) { DoTest(true, true, true); }
TEST_F(InlineSKeyDumperTest, Test011) { DoTest(false, true, true); }
TEST_F(InlineSKeyDumperTest, Test001) { DoTest(false, false, true); }
TEST_F(InlineSKeyDumperTest, Test000) { DoTest(false, false, false); }
} // namespace indexlibv2::index
