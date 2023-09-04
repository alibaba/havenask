
#include "indexlib/index/kkv/dump/test/KKVVDumpTestHelper.h"

#include "autil/MultiValueType.h"

using namespace std;
using namespace autil;

namespace {
struct ValueData : public autil::legacy::Jsonizable {
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override
    {
        json.Jsonize("chunkLength", chunkLength);
        json.Jsonize("chunkItems", chunkItems);
    }
    size_t chunkLength = 0;
    std::vector<std::string> chunkItems;
};

struct PKeyData : public autil::legacy::Jsonizable {
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override
    {
        json.Jsonize("pkey", pkey);
        json.Jsonize("chunkOffset", chunkOffset);
        json.Jsonize("inChunkOffset", inChunkOffset);
        json.Jsonize("blockHint", blockHint);
    }
    uint64_t pkey = 0;
    size_t chunkOffset = 0;
    size_t inChunkOffset = 0;
    size_t blockHint = 0;
};

} // namespace

namespace indexlibv2::index {
AUTIL_LOG_SETUP(indexlib.index, KKVVDumpTestHelper);

void KKVVDumpTestHelper::CheckValue(const std::shared_ptr<indexlib::file_system::IDirectory>& directory,
                                    const std::string& expectedValueFileData)
{
    std::vector<ValueData> expectedValueDatas;
    autil::legacy::FromJsonString(expectedValueDatas, expectedValueFileData);
    ASSERT_FALSE(expectedValueDatas.empty());

    auto fileReader = directory->CreateFileReader(KKV_VALUE_FILE_NAME, indexlib::file_system::FSOT_MEM).GetOrThrow();
    size_t totalLen = 0;
    for (auto& expectedValue : expectedValueDatas) {
        // align to 8 bytes
        totalLen = (totalLen + 7) / 8 * 8;
        indexlibv2::index::ChunkMeta chunkMeta;
        ASSERT_EQ(sizeof(chunkMeta), fileReader->Read(&chunkMeta, sizeof(chunkMeta), totalLen).GetOrThrow());
        totalLen += sizeof(chunkMeta);
        ASSERT_EQ(expectedValue.chunkLength, chunkMeta.length);

        string valueStr;
        valueStr.resize(chunkMeta.length);
        ASSERT_EQ((size_t)chunkMeta.length,
                  fileReader->Read((void*)valueStr.data(), (size_t)chunkMeta.length, totalLen).GetOrThrow());

        size_t cursor = 0;
        auto values = expectedValue.chunkItems;
        for (size_t j = 0; j < expectedValue.chunkItems.size(); j++) {
            MultiChar mc(valueStr.c_str() + cursor);
            ASSERT_EQ(values[j].size(), mc.size()) << j;
            ASSERT_EQ(values[j], string(mc.data(), mc.size())) << j;
            cursor += mc.length();
        }
        totalLen += chunkMeta.length;
    }
    ASSERT_EQ(totalLen, fileReader->GetLength());
}

void KKVVDumpTestHelper::CheckPKey(const std::shared_ptr<indexlib::file_system::IDirectory>& directory,
                                   const indexlib::config::KKVIndexPreference& indexPreference,
                                   const std::string& expectedPkeyFileData)
{
    std::vector<PKeyData> pkeyDatas;
    autil::legacy::FromJsonString(pkeyDatas, expectedPkeyFileData);
    ASSERT_FALSE(pkeyDatas.empty());

    indexlib::file_system::ReaderOption readerOption(indexlib::file_system::FSOT_MMAP);
    readerOption.isSwapMmapFile = false;
    auto [status, fileReader] = directory->CreateFileReader("pkey", readerOption).StatusWith();
    ASSERT_TRUE(status.IsOK());

    auto ptr =
        PrefixKeyTableCreator<OnDiskPKeyOffset>::Create(directory, indexPreference, PKeyTableOpenType::READ, 0, 0);
    ASSERT_NE(nullptr, ptr);

    std::shared_ptr<PrefixKeyTableBase<OnDiskPKeyOffset>> pkeyTable(ptr);
    ASSERT_EQ(pkeyDatas.size(), pkeyTable->Size());
    for (auto pkeyData : pkeyDatas) {
        auto value = pkeyTable->Find(pkeyData.pkey);
        ASSERT_NE(nullptr, value) << pkeyData.pkey;
        ASSERT_EQ(value->blockHint, pkeyData.blockHint) << pkeyData.pkey;
        ASSERT_EQ(value->chunkOffset, pkeyData.chunkOffset) << pkeyData.pkey;
        ASSERT_EQ(value->inChunkOffset, pkeyData.inChunkOffset) << pkeyData.pkey;
    }
}

void KKVVDumpTestHelper::Dump(KKVDataDumperBase& dumper, const std::string& message, bool isLastSkey,
                              autil::mem_pool::Pool& pool)
{
    uint64_t pkey = 0;
    bool isDeletedPkey = false;
    string value;
    KKVDoc kkvDoc;
    KKVVDumpTestHelper::ParseKKVMessage(message, pkey, isDeletedPkey, kkvDoc.skeyDeleted, kkvDoc.skey, kkvDoc.timestamp,
                                        kkvDoc.expireTime, value);
    size_t len = value.size();
    char* data = reinterpret_cast<char*>(pool.allocate(len));
    assert(data);
    std::copy(value.data(), value.data() + len, data);
    kkvDoc.value = std::string_view(data, len);
    ASSERT_TRUE(dumper.Dump(pkey, isDeletedPkey, isLastSkey, kkvDoc).IsOK());
}

void KKVVDumpTestHelper::ParseKKVMessage(const std::string& kkvDocStr, uint64_t& pkey, bool& isDeletedPkey,
                                         bool isDeletedSkey, uint64_t& skey, uint32_t& timestamp, uint32_t& expireTime,
                                         string& value)
{
    isDeletedPkey = false;
    isDeletedSkey = false;
    std::vector<std::vector<string>> pairs;
    StringUtil::fromString(kkvDocStr, pairs, "=", ",");
    std::map<std::string, std::string> key2val;
    for (const auto& kv : pairs) {
        ASSERT_EQ(2, kv.size()) << autil::StringUtil::toString(kv);
        ASSERT_FALSE(kv[0].empty());
        ASSERT_FALSE(kv[1].empty());
        key2val[kv[0]] = kv[1];
    }

    string cmdStr = key2val["cmd"];
    if (cmdStr == "delete") {
        if (!key2val["skey"].empty()) {
            isDeletedSkey = true;
        } else {
            isDeletedPkey = true;
        }
    } else {
        ASSERT_EQ("add", cmdStr) << cmdStr;
    }
    string pkeyStr = key2val["pkey"];
    ASSERT_TRUE(StringUtil::fromString(pkeyStr, pkey)) << pkeyStr;
    string tsStr = key2val["ts"];
    ASSERT_TRUE(tsStr.empty() || StringUtil::fromString(tsStr, timestamp)) << tsStr;

    if (isDeletedPkey) {
        return;
    }
    string skeyStr = key2val["skey"];
    ASSERT_TRUE(StringUtil::fromString(skeyStr, skey)) << skeyStr;
    string exStr = key2val["expire_time"];
    ASSERT_TRUE(StringUtil::fromString(exStr, expireTime)) << exStr;

    if (isDeletedSkey) {
        return;
    }
    value = key2val["value"];
}

} // namespace indexlibv2::index
