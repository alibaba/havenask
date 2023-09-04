#include "indexlib/index/kkv/built/BuiltSKeyIterator.h"

#include "indexlib/config/FieldConfig.h"
#include "indexlib/config/IndexConfigDeserializeResource.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/FileSystemCreator.h"
#include "indexlib/file_system/IDirectory.h"
#include "indexlib/file_system/ReaderOption.h"
#include "indexlib/index/kkv/built/KKVBuiltSegmentIteratorOption.h"
#include "indexlib/index/kkv/built/KKVBuiltValueReader.h"
#include "indexlib/index/kkv/common/KKVIndexFormat.h"
#include "indexlib/index/kkv/dump/KKVDataDumperFactory.h"
#include "unittest/unittest.h"

using namespace std;
using namespace autil;
using namespace autil::mem_pool;
using namespace indexlib::util;
using namespace indexlib::file_system;
using namespace autil::legacy::json;
using namespace autil::legacy;
using namespace indexlib::file_system;

namespace indexlibv2::index {

class BuiltSKeyIteratorTest : public TESTBASE
{
public:
    void setUp() override
    {
        auto testRoot = GET_TEMP_DATA_PATH();
        auto fs = FileSystemCreator::Create("test", testRoot).GetOrThrow();
        auto rootDiretory = Directory::Get(fs);
        _directory = rootDiretory->MakeDirectory("BuiltSKeyIteratorTest", DirectoryOption())->GetIDirectory();
        ASSERT_NE(nullptr, _directory);
        _defaultTs = 8192;
        _pkeyDeletedTs = 1024;
    }

    void tearDown() override {}

protected:
    template <bool valueInline, bool storeTs, bool storeExpireTime, bool isFixedValueLen, bool hasPkeyDeleted>
    void DoTest();

protected:
    std::vector<KKVDoc> PrepareKKVDocs(const std::shared_ptr<indexlibv2::config::KKVIndexConfig> indexConfig,
                                       bool storeTs, bool hasPKeyDeleted, uint32_t skeyCount);
    std::shared_ptr<indexlibv2::config::KKVIndexConfig> BuildIndexConfig(bool isValueInline, bool storeExpireTime,
                                                                         bool IsFixedValueLen);
    void DumpIndex(const std::shared_ptr<indexlibv2::config::KKVIndexConfig> indexConfig, bool StoreTs,
                   bool HasPkeyDeleted, const std::vector<std::vector<KKVDoc>>& kkvDocVec) const;

protected:
    autil::mem_pool::Pool _pool;
    std::shared_ptr<indexlib::file_system::IDirectory> _directory;
    uint32_t _defaultTs;
    uint32_t _pkeyDeletedTs;
};

TEST_F(BuiltSKeyIteratorTest, TestDeletedPKey)
{
    constexpr bool valueInline = true;
    constexpr bool storeTs = true;
    constexpr bool storeExpireTime = true;
    constexpr bool isFixedValueLen = false;
    constexpr bool hasPkeyDeleted = true;
    DoTest<valueInline, storeTs, storeExpireTime, isFixedValueLen, hasPkeyDeleted>();
}

TEST_F(BuiltSKeyIteratorTest, TestNotDeletedPKey)
{
    constexpr bool valueInline = true;
    constexpr bool storeTs = true;
    constexpr bool storeExpireTime = true;
    constexpr bool isFixedValueLen = false;
    constexpr bool hasPkeyDeleted = false;
    DoTest<valueInline, storeTs, storeExpireTime, isFixedValueLen, hasPkeyDeleted>();
}

TEST_F(BuiltSKeyIteratorTest, TestValueFixedLen)
{
    constexpr bool valueInline = true;
    constexpr bool storeTs = true;
    constexpr bool storeExpireTime = true;
    constexpr bool isFixedValueLen = true;
    constexpr bool hasPkeyDeleted = false;
    DoTest<valueInline, storeTs, storeExpireTime, isFixedValueLen, hasPkeyDeleted>();
}

TEST_F(BuiltSKeyIteratorTest, TestNotInline)
{
    constexpr bool valueInline = false;
    constexpr bool storeTs = true;
    constexpr bool storeExpireTime = true;
    constexpr bool isFixedValueLen = true;
    constexpr bool hasPkeyDeleted = true;
    DoTest<valueInline, storeTs, storeExpireTime, isFixedValueLen, hasPkeyDeleted>();
}

TEST_F(BuiltSKeyIteratorTest, TestNotInlineAndNotFixedLen)
{
    constexpr bool valueInline = false;
    constexpr bool storeTs = true;
    constexpr bool storeExpireTime = true;
    constexpr bool isFixedValueLen = false;
    constexpr bool hasPkeyDeleted = true;
    DoTest<valueInline, storeTs, storeExpireTime, isFixedValueLen, hasPkeyDeleted>();
}

TEST_F(BuiltSKeyIteratorTest, TestNotInlineAndNotDeletedPkey)
{
    constexpr bool valueInline = false;
    constexpr bool storeTs = true;
    constexpr bool storeExpireTime = true;
    constexpr bool isFixedValueLen = false;
    constexpr bool hasPkeyDeleted = false;
    DoTest<valueInline, storeTs, storeExpireTime, isFixedValueLen, hasPkeyDeleted>();
}

TEST_F(BuiltSKeyIteratorTest, TestNotStoreTs)
{
    constexpr bool valueInline = true;
    constexpr bool storeTs = false;
    constexpr bool storeExpireTime = true;
    constexpr bool isFixedValueLen = false;
    constexpr bool hasPkeyDeleted = true;
    DoTest<valueInline, storeTs, storeExpireTime, isFixedValueLen, hasPkeyDeleted>();
}

TEST_F(BuiltSKeyIteratorTest, TestNotStoreExpireTime)
{
    constexpr bool valueInline = true;
    constexpr bool storeTs = true;
    constexpr bool storeExpireTime = false;
    constexpr bool isFixedValueLen = false;
    constexpr bool hasPkeyDeleted = true;
    DoTest<valueInline, storeTs, storeExpireTime, isFixedValueLen, hasPkeyDeleted>();
}

TEST_F(BuiltSKeyIteratorTest, TestNotStoreTsAndNotStoreExpireTime)
{
    constexpr bool valueInline = true;
    constexpr bool storeTs = false;
    constexpr bool storeExpireTime = false;
    constexpr bool isFixedValueLen = false;
    constexpr bool hasPkeyDeleted = true;
    DoTest<valueInline, storeTs, storeExpireTime, isFixedValueLen, hasPkeyDeleted>();
}

template <bool valueInline, bool storeTs, bool storeExpireTime, bool isFixedValueLen, bool hasPkeyDeleted>
void BuiltSKeyIteratorTest::DoTest()
{
    auto indexConfig = BuildIndexConfig(valueInline, storeExpireTime, isFixedValueLen);
    auto kkvDocs = PrepareKKVDocs(indexConfig, storeTs, hasPkeyDeleted, 256);
    DumpIndex(indexConfig, storeTs, hasPkeyDeleted, {kkvDocs});

    ReaderOption readerOption(FSOT_MMAP);
    readerOption.isSwapMmapFile = false;
    auto [status, skeyFileReader] = _directory->CreateFileReader("skey", readerOption).StatusWith();
    ASSERT_TRUE(status.IsOK());

    using Option = KKVBuiltSegmentIteratorOption<valueInline, storeTs, storeExpireTime>;
    std::shared_ptr<BuiltSKeyIterator<uint64_t, Option>> skeyIterator;
    std::shared_ptr<KKVBuiltValueReader> valueReader;

    skeyIterator = std::make_shared<BuiltSKeyIterator<uint64_t, Option>>(
        _defaultTs, true, &_pool, indexConfig->GetValueConfig()->GetFixedLength(), ReadOption::LowLatency());
    if constexpr (!valueInline) {
        auto [status1, valueFileReader] = _directory->CreateFileReader("value", readerOption).StatusWith();
        ASSERT_TRUE(status1.IsOK());
        valueReader = std::make_shared<KKVBuiltValueReader>(indexConfig->GetValueConfig()->GetFixedLength(), true,
                                                            &_pool, ReadOption::LowLatency());
        valueReader->Init(valueFileReader.get(), false);
    }

    OnDiskPKeyOffset firstSkeyOffset(0, 0);
    status = future_lite::interface::syncAwait(skeyIterator->Init(skeyFileReader.get(), false, firstSkeyOffset));
    ASSERT_TRUE(status.IsOK());
    size_t i = 0;
    if constexpr (hasPkeyDeleted) {
        ASSERT_EQ(true, skeyIterator->HasPKeyDeleted());
        uint32_t expectedTs = storeTs ? _pkeyDeletedTs : _defaultTs;
        ASSERT_EQ(expectedTs, skeyIterator->GetPKeyDeletedTs());
        ++i;
    }
    for (; skeyIterator->IsValid() && i < kkvDocs.size(); ++i, skeyIterator->MoveToNext()) {
        ASSERT_EQ(kkvDocs[i].skey, skeyIterator->GetSKey()) << i;
        ASSERT_EQ(kkvDocs[i].skeyDeleted, skeyIterator->IsDeleted()) << i;
        ASSERT_EQ(kkvDocs[i].timestamp, skeyIterator->GetTs()) << i;
        ASSERT_EQ(kkvDocs[i].expireTime, skeyIterator->GetExpireTime()) << i;
        if (kkvDocs[i].skeyDeleted) {
            continue;
        }
        auto iterator = dynamic_pointer_cast<BuiltSKeyIterator<uint64_t, Option>>(skeyIterator);
        if constexpr (valueInline) {
            ASSERT_EQ(kkvDocs[i].value, iterator->GetValue()) << i;
        } else {
            auto valueOffset = iterator->GetValueOffset();
            ASSERT_EQ(kkvDocs[i].value, valueReader->Read(valueOffset)) << i;
        }
    }
    ASSERT_EQ(i, kkvDocs.size());
    ASSERT_FALSE(skeyIterator->IsValid());
}

std::vector<KKVDoc>
BuiltSKeyIteratorTest::PrepareKKVDocs(const std::shared_ptr<indexlibv2::config::KKVIndexConfig> indexConfig,
                                      bool storeTs, bool hasPKeyDeleted, uint32_t skeyCount)
{
    std::vector<KKVDoc> kkvDocs;

    if (hasPKeyDeleted) {
        KKVDoc doc;
        doc.timestamp = storeTs ? _pkeyDeletedTs : _defaultTs;
        kkvDocs.push_back(doc);
    }

    int32_t fixedLength = indexConfig->GetValueConfig()->GetFixedLength();
    bool storeExpireTime = indexConfig->StoreExpireTime();

    for (uint32_t skey = 0; skey < skeyCount; ++skey) {
        uint32_t ts = storeTs ? (skey + 1024u) : _defaultTs;
        uint32_t size = fixedLength > 0 ? fixedLength : skey;
        char* buf = reinterpret_cast<char*>(_pool.allocate(size));
        for (size_t j = 0; j < size; ++j) {
            buf[j] = skey;
        }
        KKVDoc doc(skey, ts, autil::StringView(buf, size));
        doc.expireTime = storeExpireTime ? (skey + 1024u) : 0;
        kkvDocs.push_back(doc);
        if (skey % 32 == 0) {
            kkvDocs.back().skeyDeleted = true;
            kkvDocs.back().value = autil::StringView::empty_instance();
        }
    }
    return kkvDocs;
}

void BuiltSKeyIteratorTest::DumpIndex(const std::shared_ptr<indexlibv2::config::KKVIndexConfig> indexConfig,
                                      bool storeTs, bool hasPKeyDeleted,
                                      const std::vector<std::vector<KKVDoc>>& kkvDocVecs) const
{
    auto dumper = KKVDataDumperFactory::Create<uint64_t>(indexConfig, storeTs, KKVDumpPhrase::MERGE_BOTTOMLEVEL);
    ASSERT_NE(nullptr, dumper);
    auto writerOption = indexlib::file_system::WriterOption::Buffer();
    ASSERT_TRUE(dumper->Init(_directory, writerOption, writerOption, kkvDocVecs.size()).IsOK());

    for (uint64_t pk = 0; pk < kkvDocVecs.size(); ++pk) {
        auto& kkvDocs = kkvDocVecs[pk];
        for (size_t i = 0; i < kkvDocs.size(); ++i) {
            bool isPKeyDeleted = (i == 0 && hasPKeyDeleted);
            ASSERT_TRUE(dumper->Dump(pk, isPKeyDeleted, i + 1 == kkvDocs.size(), kkvDocs[i]).IsOK());
        }
    }
    ASSERT_TRUE(dumper->Close().IsOK());
    KKVIndexFormat indexFormat(storeTs, false, indexConfig->GetValueConfig()->GetFixedLength());
    ASSERT_TRUE(indexFormat.Store(_directory).IsOK());
}

std::shared_ptr<indexlibv2::config::KKVIndexConfig>
BuiltSKeyIteratorTest::BuildIndexConfig(bool isValueInline, bool storeExpireTime, bool isFixedValueLen)
{
    string indexConfigStr = R"({
            "index_name": "pkey_skey",
            "index_type": "PRIMARY_KEY",
            "index_fields": [
                {"field_name": "uid","key_type": "prefix"},
                {"field_name": "friendid","key_type": "suffix"}
            ],
            "value_fields": ["value"],
            "index_preference":{"value_inline": false},
            "store_expire_time": false
          })";
    JsonMap jsonMap;
    FromJsonString(jsonMap, indexConfigStr);

    if (isValueInline) {
        JsonMap valueInlineJsonMap;
        FromJsonString(valueInlineJsonMap, R"({"value_inline": true})");
        jsonMap["index_preference"] = valueInlineJsonMap;
    }
    if (storeExpireTime) {
        jsonMap["store_expire_time"] = true;
    }

    auto any = ToJson(jsonMap);
    std::string fieldDesc = "uid:long;friendid:long;value:";
    fieldDesc += isFixedValueLen ? "int64" : "string";
    auto fieldConfigs = config::FieldConfig::TEST_CreateFields(fieldDesc);
    config::MutableJson runtimeSettings;
    config::IndexConfigDeserializeResource resource(fieldConfigs, runtimeSettings);
    auto indexConfig = std::make_shared<indexlibv2::config::KKVIndexConfig>();
    indexConfig->Deserialize(any, 0, resource);
    if (isFixedValueLen) {
        indexConfig->GetValueConfig()->EnableCompactFormat(true);
    }
    return indexConfig;
}

class FileReaderWrapperForTestLargeChunk : public FileReader
{
public:
    FileReaderWrapperForTestLargeChunk(FileReader* reader) : _reader(reader) {}

public:
    FSResult<void> Open() noexcept override { return _reader->Open(); }
    FSResult<void> Close() noexcept override { return _reader->Close(); }
    FSResult<size_t> Read(void* buffer, size_t length, size_t offset, ReadOption option) noexcept override
    {
        _lastReadOffset = offset;
        if (offset > std::numeric_limits<uint32_t>::max()) {
            // for test
            offset = 0;
        }
        return _reader->Read(buffer, length, offset, option);
    }
    FSResult<size_t> Read(void* buffer, size_t length, ReadOption option) noexcept override
    {
        return _reader->Read(buffer, length, option);
    }
    indexlib::util::ByteSliceList* ReadToByteSliceList(size_t length, size_t offset,
                                                       ReadOption option = ReadOption()) noexcept override
    {
        return _reader->ReadToByteSliceList(length, offset, option);
    }
    void* GetBaseAddress() const noexcept override { return _reader->GetBaseAddress(); }
    size_t GetLength() const noexcept override { return _reader->GetLength(); }
    size_t GetLogicLength() const noexcept override { return _reader->GetLogicLength(); }
    const std::string& GetLogicalPath() const noexcept override { return _reader->GetLogicalPath(); }
    const std::string& GetPhysicalPath() const noexcept override { return _reader->GetPhysicalPath(); }
    FSOpenType GetOpenType() const noexcept override { return _reader->GetOpenType(); }
    FSFileType GetType() const noexcept override { return _reader->GetType(); }
    std::shared_ptr<FileNode> GetFileNode() const noexcept override { return _reader->GetFileNode(); }

public:
    size_t GetLastReadOffset() { return _lastReadOffset; }

private:
    FileReader* _reader;

private:
    size_t _lastReadOffset;
};

TEST_F(BuiltSKeyIteratorTest, TestLargeChunkOffset)
{
    constexpr bool valueInline = false;
    constexpr bool storeTs = true;
    constexpr bool storeExpireTime = true;
    constexpr bool isFixedValueLen = false;
    constexpr bool hasPkeyDeleted = true;

    auto indexConfig = BuildIndexConfig(valueInline, storeExpireTime, isFixedValueLen);
    auto kkvDocs = PrepareKKVDocs(indexConfig, storeTs, hasPkeyDeleted, 256);
    DumpIndex(indexConfig, storeTs, hasPkeyDeleted, {kkvDocs});

    ReaderOption readerOption(FSOT_BUFFERED);
    readerOption.isSwapMmapFile = false;
    auto [status, skeyFileReader] = _directory->CreateFileReader("skey", readerOption).StatusWith();
    ASSERT_TRUE(status.IsOK());
    FileReaderWrapperForTestLargeChunk skeyFileReaderWrapper(skeyFileReader.get());

    using Option = KKVBuiltSegmentIteratorOption<valueInline, storeTs, storeExpireTime>;
    std::shared_ptr<BuiltSKeyIterator<uint64_t, Option>> skeyIterator;
    skeyIterator = std::make_shared<BuiltSKeyIterator<uint64_t, Option>>(
        _defaultTs, true, &_pool, indexConfig->GetValueConfig()->GetFixedLength(), ReadOption::LowLatency());
    // 4312642662 > uint32_t max
    uint64_t chunkOffset = 4312642662;
    OnDiskPKeyOffset firstSkeyOffset(chunkOffset, 0);
    status = future_lite::interface::syncAwait(skeyIterator->Init(&skeyFileReaderWrapper, false, firstSkeyOffset));
    ASSERT_TRUE(status.IsOK());
    ASSERT_EQ(chunkOffset + sizeof(ChunkMeta), skeyFileReaderWrapper.GetLastReadOffset());
}

} // namespace indexlibv2::index
