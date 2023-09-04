#include "indexlib/index/kkv/search/KKVCacheItem.h"

#include "indexlib/config/FieldConfig.h"
#include "indexlib/config/IndexConfigDeserializeResource.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/FileSystemCreator.h"
#include "indexlib/file_system/IDirectory.h"
#include "indexlib/index/kkv/config/KKVIndexConfig.h"
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

class KKVCacheItemTest : public TESTBASE
{
public:
    void setUp() override
    {
        auto testRoot = GET_TEMP_DATA_PATH();
        auto fs = FileSystemCreator::Create("test", testRoot).GetOrThrow();
        auto rootDiretory = Directory::Get(fs);
        _directory = rootDiretory->MakeDirectory("KKVCacheItemTest", DirectoryOption())->GetIDirectory();
        ASSERT_NE(nullptr, _directory);
        _defaultTs = 8192;
        _pkeyDeletedTs = 1024;
    }

    void tearDown() override {}

protected:
    KKVDocs PrepareKKVDocs(const std::shared_ptr<indexlibv2::config::KKVIndexConfig> indexConfig, bool storeTs,
                           bool hasPKeyDeleted, uint32_t skeyCount);
    std::shared_ptr<indexlibv2::config::KKVIndexConfig> BuildIndexConfig(bool isValueInline, bool storeExpireTime,
                                                                         bool IsFixedValueLen);

protected:
    autil::mem_pool::Pool _pool;
    std::shared_ptr<indexlib::file_system::IDirectory> _directory;
    uint32_t _defaultTs;
    uint32_t _pkeyDeletedTs;
};

KKVDocs KKVCacheItemTest::PrepareKKVDocs(const std::shared_ptr<indexlibv2::config::KKVIndexConfig> indexConfig,
                                         bool storeTs, bool hasPKeyDeleted, uint32_t skeyCount)
{
    KKVDocs kkvDocs(&_pool);

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

std::shared_ptr<indexlibv2::config::KKVIndexConfig>
KKVCacheItemTest::BuildIndexConfig(bool isValueInline, bool storeExpireTime, bool isFixedValueLen)
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

TEST_F(KKVCacheItemTest, TestCreate)
{
    using SKeyType = uint64_t;
    constexpr bool valueInline = false;
    constexpr bool storeTs = true;
    constexpr bool storeExpireTime = true;
    constexpr bool isFixedValueLen = false;
    constexpr bool hasPkeyDeleted = false;
    auto indexConfig = BuildIndexConfig(valueInline, storeExpireTime, isFixedValueLen);
    auto kkvDocs = PrepareKKVDocs(indexConfig, storeTs, hasPkeyDeleted, 256);
    auto docBeginIter = kkvDocs.begin();
    auto docEndIter = kkvDocs.begin() + kkvDocs.size();
    auto newKkvCacheItem = index::KKVCacheItem<SKeyType>::Create(docBeginIter, docEndIter);
    ASSERT_TRUE(newKkvCacheItem);
    ASSERT_TRUE(newKkvCacheItem->IsCacheItemValid());
    uint32_t count = 256 - (256 / 32);
    ASSERT_EQ(count, newKkvCacheItem->count);
    auto skeyNodes = newKkvCacheItem->GetSKeyNodes();
    ASSERT_TRUE(skeyNodes);
    uint32_t skey = 0;
    for (auto i = 0; i < count; ++i) {
        if (skey % 32 == 0) {
            ++skey;
        }
        ASSERT_EQ(skeyNodes[i].skey, skey);
        ASSERT_EQ(skeyNodes[i].timestamp, skey + 1024u);
        ASSERT_EQ(skeyNodes[i].expireTime, skey + 1024u);
        ++skey;
    }
    ASSERT_EQ(newKkvCacheItem->Size(), sizeof(KKVCacheItem<SKeyType>) + sizeof(CachedSKeyNode<SKeyType>) * count +
                                           skeyNodes[count - 1].valueOffset);
}

TEST_F(KKVCacheItemTest, TestClone)
{
    using SKeyType = uint64_t;
    constexpr bool valueInline = false;
    constexpr bool storeTs = true;
    constexpr bool storeExpireTime = true;
    constexpr bool isFixedValueLen = false;
    constexpr bool hasPkeyDeleted = false;
    auto indexConfig = BuildIndexConfig(valueInline, storeExpireTime, isFixedValueLen);
    auto kkvDocs = PrepareKKVDocs(indexConfig, storeTs, hasPkeyDeleted, 256);
    auto docBeginIter = kkvDocs.begin();
    auto docEndIter = kkvDocs.begin() + kkvDocs.size();
    auto newKkvCacheItem = index::KKVCacheItem<SKeyType>::Create(docBeginIter, docEndIter);
    ASSERT_TRUE(newKkvCacheItem);
    framework::Locator locator;
    auto cloneKkvCacheItem = newKkvCacheItem->Clone(&locator);
    ASSERT_TRUE(cloneKkvCacheItem);
    ASSERT_EQ(cloneKkvCacheItem->nextRtSegmentId, newKkvCacheItem->nextRtSegmentId);
    ASSERT_EQ(cloneKkvCacheItem->timestamp, newKkvCacheItem->timestamp);
    ASSERT_EQ(cloneKkvCacheItem->locator, locator);
    ASSERT_EQ(cloneKkvCacheItem->count, newKkvCacheItem->count);
    ASSERT_EQ(cloneKkvCacheItem->Size(), newKkvCacheItem->Size());
    size_t bufferLen = cloneKkvCacheItem->Size() - sizeof(KKVCacheItem<SKeyType>);
    for (auto i = 0; i < bufferLen; ++i) {
        ASSERT_EQ((cloneKkvCacheItem->base)[i], (newKkvCacheItem->base)[i]);
    }
}

} // namespace indexlibv2::index
