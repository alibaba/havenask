#include "indexlib/index/kkv/pkey_table/OnDiskSeparateChainHashIterator.h"

#include "indexlib/config/ITabletSchema.h"
#include "indexlib/file_system/FileSystemCreator.h"
#include "indexlib/file_system/IDirectory.h"
#include "indexlib/index/common/hash_table/HashTableWriter.h"
#include "indexlib/index/kkv/Constant.h"
#include "indexlib/index/kkv/config/KKVIndexConfig.h"
#include "indexlib/table/kkv_table/test/KKVTabletSchemaMaker.h"
#include "unittest/unittest.h"

using namespace std;
using namespace indexlib::util;
using namespace indexlib::config;
using namespace indexlib::file_system;

namespace indexlibv2::index {

class OnDiskSeparateChainHashIteratorTest : public TESTBASE
{
public:
    void setUp() override;
    void tearDown() override;

private:
    void PrepareData(size_t count);
    void CheckData(size_t count);

private:
    std::shared_ptr<config::ITabletSchema> _tabletSchema;
    std::shared_ptr<config::KKVIndexConfig> _kkvIndexConfig;
    std::shared_ptr<indexlib::file_system::IDirectory> _rootDir;

private:
    AUTIL_LOG_DECLARE();
};

AUTIL_LOG_SETUP(indexlib.index, OnDiskSeparateChainHashIteratorTest);

void OnDiskSeparateChainHashIteratorTest::setUp()
{
    auto testRoot = GET_TEMP_DATA_PATH();
    auto fs = indexlib::file_system::FileSystemCreator::Create("test", testRoot).GetOrThrow();
    _rootDir = indexlib::file_system::IDirectory::Get(fs);

    string fields = "single_int8:int8;single_int16:int16;pkey:uint64;skey:uint64";
    _tabletSchema = table::KKVTabletSchemaMaker::Make(fields, "pkey", "skey", "single_int8;single_int16;skey");
    _kkvIndexConfig =
        std::dynamic_pointer_cast<indexlibv2::config::KKVIndexConfig>(_tabletSchema->GetIndexConfig("kkv", "pkey"));
}

void OnDiskSeparateChainHashIteratorTest::tearDown() {}

TEST_F(OnDiskSeparateChainHashIteratorTest, TestSimpleProcess)
{
    size_t count = 10;
    PrepareData(count);
    CheckData(count);
}

void OnDiskSeparateChainHashIteratorTest::PrepareData(size_t count)
{
    auto dataDir = _rootDir->MakeDirectory("index/pkey", DirectoryOption::Mem()).GetOrThrow();
    HashTableWriter<uint64_t, OnDiskPKeyOffset> writer;
    writer.Init(dataDir, PREFIX_KEY_FILE_NAME, count * 3);
    for (size_t i = 0; i < count; ++i) {
        uint64_t key = i;
        OnDiskPKeyOffset offset(i, i);
        writer.Add(key, offset);
    }
    writer.Close();
}

void OnDiskSeparateChainHashIteratorTest::CheckData(size_t count)
{
    auto dataDir = _rootDir->GetDirectory("index/pkey").GetOrThrow();
    OnDiskSeparateChainHashIterator iter;
    iter.Open(_kkvIndexConfig, dataDir);
    size_t totalCount = 0;
    while (iter.IsValid()) {
        uint64_t expectKey = totalCount;
        OnDiskPKeyOffset expectOffset(totalCount, totalCount);
        ASSERT_EQ(expectKey, iter.Key());
        ASSERT_EQ(expectOffset, iter.Value());
        iter.MoveToNext();
        ++totalCount;
    }

    ASSERT_EQ(count, totalCount);
    ASSERT_EQ(count, iter.Size());
}

} // namespace indexlibv2::index
