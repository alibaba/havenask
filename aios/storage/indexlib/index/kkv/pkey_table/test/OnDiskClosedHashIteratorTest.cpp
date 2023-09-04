#include "indexlib/index/kkv/pkey_table/OnDiskClosedHashIterator.h"

#include "indexlib/config/ITabletSchema.h"
#include "indexlib/file_system/FileSystemCreator.h"
#include "indexlib/file_system/IDirectory.h"
#include "indexlib/index/kkv/Constant.h"
#include "indexlib/index/kkv/config/KKVIndexConfig.h"
#include "indexlib/table/kkv_table/test/KKVTabletSchemaMaker.h"
#include "unittest/unittest.h"

using namespace std;
using namespace indexlib::util;
using namespace indexlib::config;
using namespace indexlib::file_system;

namespace indexlibv2::index {

class OnDiskClosedHashIteratorTest : public TESTBASE
{
public:
    void setUp() override;
    void tearDown() override;

private:
    template <PKeyTableType Type>
    void PrepareData(size_t count);
    template <PKeyTableType Type>
    void CheckData(size_t count);

private:
    std::shared_ptr<config::ITabletSchema> _tabletSchema;
    std::shared_ptr<config::KKVIndexConfig> _kkvIndexConfig;
    std::shared_ptr<indexlib::file_system::IDirectory> _rootDir;

private:
    AUTIL_LOG_DECLARE();
};

AUTIL_LOG_SETUP(indexlib.index, OnDiskClosedHashIteratorTest);

void OnDiskClosedHashIteratorTest::setUp()
{
    auto testRoot = GET_TEMP_DATA_PATH();
    auto fs = indexlib::file_system::FileSystemCreator::Create("test", testRoot).GetOrThrow();
    _rootDir = indexlib::file_system::IDirectory::Get(fs);

    string fields = "single_int8:int8;single_int16:int16;pkey:uint64;skey:uint64";
    _tabletSchema = table::KKVTabletSchemaMaker::Make(fields, "pkey", "skey", "single_int8;single_int16;skey");
    _kkvIndexConfig =
        std::dynamic_pointer_cast<indexlibv2::config::KKVIndexConfig>(_tabletSchema->GetIndexConfig("kkv", "pkey"));
}

void OnDiskClosedHashIteratorTest::tearDown() {}

TEST_F(OnDiskClosedHashIteratorTest, TestDenseProcess)
{
    size_t count = 10;
    PrepareData<PKeyTableType::DENSE>(count);
    CheckData<PKeyTableType::DENSE>(count);
}

TEST_F(OnDiskClosedHashIteratorTest, TestCuckooProcess)
{
    size_t count = 10;
    PrepareData<PKeyTableType::CUCKOO>(count);
    CheckData<PKeyTableType::CUCKOO>(count);
}

template <PKeyTableType Type>
void OnDiskClosedHashIteratorTest::PrepareData(size_t count)
{
    auto dataDir = _rootDir->MakeDirectory("index/pkey", DirectoryOption::Mem()).GetOrThrow();
    using Traits = typename ClosedHashPrefixKeyTableTraits<uint64_t, Type>::Traits;
    using ClosedHashTable = typename Traits::HashTable;

    size_t memorySize = ClosedHashTable::DoCapacityToTableMemory(count, 50);
    vector<char> buffer;
    buffer.resize(memorySize);
    ClosedHashTable table;
    table.MountForWrite(buffer.data(), memorySize, 50);
    EXPECT_LE(count, table.Capacity());

    for (size_t i = 0; i < count; ++i) {
        uint64_t key = i;
        OnDiskPKeyOffset offset(i, i);
        table.Insert(key, offset.ToU64Value());
    }
    auto fileWriter =
        dataDir->CreateFileWriter(PREFIX_KEY_FILE_NAME, indexlib::file_system::WriterOption()).GetOrThrow();
    fileWriter->Write(buffer.data(), buffer.size()).GetOrThrow();
    ASSERT_EQ(FSEC_OK, fileWriter->Close());
    _rootDir->Sync(true).GetOrThrow();
}

template <PKeyTableType Type>
void OnDiskClosedHashIteratorTest::CheckData(size_t count)
{
    auto dataDir = _rootDir->GetDirectory("index/pkey").GetOrThrow();
    OnDiskClosedHashIterator<Type> iter;
    iter.Open(_kkvIndexConfig, dataDir);
    iter.SortByKey();
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
