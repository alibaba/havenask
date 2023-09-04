
#include "indexlib/table/kv_table/KVReaderImpl.h"

#include "FakeSegmentReader.h"
#include "future_lite/CoroInterface.h"
#include "future_lite/executors/SimpleExecutor.h"
#include "indexlib/config/BuildConfig.h"
#include "indexlib/config/OnlineConfig.h"
#include "indexlib/config/TabletOptions.h"
#include "indexlib/file_system/test/LoadConfigListCreator.h"
#include "indexlib/framework/IndexRoot.h"
#include "indexlib/index/kv/IKVSegmentReader.h"
#include "indexlib/index/kv/KVDiskIndexer.h"
#include "indexlib/index/kv/KVIndexReader.h"
#include "unittest/unittest.h"

using namespace std;
using namespace autil;
using namespace indexlib;
using namespace indexlib::util;
using namespace indexlib::config;
using namespace indexlib::file_system;
using namespace indexlibv2::framework;
using namespace indexlibv2::index;

namespace indexlibv2 { namespace table {

class KVReaderImplTest : public TESTBASE
{
public:
    KVReaderImplTest() {}
    ~KVReaderImplTest() {}

public:
    void setUp() override
    {
        _pool = new autil::mem_pool::Pool();
        setenv("DISABLE_CODEGEN", "true", 1);
    }
    void tearDown() override
    {
        DELETE_AND_SET_NULL(_pool);
        unsetenv("DISABLE_CODEGEN");
    }

private:
    template <typename Reader>
    void PrepareSegmentReader(const std::string& offlineReaderStr, Reader& reader, size_t shardCount = 1);
    void InnerTestGet(const std::string& offlineValues, uint64_t ttl, uint64_t key, uint64_t searchTs,
                      bool successWithTTL, bool successWithoutTTL, const std::string& expectValue,
                      size_t shardCount = 1, std::shared_ptr<autil::TimeoutTerminator> timeoutTerminator = nullptr);

private:
    autil::mem_pool::Pool* _pool = nullptr;

private:
    AUTIL_LOG_DECLARE();
};

AUTIL_LOG_SETUP(indexlib.index, KVReaderImplTest);

TEST_F(KVReaderImplTest, TestGet)
{
    // test read from offline segment reader
    // ttl=1, search key=2, searchTs=0. hit offline reader
    InnerTestGet("2,2,false,1", 1, 2, 0, true, true, "2");
    // test offine value is deleted
    InnerTestGet("2,2,true,1", 1, 2, 0, false, false, "");
    // test not exist value
    // search key=3 not exist
    InnerTestGet("2,2,false,1", 1, 3, 0, false, false, "");
    // test offline value outOfTime
    // ttl=1, search key=2, searchTs=4, offline value ts=2,
    // hit offline reader, and out of time
    InnerTestGet("2,2,false,2", 1, 2, 4000000, false, true, "2");
    // test multi segment for offline reader
    InnerTestGet("1,1,false,1;2,2,false,1", 1, 1, 0, true, true, "1");

    // test multi shard
    for (size_t e = 1; e < 8; ++e) {
        size_t shardCount = 1 << e;
        InnerTestGet("2,2,false,1", 1, 2, 0, true, true, "2", shardCount);
    }

    // test timeout
    auto timeoutTerminator = std::make_shared<autil::TimeoutTerminator>(1);
    usleep(2);
    InnerTestGet("2,2,false,1", 1, 2, 0, false, false, "2", 1, timeoutTerminator);
}

void KVReaderImplTest::InnerTestGet(const string& offlineValues, uint64_t ttl, uint64_t key, uint64_t searchTs,
                                    bool successWithTTL, bool successWithoutTTL, const string& expectValue,
                                    size_t shardCount, std::shared_ptr<autil::TimeoutTerminator> timeoutTerminator)
{
    KVReadOptions readOptions;
    readOptions.timestamp = searchTs;
    readOptions.timeoutTerminator = timeoutTerminator;
    readOptions.searchCacheType = indexlib::tsc_no_cache;
    future_lite::executors::SimpleExecutor ex(1);
    {
        KVReaderImpl reader(DEFAULT_SCHEMAID);
        reader._hasTTL = true;
        reader._ttl = ttl;
        PrepareSegmentReader(offlineValues, reader, shardCount);
        autil::StringView value;
        auto status = future_lite::interface::syncAwait(reader.InnerGet(&readOptions, key, value), &ex);
        ASSERT_EQ(successWithTTL, status == KVResultStatus::FOUND);
        if (successWithTTL) {
            ASSERT_EQ(expectValue, value.to_string());
        }
    }

    {
        KVReaderImpl reader(DEFAULT_SCHEMAID);
        reader._hasTTL = false;
        PrepareSegmentReader(offlineValues, reader, shardCount);
        autil::StringView value;
        auto status = future_lite::interface::syncAwait(reader.InnerGet(&readOptions, key, value), &ex);
        ASSERT_EQ(successWithoutTTL, status == KVResultStatus::FOUND);
        if (successWithoutTTL) {
            ASSERT_EQ(expectValue, value.to_string());
        }
    }
}

// str:"key,value,isdeleted,ts;key,value,isDeleted,ts"
template <typename Reader>
void KVReaderImplTest::PrepareSegmentReader(const std::string& offlineReaderStr, Reader& reader, size_t shardCount)
{
    vector<vector<string>> values;
    StringUtil::fromString(offlineReaderStr, values, ",", ";");
    vector<vector<std::pair<std::shared_ptr<IKVSegmentReader>, std::shared_ptr<framework::Locator>>>> readers(
        shardCount);
    for (size_t i = 0; i < values.size(); i++) {
        assert(values[i].size() == 4);
        auto segReader = std::make_shared<FakeSegmentReader>();
        segReader->SetKeyValue(values[i][0], values[i][1], values[i][2], values[i][3]);
        keytype_t keyHash {};
        indexlib::util::GetHashKey(hft_uint64, values[i][0], keyHash);
        auto shardId = indexlib::util::ShardUtil::GetShardId(keyHash, shardCount);
        readers[shardId].emplace_back(std::make_pair(std::move(segReader), std::shared_ptr<framework::Locator>()));
    }
    reader._diskShardReaders = std::move(readers);
}

}} // namespace indexlibv2::table
