#include "FakeSegmentReader.h"
#include "future_lite/CoroInterface.h"
#include "future_lite/executors/SimpleExecutor.h"
#include "indexlib/config/BuildConfig.h"
#include "indexlib/config/OnlineConfig.h"
#include "indexlib/config/TabletOptions.h"
#include "indexlib/file_system/load_config/CacheLoadStrategy.h"
#include "indexlib/framework/IndexRoot.h"
#include "indexlib/index/kv/IKVSegmentReader.h"
#include "indexlib/index/kv/KVDiskIndexer.h"
#include "indexlib/index/kv/KVIndexReader.h"
#include "indexlib/table/kv_table/KVReaderImpl.h"
#include "unittest/unittest.h"

using namespace std;
using namespace autil;
using namespace indexlib;
using namespace indexlib::util;
using namespace indexlib::config;
using namespace indexlibv2::framework;
using namespace indexlibv2::index;

namespace indexlibv2 { namespace table {

class KVReaderTest : public TESTBASE
{
public:
    KVReaderTest() {}
    ~KVReaderTest() {}

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
    void PrepareSegmentReader(const std::string& offlineReaderStr, Reader& reader);
    void BatchGet(bool hasMetricsCollector);

private:
    AUTIL_LOG_DECLARE();
    autil::mem_pool::Pool* _pool = nullptr;
};

AUTIL_LOG_SETUP(indexlib.index, KVReaderTest);

TEST_F(KVReaderTest, TestGet)
{
    future_lite::executors::SimpleExecutor ex(1);
    {
        KVReaderImpl reader(DEFAULT_SCHEMAID);
        reader._hasTTL = true;
        reader._ttl = 1;
        PrepareSegmentReader("1,1,false,1", reader);
        autil::StringView value;
        auto status = future_lite::interface::syncAwait(reader.GetAsync(1, value, KVReadOptions()), &ex);
        ASSERT_EQ(status, KVResultStatus::FOUND);
        ASSERT_EQ("1", value.to_string());
        std::string key("xx");
        status = reader.Get(autil::StringView(key), value, KVReadOptions());
        ASSERT_EQ(KVResultStatus::NOT_FOUND, status);
    }

    {
        KVReaderImpl reader(DEFAULT_SCHEMAID);
        reader._hasTTL = false;
        reader._ttl = 1;
        PrepareSegmentReader("1,1,false,1", reader);
        autil::StringView value;
        auto status = future_lite::interface::syncAwait(reader.GetAsync(1, value, KVReadOptions()), &ex);
        ASSERT_EQ(status, KVResultStatus::FOUND);
        ASSERT_EQ("1", value.to_string());
    }
}

TEST_F(KVReaderTest, TestBatchGet)
{
    BatchGet(true);
    BatchGet(false);
}

// str:"key,value,isdeleted,ts;key,value,isDeleted,ts"
template <typename Reader>
void KVReaderTest::PrepareSegmentReader(const std::string& offlineReaderStr, Reader& reader)
{
    vector<vector<string>> values;
    StringUtil::fromString(offlineReaderStr, values, ",", ";");
    vector<std::pair<std::shared_ptr<IKVSegmentReader>, std::shared_ptr<framework::Locator>>> readers;
    for (size_t i = 0; i < values.size(); i++) {
        assert(values[i].size() == 4);
        auto segReader = std::make_shared<FakeSegmentReader>();
        segReader->SetKeyValue(values[i][0], values[i][1], values[i][2], values[i][3]);
        readers.emplace_back(std::make_pair(std::move(segReader), std::shared_ptr<framework::Locator>()));
    }
    reader._diskShardReaders.emplace_back(std::move(readers));
}

void KVReaderTest::BatchGet(bool hasMetricsCollector)
{
    KVMetricsCollector metricsCollector;
    KVReadOptions options;
    options.pool = _pool;
    options.metricsCollector = hasMetricsCollector ? &metricsCollector : NULL;
    // exist and not deleted key
    {
        KVReaderImpl reader(DEFAULT_SCHEMAID);
        reader._hasTTL = true;
        PrepareSegmentReader("0,0,false,0;1,1,false,1;2,2,true,2", reader);

        vector<uint64_t> keys = {0, 1, 2};
        vector<StringView> values;

        auto res = future_lite::interface::syncAwait(reader.BatchGetAsync(keys, values, options));
        ASSERT_EQ(future_lite::interface::getTryValue(res[0]), KVResultStatus::FOUND);
        ASSERT_EQ(future_lite::interface::getTryValue(res[1]), KVResultStatus::FOUND);
        ASSERT_EQ(future_lite::interface::getTryValue(res[2]), KVResultStatus::DELETED); // deleted
        ASSERT_EQ("0", values[0]);
        ASSERT_EQ("1", values[1]);
    }
    // exist and not exist key
    {
        KVReaderImpl reader(DEFAULT_SCHEMAID);
        reader._hasTTL = true;
        PrepareSegmentReader("0,0,false,0;1,1,true,1;2,2,true,2", reader);

        vector<uint64_t> keys = {0, 3};
        vector<StringView> values;

        auto res = future_lite::interface::syncAwait(reader.BatchGetAsync(keys, values, options));
        ASSERT_EQ(future_lite::interface::getTryValue(res[0]), KVResultStatus::FOUND);
        ASSERT_EQ(future_lite::interface::getTryValue(res[1]), KVResultStatus::NOT_FOUND); // not exist
        ASSERT_EQ("0", values[0]);
    }
    // same key in two segment
    {
        KVReaderImpl reader(DEFAULT_SCHEMAID);
        reader._hasTTL = true;
        PrepareSegmentReader("0,0,false,0;0,1,false,1;0,2,false,2", reader);

        vector<uint64_t> keys = {0, 1};
        vector<StringView> values;

        auto res = future_lite::interface::syncAwait(reader.BatchGetAsync(keys, values, options));
        ASSERT_EQ(future_lite::interface::getTryValue(res[0]), KVResultStatus::FOUND);
        ASSERT_EQ("0", values[0]);
    }
}

}} // namespace indexlibv2::table
