#include "suez/service/KVTableSearcher.h"

#include "future_lite/Executor.h"
#include "future_lite/ExecutorCreator.h"
#include "indexlib/index/common/KeyHasherWrapper.h"
#include "indexlib/table/kv_table/KVTabletSessionReader.h"
#include "indexlib/table/kv_table/test/KVTableTestHelper.h"
#include "indexlib/table/kv_table/test/KVTabletSchemaMaker.h"
#include "table/TableUtil.h"
#include "table/test/TableTestUtil.h"
#include "unittest/unittest.h"

using namespace std;
using namespace autil;
using namespace table;
using namespace google::protobuf;

namespace suez {

class KVTableSearcherTest : public TESTBASE {
public:
    KVTableSearcherTest() {}
    ~KVTableSearcherTest() {}

public:
    std::shared_ptr<indexlibv2::framework::ITablet> createTablet(const string &tableName);
    std::unique_ptr<KVTableSearcher> prepareKvTabletSearcher(std::shared_ptr<indexlibv2::framework::ITablet> &tablet);
    std::unique_ptr<LookupOptions> makeLookupOptions(const vector<string> &attrs);

    vector<autil::StringView> sortedValues(const absl::flat_hash_map<uint64_t, autil::StringView> &foundValues) {
        std::vector<autil::StringView> values;
        std::map<uint64_t, autil::StringView> sortedValues;
        for (auto [hashKey, value] : foundValues) {
            sortedValues[hashKey] = value;
        }
        for (auto [hashKey, value] : sortedValues) {
            values.emplace_back(value);
        }
        return values;
    }

public:
    void setUp() override {
        _executor = future_lite::ExecutorCreator::Create(
            "hos", future_lite::ExecutorCreator::Parameters().SetExecutorName("test").SetThreadNum(2));
        _pool = autil::mem_pool::PoolPtr(new autil::mem_pool::Pool(1024));
        _multiTableReader = make_shared<MultiTableReader>();
    }

protected:
    std::unique_ptr<indexlibv2::table::KVTableTestHelper> _helper;
    std::unique_ptr<future_lite::Executor> _executor;
    std::shared_ptr<MultiTableReader> _multiTableReader;
    autil::mem_pool::PoolPtr _pool;
};

std::shared_ptr<indexlibv2::framework::ITablet> KVTableSearcherTest::createTablet(const string &tableName) {
    auto schemaPtr = indexlibv2::table::KVTabletSchemaMaker::Make(
        "pk:string;int_field:int32;double_field:double;float_field:float;multi_int_field:int32:true;multi_double_field:"
        "double:true;string_field:string;multi_string_field:string:true;multi_int_fix_count:int32:true:5;multi_"
        "float_fix_count:float:true:6",
        "pk",
        "int_field;double_field;float_field;multi_int_field;multi_double_field;string_field;multi_string_field;multi_"
        "int_fix_count:true;multi_float_fix_count:true");
    schemaPtr->TEST_SetTableName(tableName);
    string docStr = "cmd=add,pk=pk0,"
                    "int_field=2,"
                    "double_field=0.01,"
                    "float_field=3.3,"
                    "multi_int_field=11 12,"
                    "multi_double_field=22.1 22.2 22.3,"
                    "string_field=str0,"
                    "multi_string_field=str00 str01 str02 str03,"
                    "multi_int_fix_count=9 10 9 10 8,"
                    "multi_float_fix_count=9.1 9.2 9.3 9.4 9.5 9.6;"
                    "cmd=add,pk=pk1,"
                    "int_field=4,"
                    "double_field=0.02,"
                    "float_field=5.5,"
                    "multi_int_field=41 42,"
                    "multi_double_field=66.1 66.2 66.3,"
                    "string_field=str1,"
                    "multi_string_field=str10 str11 str12 str13,"
                    "multi_int_fix_count=29 20 29 20 28,"
                    "multi_float_fix_count=29.1 29.2 29.3 29.4 29.5 29.6;";
    const string path = GET_TEMP_DATA_PATH() + "/kv_" + tableName;
    indexlibv2::framework::IndexRoot indexRoot(path, path);
    unique_ptr<indexlibv2::config::TabletOptions> tabletOptions = make_unique<indexlibv2::config::TabletOptions>();
    tabletOptions->SetIsOnline(true);
    _helper = std::make_unique<indexlibv2::table::KVTableTestHelper>();
    auto status = _helper->Open(indexRoot, schemaPtr, std::move(tabletOptions));
    if (!status.IsOK()) {
        return nullptr;
    }
    auto tablet = _helper->GetITablet();
    status = _helper->Build(docStr);
    if (!status.IsOK()) {
        return nullptr;
    }
    return tablet;
}

std::unique_ptr<KVTableSearcher>
KVTableSearcherTest::prepareKvTabletSearcher(std::shared_ptr<indexlibv2::framework::ITablet> &tablet) {
    auto tabletReader = tablet->GetTabletReader();
    if (!tabletReader) {
        return nullptr;
    }
    auto kvTabletReader = std::dynamic_pointer_cast<indexlibv2::table::KVTabletSessionReader>(tabletReader);
    if (!kvTabletReader) {
        return nullptr;
    }
    auto searcher = std::make_unique<KVTableSearcher>(_multiTableReader.get());
    searcher->_tableName = "test_table";
    searcher->_indexName = "pk";
    searcher->_tabletReaderHolder[0] = kvTabletReader;
    return searcher;
}

std::unique_ptr<LookupOptions> KVTableSearcherTest::makeLookupOptions(const vector<string> &attrs) {
    std::unique_ptr<LookupOptions> options =
        std::make_unique<LookupOptions>(attrs, 60000, _executor.get(), _pool.get());
    return options;
}

TEST_F(KVTableSearcherTest, TestLookupExistKey) {
    auto tablet = createTablet("test_table");
    ASSERT_TRUE(tablet);
    auto kvSearcher = prepareKvTabletSearcher(tablet);
    ASSERT_TRUE(kvSearcher);

    vector<string> attrs{"int_field", "string_field", "double_field"};
    auto options = makeLookupOptions(attrs);

    string key1 = "pk0";
    string key2 = "pk1";
    indexlib::dictkey_t hashKey1 = 0;
    indexlib::dictkey_t hashKey2 = 0;
    indexlib::index::KeyHasherWrapper::GetHashKey(ft_string, true, key1.data(), key1.size(), hashKey1);
    indexlib::index::KeyHasherWrapper::GetHashKey(ft_string, true, key2.data(), key2.size(), hashKey2);
    KVTableSearcher::InputHashKeys inputHashKeys;
    inputHashKeys.emplace_back(make_pair<int, vector<uint64_t>>(0, {hashKey1, hashKey2}));
    auto lookupResult = kvSearcher->lookup(inputHashKeys, options.get());
    ASSERT_TRUE(lookupResult.is_ok()) << lookupResult.get_error().get_stack_trace();

    std::vector<autil::StringView> values = sortedValues(lookupResult.get().foundValues);
    auto dataResult = kvSearcher->convertResult(0, values, options.get());
    ASSERT_TRUE(dataResult.is_ok()) << dataResult.get_error().get_stack_trace();
    auto table = std::make_shared<table::Table>(_pool);
    table->deserializeFromString(dataResult.get(), _pool.get());
    ASSERT_EQ(2, table->getRowCount());
    ASSERT_EQ(3, table->getColumnCount());
    ASSERT_NO_FATAL_FAILURE(TableTestUtil::checkOutputColumn<int32_t>(table, "int_field", {2, 4}));
    ASSERT_NO_FATAL_FAILURE(TableTestUtil::checkOutputColumn<double>(table, "double_field", {0.01, 0.02}));
    ASSERT_NO_FATAL_FAILURE(TableTestUtil::checkOutputColumn<string>(table, "string_field", {"str0", "str1"}));
}

TEST_F(KVTableSearcherTest, TestLookupNotExistKey) {
    auto tablet = createTablet("test_table");
    ASSERT_TRUE(tablet);
    auto kvSearcher = prepareKvTabletSearcher(tablet);
    ASSERT_TRUE(kvSearcher);

    // pk没完全找到需要应该需要报一下
    vector<string> attrs{"int_field", "float_field"};
    auto options = makeLookupOptions(attrs);
    string key1 = "pk0";
    string key2 = "pkNotExist";
    indexlib::dictkey_t hashKey1 = 0;
    indexlib::dictkey_t hashKey2 = 0;
    indexlib::index::KeyHasherWrapper::GetHashKey(ft_string, true, key1.data(), key1.size(), hashKey1);
    indexlib::index::KeyHasherWrapper::GetHashKey(ft_string, true, key2.data(), key2.size(), hashKey2);
    KVTableSearcher::InputHashKeys inputHashKeys;
    inputHashKeys.emplace_back(make_pair<int, vector<uint64_t>>(0, {hashKey1, hashKey2}));
    auto lookupResult = kvSearcher->lookup(inputHashKeys, options.get());
    ASSERT_TRUE(lookupResult.is_ok()) << lookupResult.get_error().get_stack_trace();

    std::vector<autil::StringView> values = sortedValues(lookupResult.get().foundValues);
    auto dataResult = kvSearcher->convertResult(0, values, options.get());
    ASSERT_TRUE(dataResult.is_ok()) << dataResult.get_error().get_stack_trace();
    auto table = std::make_shared<table::Table>(_pool);
    table->deserializeFromString(dataResult.get(), _pool.get());
    ASSERT_EQ(1, table->getRowCount());
    ASSERT_EQ(2, table->getColumnCount());
    ASSERT_NO_FATAL_FAILURE(TableTestUtil::checkOutputColumn<int32_t>(table, "int_field", {2}));
    ASSERT_NO_FATAL_FAILURE(TableTestUtil::checkOutputColumn<float>(table, "float_field", {3.3}));
}

TEST_F(KVTableSearcherTest, TestLookupFieldError) {
    auto tablet = createTablet("test_table");
    ASSERT_TRUE(tablet);
    auto kvSearcher = prepareKvTabletSearcher(tablet);
    ASSERT_TRUE(kvSearcher);

    // field not exist
    {
        vector<string> attrs{"int_field", "not_exist_field"};
        auto options = makeLookupOptions(attrs);
        string key1 = "pk0";
        string key2 = "pk1";
        indexlib::dictkey_t hashKey1 = 0;
        indexlib::dictkey_t hashKey2 = 0;
        indexlib::index::KeyHasherWrapper::GetHashKey(ft_string, true, key1.data(), key1.size(), hashKey1);
        indexlib::index::KeyHasherWrapper::GetHashKey(ft_string, true, key2.data(), key2.size(), hashKey2);
        KVTableSearcher::InputHashKeys inputHashKeys;
        inputHashKeys.emplace_back(make_pair<int, vector<uint64_t>>(0, {hashKey1, hashKey2}));
        auto lookupResult = kvSearcher->lookup(inputHashKeys, options.get());
        ASSERT_TRUE(lookupResult.is_ok());

        std::vector<autil::StringView> values = sortedValues(lookupResult.get().foundValues);
        auto dataResult = kvSearcher->convertResult(0, values, options.get());
        ASSERT_FALSE(dataResult.is_ok()) << dataResult.get_error().get_stack_trace();
    }

    // field empty
    {
        vector<string> attrs{};
        auto options = makeLookupOptions(attrs);
        string key1 = "pk0";
        string key2 = "pk1";
        indexlib::dictkey_t hashKey1 = 0;
        indexlib::dictkey_t hashKey2 = 0;
        indexlib::index::KeyHasherWrapper::GetHashKey(ft_string, true, key1.data(), key1.size(), hashKey1);
        indexlib::index::KeyHasherWrapper::GetHashKey(ft_string, true, key2.data(), key2.size(), hashKey2);
        KVTableSearcher::InputHashKeys inputHashKeys;
        inputHashKeys.emplace_back(make_pair<int, vector<uint64_t>>(0, {hashKey1, hashKey2}));
        auto lookupResult = kvSearcher->lookup(inputHashKeys, options.get());
        ASSERT_TRUE(lookupResult.is_ok());

        std::vector<autil::StringView> values = sortedValues(lookupResult.get().foundValues);
        auto dataResult = kvSearcher->convertResult(0, values, options.get());
        ASSERT_TRUE(dataResult.is_ok());
        auto table = std::make_shared<table::Table>(_pool);
        table->deserializeFromString(dataResult.get(), _pool.get());
        ASSERT_EQ(2, table->getRowCount());
        ASSERT_EQ(0, table->getColumnCount());
    }
}

TEST_F(KVTableSearcherTest, TestLookupMultiValueField) {
    auto tablet = createTablet("test_table");
    ASSERT_TRUE(tablet);
    auto kvSearcher = prepareKvTabletSearcher(tablet);
    ASSERT_TRUE(kvSearcher);

    vector<string> attrs{
        "multi_int_field", "multi_double_field", "multi_string_field", "multi_int_fix_count", "multi_float_fix_count"};
    auto options = makeLookupOptions(attrs);
    string key1 = "pk0";
    string key2 = "pk1";
    indexlib::dictkey_t hashKey1 = 0;
    indexlib::dictkey_t hashKey2 = 0;
    indexlib::index::KeyHasherWrapper::GetHashKey(ft_string, true, key1.data(), key1.size(), hashKey1);
    indexlib::index::KeyHasherWrapper::GetHashKey(ft_string, true, key2.data(), key2.size(), hashKey2);
    KVTableSearcher::InputHashKeys inputHashKeys;
    inputHashKeys.emplace_back(make_pair<int, vector<uint64_t>>(0, {hashKey1, hashKey2}));
    auto lookupResult = kvSearcher->lookup(inputHashKeys, options.get());
    ASSERT_TRUE(lookupResult.is_ok()) << lookupResult.get_error().get_stack_trace();
    std::vector<autil::StringView> values = sortedValues(lookupResult.get().foundValues);
    auto dataResult = kvSearcher->convertResult(0, values, options.get());
    ASSERT_TRUE(dataResult.is_ok()) << dataResult.get_error().get_stack_trace();
    auto table = std::make_shared<table::Table>(_pool);
    table->deserializeFromString(dataResult.get(), _pool.get());
    ASSERT_EQ(2, table->getRowCount());
    ASSERT_EQ(5, table->getColumnCount());
    ASSERT_NO_FATAL_FAILURE(TableTestUtil::checkOutputMultiColumn<int>(table, "multi_int_field", {{11, 12}, {41, 42}}));
    ASSERT_NO_FATAL_FAILURE(TableTestUtil::checkOutputMultiColumn<double>(
        table, "multi_double_field", {{22.1, 22.2, 22.3}, {66.1, 66.2, 66.3}}));
    ASSERT_NO_FATAL_FAILURE(TableTestUtil::checkOutputMultiColumn<string>(
        table, "multi_string_field", {{"str00", "str01", "str02", "str03"}, {"str10", "str11", "str12", "str13"}}));
    ASSERT_NO_FATAL_FAILURE(TableTestUtil::checkOutputMultiColumn<int>(
        table, "multi_int_fix_count", {{9, 10, 9, 10, 8}, {29, 20, 29, 20, 28}}));
    ASSERT_NO_FATAL_FAILURE(TableTestUtil::checkOutputMultiColumn<float>(
        table, "multi_float_fix_count", {{9.1, 9.2, 9.3, 9.4, 9.5, 9.6}, {29.1, 29.2, 29.3, 29.4, 29.5, 29.6}}));
}

TEST_F(KVTableSearcherTest, TestPoolDestory) {
    auto tablet = createTablet("test_table");
    ASSERT_TRUE(tablet);
    auto kvSearcher = prepareKvTabletSearcher(tablet);
    ASSERT_TRUE(kvSearcher);

    vector<string> attrs{"string_field", "multi_string_field"};
    auto options = makeLookupOptions(attrs);
    string key1 = "pk0";
    string key2 = "pk1";
    indexlib::dictkey_t hashKey1 = 0;
    indexlib::dictkey_t hashKey2 = 0;
    indexlib::index::KeyHasherWrapper::GetHashKey(ft_string, true, key1.data(), key1.size(), hashKey1);
    indexlib::index::KeyHasherWrapper::GetHashKey(ft_string, true, key2.data(), key2.size(), hashKey2);
    KVTableSearcher::InputHashKeys inputHashKeys;
    inputHashKeys.emplace_back(make_pair<int, vector<uint64_t>>(0, {hashKey1, hashKey2}));
    auto lookupResult = kvSearcher->lookup(inputHashKeys, options.get());
    ASSERT_TRUE(lookupResult.is_ok()) << lookupResult.get_error().get_stack_trace();
    std::vector<autil::StringView> values = sortedValues(lookupResult.get().foundValues);

    auto dataResult = kvSearcher->convertResult(0, values, options.get());
    ASSERT_TRUE(dataResult.is_ok()) << dataResult.get_error().get_stack_trace();

    // query pool destruct, still could read result
    _pool->clear();
    auto table = std::make_shared<table::Table>(_pool);
    table->deserializeFromString(dataResult.get(), _pool.get());
    ASSERT_EQ(2, table->getRowCount());
    ASSERT_EQ(2, table->getColumnCount());
    ASSERT_NO_FATAL_FAILURE(TableTestUtil::checkOutputColumn<string>(table, "string_field", {"str0", "str1"}));
    ASSERT_NO_FATAL_FAILURE(TableTestUtil::checkOutputMultiColumn<string>(
        table, "multi_string_field", {{"str00", "str01", "str02", "str03"}, {"str10", "str11", "str12", "str13"}}));
}

} // namespace suez
