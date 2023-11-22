#include "autil/HashAlgorithm.h"
#include "autil/MultiValueType.h"
#include "autil/StringUtil.h"
#include "indexlib/index/common/KeyHasherWrapper.h"
#include "indexlib/table/kv_table/test/KVTableTestHelper.h"
#include "indexlib/table/kv_table/test/KVTabletSchemaMaker.h"
#include "indexlib/util/KeyHasherTyped.h"
#include "suez/sdk/SearchInitParam.h"
#include "suez/sdk/SuezTabletPartitionData.h"
#include "suez/service/TableServiceImpl.h"
#include "table/TableUtil.h"
#include "table/test/TableTestUtil.h"
#include "unittest/unittest.h"
using namespace std;
using namespace table;
using namespace google::protobuf;

namespace suez {
class TableServiceKVBatchGetTest : public TESTBASE {

public:
    std::shared_ptr<indexlibv2::framework::ITablet> createTablet(const std::string &tableName,
                                                                 const std::string &fieldNames,
                                                                 const std::string &keyName,
                                                                 const std::string &valueNames,
                                                                 const std::string &docStr,
                                                                 bool useNumberHash = true);
    std::vector<PartitionId> createPartIds(const std::string &tableName, const int partCount);
    std::shared_ptr<TableServiceImpl> initTableService(std::shared_ptr<indexlibv2::framework::ITablet> tablets,
                                                       std::vector<PartitionId> partIds);

public:
    void setUp();

protected:
    std::unique_ptr<indexlibv2::table::KVTableTestHelper> _helper;
    MultiTableReader _multiReader;
    IndexProvider _provider;
    std::unique_ptr<future_lite::Executor> _executor;
};

std::vector<PartitionId> TableServiceKVBatchGetTest::createPartIds(const std::string &tableName, const int partCount) {
    int step = 65535 / partCount;
    std::vector<PartitionId> partIds;
    for (auto i = 0; i < partCount; i++) {
        PartitionId part;
        part.tableId.fullVersion = 1;
        part.tableId.tableName = tableName;
        part.from = i * step;
        part.to = (i + 1) * step - 1;
        part.index = i;
        partIds.emplace_back(part);
    }
    partIds.back().to = 65535;
    return partIds;
}

std::shared_ptr<indexlibv2::framework::ITablet> TableServiceKVBatchGetTest::createTablet(const std::string &tableName,
                                                                                         const std::string &fieldNames,
                                                                                         const std::string &keyName,
                                                                                         const std::string &valueNames,
                                                                                         const std::string &docStr,
                                                                                         bool useNumberHash) {
    auto schemaPtr =
        indexlibv2::table::KVTabletSchemaMaker::Make(fieldNames, keyName, valueNames, -1, "", false, useNumberHash);
    schemaPtr->TEST_SetTableName(tableName);
    const string path = GET_TEMP_DATA_PATH() + "kv_" + tableName;
    indexlibv2::framework::IndexRoot indexRoot(path, path);
    unique_ptr<indexlibv2::config::TabletOptions> tabletOptions = make_unique<indexlibv2::config::TabletOptions>();
    tabletOptions->SetIsOnline(true);
    _helper = std::make_unique<indexlibv2::table::KVTableTestHelper>();
    auto status = _helper->Open(indexRoot, schemaPtr, move(tabletOptions));
    EXPECT_TRUE(status.IsOK());
    auto tablet = _helper->GetITablet();
    status = _helper->Build(docStr);
    EXPECT_TRUE(status.IsOK());
    return tablet;
}

void TableServiceKVBatchGetTest::setUp() {
    _executor = future_lite::ExecutorCreator::Create(
        "async_io", future_lite::ExecutorCreator::Parameters().SetExecutorName("test").SetThreadNum(2));
}

std::shared_ptr<TableServiceImpl>
TableServiceKVBatchGetTest::initTableService(std::shared_ptr<indexlibv2::framework::ITablet> tablet,
                                             std::vector<PartitionId> partIds) {
    for (auto i = 0; i < partIds.size(); i++) {
        SuezPartitionDataPtr partitionData = std::make_shared<SuezTabletPartitionData>(partIds[i], i, tablet, true);
        SingleTableReaderPtr singleReader(new SingleTableReader(partitionData));
        _multiReader.addTableReader(partIds[i], singleReader);
    }
    _provider.multiTableReader = _multiReader;
    auto tableServiceImpl = make_shared<TableServiceImpl>();
    tableServiceImpl->_executor = _executor.get();
    tableServiceImpl->setIndexProvider(make_shared<IndexProvider>(_provider));
    return tableServiceImpl;
}

TEST_F(TableServiceKVBatchGetTest, testInt64KeyWithoutNumberHash) {
    auto tableName = "test";
    auto fieldNames = "key:int64;weights:float:true:3";
    auto keyName = "key";
    auto valueName = "weights:true";
    auto docStr = "cmd=add,key=1,weights=1.1 1.2 1.3;"
                  "cmd=add,key=3,weights=1.4 1.5 1.6;";

    auto partIds = createPartIds(tableName, 2);
    auto tablet = createTablet(tableName, fieldNames, keyName, valueName, docStr, false);
    auto tableServiceImpl = initTableService(tablet, partIds);

    KVBatchGetRequest request;
    KVBatchGetResponse response;

    string key1 = "1";
    string key2 = "2";
    string key3 = "3";
    indexlib::dictkey_t hashKey1 = 0;
    indexlib::dictkey_t hashKey2 = 0;
    indexlib::dictkey_t hashKey3 = 0;
    indexlib::index::KeyHasherWrapper::GetHashKey(ft_int64, false, key1.data(), key1.size(), hashKey1);
    indexlib::index::KeyHasherWrapper::GetHashKey(ft_int64, false, key2.data(), key2.size(), hashKey2);
    indexlib::index::KeyHasherWrapper::GetHashKey(ft_int64, false, key3.data(), key3.size(), hashKey3);

    request.set_tablename(tableName);
    request.set_usehashkey(true);
    auto inputKey0 = request.add_inputkeys();
    inputKey0->add_hashkeys(hashKey1);
    inputKey0->add_hashkeys(hashKey2);
    inputKey0->set_partid(0);

    auto inputKey1 = request.add_inputkeys();
    inputKey1->add_hashkeys(hashKey3);
    inputKey1->set_partid(1);

    tableServiceImpl->kvBatchGet(nullptr, &request, &response, nullptr);
    auto errorInfo = response.errorinfo();
    auto value = response.values();
    auto foundKeys = response.foundkeys();
    std::map<uint64_t, int> idx;
    for (int i = 0; i < foundKeys.size(); ++i) {
        idx[foundKeys[i]] = i;
    }
    ASSERT_EQ(TBS_ERROR_NONE, errorInfo.errorcode()) << errorInfo.errormsg();
    ASSERT_EQ(2, foundKeys.size());
    ASSERT_TRUE(idx.count(hashKey1));
    ASSERT_TRUE(idx.count(hashKey3));

    ASSERT_EQ(2, value.size());
    autil::MultiFloat value0(value[idx[hashKey1]].data(), value[idx[hashKey1]].size() / sizeof(float)); // key = 1
    ASSERT_EQ(float(1.1), value0[0]);
    ASSERT_EQ(float(1.2), value0[1]);
    ASSERT_EQ(float(1.3), value0[2]);

    autil::MultiFloat value1(value[idx[hashKey3]].data(), value[idx[hashKey3]].size() / sizeof(float)); // key = 3
    ASSERT_EQ(float(1.4), value1[0]);
    ASSERT_EQ(float(1.5), value1[1]);
    ASSERT_EQ(float(1.6), value1[2]);
}

TEST_F(TableServiceKVBatchGetTest, testNumberHash) {
    auto tableName = "test";
    auto fieldNames = "key:int64;weights:float:true:3";
    auto keyName = "key";
    auto valueName = "weights:true";
    auto docStr = "cmd=add,key=1,weights=1.1 1.2 1.3;"
                  "cmd=add,key=3,weights=1.4 1.5 1.6;";

    auto partIds = createPartIds(tableName, 2);
    auto tablet = createTablet(tableName, fieldNames, keyName, valueName, docStr, true);
    auto tableServiceImpl = initTableService(tablet, partIds);

    KVBatchGetRequest request;
    KVBatchGetResponse response;

    string key1 = "1";
    string key2 = "2";
    string key3 = "3";
    indexlib::dictkey_t hashKey1 = 0;
    indexlib::dictkey_t hashKey2 = 0;
    indexlib::dictkey_t hashKey3 = 0;
    indexlib::index::KeyHasherWrapper::GetHashKey(ft_int64, true, key1.data(), key1.size(), hashKey1);
    indexlib::index::KeyHasherWrapper::GetHashKey(ft_int64, true, key2.data(), key2.size(), hashKey2);
    indexlib::index::KeyHasherWrapper::GetHashKey(ft_int64, true, key3.data(), key3.size(), hashKey3);
    request.set_tablename(tableName);
    request.set_usehashkey(true);
    auto inputKey0 = request.add_inputkeys();
    inputKey0->add_hashkeys(hashKey1);
    inputKey0->add_hashkeys(hashKey2);
    inputKey0->set_partid(0);

    auto inputKey1 = request.add_inputkeys();
    inputKey1->add_hashkeys(hashKey3);
    inputKey1->set_partid(1);

    tableServiceImpl->kvBatchGet(nullptr, &request, &response, nullptr);
    auto errorInfo = response.errorinfo();
    auto value = response.values();
    auto foundKeys = response.foundkeys();
    std::map<uint64_t, int> idx;
    for (int i = 0; i < foundKeys.size(); ++i) {
        idx[foundKeys[i]] = i;
    }
    ASSERT_EQ(TBS_ERROR_NONE, errorInfo.errorcode()) << errorInfo.errormsg();
    ASSERT_EQ(2, foundKeys.size());
    ASSERT_TRUE(idx.count(hashKey1));
    ASSERT_TRUE(idx.count(hashKey3));

    ASSERT_EQ(2, value.size());
    autil::MultiFloat value0(value[idx[hashKey1]].data(), value[idx[hashKey1]].size() / sizeof(float)); // key = 1
    ASSERT_EQ(float(1.1), value0[0]);
    ASSERT_EQ(float(1.2), value0[1]);
    ASSERT_EQ(float(1.3), value0[2]);

    autil::MultiFloat value1(value[idx[hashKey3]].data(), value[idx[hashKey3]].size() / sizeof(float)); // key = 3
    ASSERT_EQ(float(1.4), value1[0]);
    ASSERT_EQ(float(1.5), value1[1]);
    ASSERT_EQ(float(1.6), value1[2]);
}

TEST_F(TableServiceKVBatchGetTest, testFloat) {
    auto tableName = "test";
    auto fieldNames = "key:string;weights:float:true:3";
    auto keyName = "key";
    auto valueName = "weights:true";
    auto docStr = "cmd=add,key=k1,weights=1.1 1.2 1.3;"
                  "cmd=add,key=k3,weights=1.4 1.5 1.6;";

    auto partIds = createPartIds(tableName, 2);
    auto tablet = createTablet(tableName, fieldNames, keyName, valueName, docStr);
    auto tableServiceImpl = initTableService(tablet, partIds);

    KVBatchGetRequest request;
    KVBatchGetResponse response;

    string key1 = "k1"; // 11953212285221140034 part0
    string key2 = "k2"; // not found
    string key3 = "k3"; // 8605405837331291877 part1
    auto hashKey1 = autil::MurmurHash::MurmurHash64A(key1.data(), key1.size(), indexlib::util::MurmurHasher::SEED);
    auto hashKey2 = autil::MurmurHash::MurmurHash64A(key2.data(), key2.size(), indexlib::util::MurmurHasher::SEED);
    auto hashKey3 = autil::MurmurHash::MurmurHash64A(key3.data(), key3.size(), indexlib::util::MurmurHasher::SEED);

    request.set_tablename(tableName);
    request.set_usehashkey(true);
    auto inputKey0 = request.add_inputkeys();
    inputKey0->add_hashkeys(hashKey1); // find
    inputKey0->add_hashkeys(hashKey2); // not find
    inputKey0->set_partid(0);

    auto inputKey1 = request.add_inputkeys();
    inputKey1->add_hashkeys(hashKey3); // find
    inputKey1->set_partid(1);

    tableServiceImpl->kvBatchGet(nullptr, &request, &response, nullptr);
    auto errorInfo = response.errorinfo();
    auto value = response.values();
    auto foundKeys = response.foundkeys();
    std::map<uint64_t, int> idx;
    for (int i = 0; i < foundKeys.size(); ++i) {
        idx[foundKeys[i]] = i;
    }
    ASSERT_EQ(TBS_ERROR_NONE, errorInfo.errorcode()) << errorInfo.errormsg();
    ASSERT_EQ(2, foundKeys.size());
    ASSERT_TRUE(idx.count(hashKey3));
    ASSERT_TRUE(idx.count(hashKey1));

    ASSERT_EQ(2, value.size());
    autil::MultiFloat value0(value[idx[hashKey3]].data(), value[idx[hashKey3]].size() / sizeof(float)); // key = k3
    ASSERT_EQ(float(1.4), value0[0]);
    ASSERT_EQ(float(1.5), value0[1]);
    ASSERT_EQ(float(1.6), value0[2]);
    autil::MultiFloat value1(value[idx[hashKey1]].data(), value[idx[hashKey1]].size() / sizeof(float)); // key = k1
    ASSERT_EQ(float(1.1), value1[0]);
    ASSERT_EQ(float(1.2), value1[1]);
    ASSERT_EQ(float(1.3), value1[2]);
}

TEST_F(TableServiceKVBatchGetTest, testDouble) {
    auto tableName = "test";
    auto fieldNames = "key:string;weights:double:true:3";
    auto keyName = "key";
    auto valueName = "weights:true";
    auto docStr = "cmd=add,key=k1,weights=1.1 1.2 1.3;"
                  "cmd=add,key=k3,weights=1.4 1.5 1.6;";
    auto partIds = createPartIds(tableName, 2);
    auto tablet = createTablet(tableName, fieldNames, keyName, valueName, docStr);
    auto tableServiceImpl = initTableService(tablet, partIds);

    KVBatchGetRequest request;
    KVBatchGetResponse response;

    string key1 = "k1"; // 11953212285221140034 --> 63042 --> part1
    string key2 = "k3"; // 8605405837331291877 --> 24293 --> part0
    auto hashKey1 = autil::MurmurHash::MurmurHash64A(key1.data(), key1.size(), indexlib::util::MurmurHasher::SEED);
    auto hashKey2 = autil::MurmurHash::MurmurHash64A(key2.data(), key2.size(), indexlib::util::MurmurHasher::SEED);

    request.set_tablename(tableName);
    request.set_usehashkey(true);
    auto inputKey1 = request.add_inputkeys();
    inputKey1->add_hashkeys(hashKey1); // find
    inputKey1->set_partid(0);

    auto inputKey0 = request.add_inputkeys();
    inputKey0->add_hashkeys(hashKey2); // find
    inputKey0->set_partid(1);

    tableServiceImpl->kvBatchGet(nullptr, &request, &response, nullptr);
    auto errorInfo = response.errorinfo();
    auto value = response.values();
    auto foundKeys = response.foundkeys();
    std::map<uint64_t, int> idx;
    for (int i = 0; i < foundKeys.size(); ++i) {
        idx[foundKeys[i]] = i;
    }
    ASSERT_EQ(TBS_ERROR_NONE, errorInfo.errorcode()) << errorInfo.errormsg();
    ASSERT_EQ(2, foundKeys.size());
    ASSERT_TRUE(idx.count(hashKey2));
    ASSERT_TRUE(idx.count(hashKey1));
    autil::MultiDouble value0(value[idx[hashKey2]].data(), value[idx[hashKey2]].size() / sizeof(double));
    ASSERT_EQ(double(1.4), value0[0]);
    ASSERT_EQ(double(1.5), value0[1]);
    ASSERT_EQ(double(1.6), value0[2]);
    autil::MultiDouble value1(value[idx[hashKey1]].data(), value[idx[hashKey1]].size() / sizeof(double));
    ASSERT_EQ(double(1.1), value1[0]);
    ASSERT_EQ(double(1.2), value1[1]);
    ASSERT_EQ(double(1.3), value1[2]);
}

TEST_F(TableServiceKVBatchGetTest, testInt8) {
    auto tableName = "test";
    auto fieldNames = "key:string;weights:int8:true:3";
    auto keyName = "key";
    auto valueName = "weights:true";
    auto docStr = "cmd=add,key=k1,weights=1 2 3;"
                  "cmd=add,key=k3,weights=4 5 6;";

    auto partIds = createPartIds(tableName, 2);
    auto tablet = createTablet(tableName, fieldNames, keyName, valueName, docStr);
    auto tableServiceImpl = initTableService(tablet, partIds);

    KVBatchGetRequest request;
    KVBatchGetResponse response;

    string key1 = "k1"; // 11953212285221140034
    string key2 = "k3"; // 8605405837331291877
    auto hashKey1 = autil::MurmurHash::MurmurHash64A(key1.data(), key1.size(), indexlib::util::MurmurHasher::SEED);
    auto hashKey2 = autil::MurmurHash::MurmurHash64A(key2.data(), key2.size(), indexlib::util::MurmurHasher::SEED);

    request.set_tablename(tableName);
    auto inputKey1 = request.add_inputkeys();
    inputKey1->add_hashkeys(hashKey1); // find
    inputKey1->set_partid(0);

    auto inputKey0 = request.add_inputkeys();
    inputKey0->add_hashkeys(hashKey2); // find
    inputKey0->set_partid(1);

    tableServiceImpl->kvBatchGet(nullptr, &request, &response, nullptr);
    auto errorInfo = response.errorinfo();
    auto value = response.values();
    auto foundKeys = response.foundkeys();
    std::map<uint64_t, int> idx;
    for (int i = 0; i < foundKeys.size(); ++i) {
        idx[foundKeys[i]] = i;
    }
    ASSERT_EQ(TBS_ERROR_NONE, errorInfo.errorcode()) << errorInfo.errormsg();
    ASSERT_EQ(2, foundKeys.size());
    ASSERT_TRUE(idx.count(hashKey2));
    ASSERT_TRUE(idx.count(hashKey1));
    autil::MultiInt8 value0(value[idx[hashKey2]].data(), value[idx[hashKey2]].size() / sizeof(int8_t));
    ASSERT_EQ(int8_t(4), value0[0]);
    ASSERT_EQ(int8_t(5), value0[1]);
    ASSERT_EQ(int8_t(6), value0[2]);
    autil::MultiInt8 value1(value[idx[hashKey1]].data(), value[idx[hashKey1]].size() / sizeof(int8_t));
    ASSERT_EQ(int8_t(1), value1[0]);
    ASSERT_EQ(int8_t(2), value1[1]);
    ASSERT_EQ(int8_t(3), value1[2]);
}

TEST_F(TableServiceKVBatchGetTest, testUInt16) {
    auto tableName = "test2";
    auto fieldNames = "key:string;weights:uint16:true:3";
    auto keyName = "key";
    auto valueName = "weights:true";
    auto docStr = "cmd=add,key=k1,weights=1 2 3;"
                  "cmd=add,key=k3,weights=4 5 6;";

    auto partIds = createPartIds(tableName, 2);
    auto tablet = createTablet(tableName, fieldNames, keyName, valueName, docStr);
    auto tableServiceImpl = initTableService(tablet, partIds);

    KVBatchGetRequest request;
    KVBatchGetResponse response;

    string key1 = "k1"; // 11953212285221140034
    string key2 = "k3"; // 8605405837331291877
    auto hashKey1 = autil::MurmurHash::MurmurHash64A(key1.data(), key1.size(), indexlib::util::MurmurHasher::SEED);
    auto hashKey2 = autil::MurmurHash::MurmurHash64A(key2.data(), key2.size(), indexlib::util::MurmurHasher::SEED);

    request.set_tablename(tableName);
    auto inputKey1 = request.add_inputkeys();
    inputKey1->add_hashkeys(hashKey1); // find
    inputKey1->set_partid(0);

    auto inputKey0 = request.add_inputkeys();
    inputKey0->add_hashkeys(hashKey2); // find
    inputKey0->set_partid(1);

    tableServiceImpl->kvBatchGet(nullptr, &request, &response, nullptr);
    auto errorInfo = response.errorinfo();
    auto value = response.values();
    auto foundKeys = response.foundkeys();
    std::map<uint64_t, int> idx;
    for (int i = 0; i < foundKeys.size(); ++i) {
        idx[foundKeys[i]] = i;
    }
    ASSERT_EQ(TBS_ERROR_NONE, errorInfo.errorcode()) << errorInfo.errormsg();
    ASSERT_EQ(2, foundKeys.size());
    ASSERT_TRUE(idx.count(hashKey2));
    ASSERT_TRUE(idx.count(hashKey1));
    autil::MultiUInt16 value0(value[idx[hashKey2]].data(), value[idx[hashKey2]].size() / sizeof(uint16_t));
    ASSERT_EQ(uint16_t(4), value0[0]);
    ASSERT_EQ(uint16_t(5), value0[1]);
    ASSERT_EQ(uint16_t(6), value0[2]);
    autil::MultiUInt16 value1(value[idx[hashKey1]].data(), value[idx[hashKey1]].size() / sizeof(uint16_t));
    ASSERT_EQ(uint16_t(1), value1[0]);
    ASSERT_EQ(uint16_t(2), value1[1]);
    ASSERT_EQ(uint16_t(3), value1[2]);
}

// 下面几个测试返回table形式
TEST_F(TableServiceKVBatchGetTest, testSimpleQuery) {
    auto tableName = "test2";
    auto fieldNames = "key:string;weights:float:true:3";
    auto keyName = "key";
    auto valueName = "key;weights:true";
    auto docStr = "cmd=add,key=k1,weights=1 2 3;"
                  "cmd=add,key=k3,weights=4 5 6;";

    auto partIds = createPartIds(tableName, 2);
    auto tablet = createTablet(tableName, fieldNames, keyName, valueName, docStr);
    auto tableServiceImpl = initTableService(tablet, partIds);

    KVBatchGetRequest request;
    KVBatchGetResponse response;

    string key1 = "k1"; // 11953212285221140034
    string key2 = "k3"; // 8605405837331291877
    request.set_tablename(tableName);
    request.set_usehashkey(false);
    auto inputKey1 = request.add_inputkeys();
    inputKey1->add_keys(key1);
    inputKey1->set_partid(0);

    auto inputKey2 = request.add_inputkeys();
    inputKey2->add_keys(key2);
    inputKey2->set_partid(1);

    request.set_indexname("key");
    request.add_attrs("key");
    request.add_attrs("weights");
    request.set_resulttype(KVBatchGetResultType::TABLE);
    tableServiceImpl->kvBatchGet(nullptr, &request, &response, nullptr);
    auto errorInfo = response.errorinfo();
    auto value = response.binvalues();
    auto valueStr = std::string(value.data(), value.size());
    ASSERT_EQ(TBS_ERROR_NONE, errorInfo.errorcode()) << errorInfo.errormsg();
    auto pool = autil::mem_pool::PoolPtr(new autil::mem_pool::Pool(1024));
    auto table = std::make_shared<table::Table>(pool);
    table->deserializeFromString(valueStr, pool.get());
    ASSERT_EQ(2, table->getRowCount());
    ASSERT_EQ(2, table->getColumnCount());

    int idx = 0;
    auto *column = table->getColumn(0);
    if (column->toString(0) == "k1") {
        idx = 1;
    }
    ASSERT_EQ("k3", column->toString(idx));
    ASSERT_EQ("k1", column->toString(1 - idx));

    column = table->getColumn(1);
    ASSERT_EQ("4^]5^]6", column->toString(idx));
    ASSERT_EQ("1^]2^]3", column->toString(1 - idx));
}

TEST_F(TableServiceKVBatchGetTest, testEmptyQuery) {
    auto tableName = "test2";
    auto fieldNames = "key:string;weights:float:true:3";
    auto keyName = "key";
    auto valueName = "weights:true";
    auto docStr = "cmd=add,key=k1,weights=1 2 3;"
                  "cmd=add,key=k3,weights=4 5 6;";

    auto partIds = createPartIds(tableName, 2);
    auto tablet = createTablet(tableName, fieldNames, keyName, valueName, docStr);
    auto tableServiceImpl = initTableService(tablet, partIds);

    KVBatchGetRequest request;
    KVBatchGetResponse response;

    string key1 = "k1"; // 11953212285221140034
    string key2 = "k3"; // 8605405837331291877

    request.set_tablename(tableName);
    auto inputKey = request.add_inputkeys();
    inputKey->add_keys("");
    inputKey->set_partid(0);

    request.set_indexname(keyName);
    request.add_attrs("weights");
    request.set_resulttype(KVBatchGetResultType::TABLE);
    tableServiceImpl->kvBatchGet(nullptr, &request, &response, nullptr);

    auto errorInfo = response.errorinfo();
    auto value = response.binvalues();
    auto valueStr = std::string(value.data(), value.size());
    ASSERT_EQ(TBS_ERROR_NONE, errorInfo.errorcode()) << errorInfo.errormsg();
    auto pool = autil::mem_pool::PoolPtr(new autil::mem_pool::Pool(1024));
    auto table = std::make_shared<table::Table>(pool);
    table->deserializeFromString(valueStr, pool.get());
    ASSERT_EQ(0, table->getRowCount());
    ASSERT_EQ(1, table->getColumnCount());
}

TEST_F(TableServiceKVBatchGetTest, testQueryError) {
    auto tableName = "test2";
    auto fieldNames = "key:string;weights:float:true:3";
    auto keyName = "key";
    auto valueName = "weights:true";
    auto docStr = "cmd=add,key=k1,weights=1 2 3;"
                  "cmd=add,key=k3,weights=4 5 6;";

    auto partIds = createPartIds(tableName, 2);
    auto tablet = createTablet(tableName, fieldNames, keyName, valueName, docStr);
    auto tableServiceImpl = initTableService(tablet, partIds);

    string key1 = "k1"; // 11953212285221140034 part1
    string key2 = "k3"; // 8605405837331291877 part0

    // no async executor
    {
        tableServiceImpl->_executor = nullptr;
        KVBatchGetRequest request;
        request.set_tablename(tableName);
        auto inputKey1 = request.add_inputkeys();
        inputKey1->add_keys(key1);
        inputKey1->set_partid(0);

        auto inputKey0 = request.add_inputkeys();
        inputKey0->add_keys(key2);
        inputKey0->set_partid(1);

        request.set_indexname(keyName);
        request.add_attrs("weights");
        KVBatchGetResponse response;
        tableServiceImpl->kvBatchGet(nullptr, &request, &response, nullptr);
        auto errorInfo = response.errorinfo();
        ASSERT_EQ(TBS_ERROR_OTHERS, errorInfo.errorcode()) << errorInfo.errormsg();
    }
}

TEST_F(TableServiceKVBatchGetTest, testFlatValues) {
    auto tableName = "test";
    auto fieldNames = "key:string;weights:float:true:3";
    auto keyName = "key";
    auto valueName = "weights:true";
    auto docStr = "cmd=add,key=k1,weights=1.1 1.2 1.3;"
                  "cmd=add,key=k3,weights=1.4 1.5 1.6;";

    auto partIds = createPartIds(tableName, 2);
    auto tablet = createTablet(tableName, fieldNames, keyName, valueName, docStr);
    auto tableServiceImpl = initTableService(tablet, partIds);

    KVBatchGetRequest request;
    KVBatchGetResponse response;

    string key1 = "k1"; // 11953212285221140034 part0
    string key2 = "k2"; // not found
    string key3 = "k3"; // 8605405837331291877 part1
    auto hashKey1 = autil::MurmurHash::MurmurHash64A(key1.data(), key1.size(), indexlib::util::MurmurHasher::SEED);
    auto hashKey2 = autil::MurmurHash::MurmurHash64A(key2.data(), key2.size(), indexlib::util::MurmurHasher::SEED);
    auto hashKey3 = autil::MurmurHash::MurmurHash64A(key3.data(), key3.size(), indexlib::util::MurmurHasher::SEED);
    request.set_resulttype(::suez::KVBatchGetResultType::FLATBYTES);
    request.set_tablename(tableName);
    request.set_usehashkey(true);
    auto inputKey0 = request.add_inputkeys();
    inputKey0->add_hashkeys(hashKey1); // find
    inputKey0->add_hashkeys(hashKey2); // not find
    inputKey0->set_partid(0);

    auto inputKey1 = request.add_inputkeys();
    inputKey1->add_hashkeys(hashKey3); // find
    inputKey1->set_partid(1);

    tableServiceImpl->kvBatchGet(nullptr, &request, &response, nullptr);
    auto errorInfo = response.errorinfo();
    auto value = response.values();
    auto foundKeys = response.foundkeys();
    std::map<uint64_t, int> idx;
    for (int i = 0; i < foundKeys.size(); ++i) {
        idx[foundKeys[i]] = i;
    }
    ASSERT_EQ(TBS_ERROR_NONE, errorInfo.errorcode()) << errorInfo.errormsg();
    ASSERT_EQ(2, foundKeys.size());
    ASSERT_TRUE(idx.count(hashKey3));
    ASSERT_TRUE(idx.count(hashKey1));

    ASSERT_EQ(0, value.size());
    ASSERT_TRUE(response.has_binvalues());
    ASSERT_EQ(24, response.binvalues().size());
    autil::MultiFloat value0(response.binvalues().data() + idx[hashKey3] * 12, 3); // key = k3
    ASSERT_EQ(float(1.4), value0[0]);
    ASSERT_EQ(float(1.5), value0[1]);
    ASSERT_EQ(float(1.6), value0[2]);
    autil::MultiFloat value1(response.binvalues().data() + idx[hashKey1] * 12, 3); // key = k1
    ASSERT_EQ(float(1.1), value1[0]);
    ASSERT_EQ(float(1.2), value1[1]);
    ASSERT_EQ(float(1.3), value1[2]);
}

TEST_F(TableServiceKVBatchGetTest, testDebug) {
    auto tableName = "test";
    auto fieldNames = "key:string;weights:float:true:3";
    auto keyName = "key";
    auto valueName = "weights:true";
    auto docStr = "cmd=add,key=k1,weights=1.1 1.2 1.3;"
                  "cmd=add,key=k3,weights=1.4 1.5 1.6;";

    auto partIds = createPartIds(tableName, 2);
    auto tablet = createTablet(tableName, fieldNames, keyName, valueName, docStr);
    auto tableServiceImpl = initTableService(tablet, partIds);

    KVBatchGetRequest request;
    KVBatchGetResponse response;

    string key1 = "k1"; // 11953212285221140034 part0
    string key3 = "k3"; // 8605405837331291877 part1

    request.set_tablename(tableName);
    request.set_usehashkey(false);
    request.set_resulttype(KVBatchGetResultType::DEBUG);
    request.add_attrs("weights");
    auto inputKey0 = request.add_inputkeys();
    inputKey0->add_keys(key1);
    inputKey0->set_partid(0);

    auto inputKey1 = request.add_inputkeys();
    inputKey1->add_keys(key3);
    inputKey1->set_partid(1);

    tableServiceImpl->kvBatchGet(nullptr, &request, &response, nullptr);
    auto errorInfo = response.errorinfo();
    ASSERT_EQ(TBS_ERROR_NONE, errorInfo.errorcode()) << errorInfo.errormsg();
    auto value0 = response.values()[0].to_string();
    auto value1 = response.values()[1].to_string();
    std::string result0 = R"({
"match_count":
  1,
"match_docs":
  [
    {
    "weights":
      "1.11.21.3"
    }
  ]
})";
    std::string result1 = R"({
"match_count":
  1,
"match_docs":
  [
    {
    "weights":
      "1.41.51.6"
    }
  ]
})";
    ASSERT_EQ(result0, value0);
    ASSERT_EQ(result1, value1);
}

} // namespace suez
