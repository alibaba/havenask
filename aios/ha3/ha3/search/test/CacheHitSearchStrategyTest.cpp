#include<unittest/unittest.h>
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/search/CacheHitSearchStrategy.h>
#include <ha3/config/JoinConfig.h>
#include <ha3_sdk/testlib/index/FakeIndexPartitionReaderCreator.h>
#include <indexlib/partition/index_application.h>
#include <indexlib/testlib/indexlib_partition_creator.h>
#include <indexlib/storage/file_system_wrapper.h>
#include <ha3_sdk/testlib/qrs/RequestCreator.h>
#include <autil/StringUtil.h>

using namespace std;
using namespace autil;
IE_NAMESPACE_USE(index);
IE_NAMESPACE_USE(partition);
IE_NAMESPACE_USE(storage);
IE_NAMESPACE_USE(testlib);

USE_HA3_NAMESPACE(index);
USE_HA3_NAMESPACE(common);
USE_HA3_NAMESPACE(config);
USE_HA3_NAMESPACE(qrs);

BEGIN_HA3_NAMESPACE(search);

class CacheHitSearchStrategyTest : public TESTBASE {
public:
    CacheHitSearchStrategyTest();
    ~CacheHitSearchStrategyTest();
public:
    void setUp();
    void tearDown();
protected:
    common::ResultPtr prepareResult();
    void checkRefreshedValues(common::Result *result,
                              const std::string &attrNamesStr,
                              const std::string &attrValuesStr);
protected:
    IE_NAMESPACE(partition)::PartitionReaderSnapshotPtr _snapshotPtr;
    IE_NAMESPACE(partition)::IndexApplication::IndexPartitionMap _indexPartitionMap;
    IE_NAMESPACE(partition)::IndexApplicationPtr _indexApp;
    std::string _testPath;
    common::Ha3MatchDocAllocatorPtr _matchDocAllocator;
    autil::mem_pool::Pool *_pool;
    const std::string _tableName = "mainTable";
    const std::string _joinTableName = "joinTable";
protected:
    HA3_LOG_DECLARE();
};
HA3_LOG_SETUP(search, CacheHitSearchStrategyTest);


CacheHitSearchStrategyTest::CacheHitSearchStrategyTest() {
    _pool = NULL;
}

CacheHitSearchStrategyTest::~CacheHitSearchStrategyTest() {
}

void CacheHitSearchStrategyTest::setUp() {
    HA3_LOG(DEBUG, "setUp!");
    _pool = new mem_pool::Pool;
    _matchDocAllocator.reset(new common::Ha3MatchDocAllocator(_pool));
    _testPath = GET_TEMP_DATA_PATH() + "searcher_catch_test_path/";
    if (!_testPath.empty()) {
        FileSystemWrapper::DeleteIfExist(_testPath);
        FileSystemWrapper::DeleteIfExist(_testPath + _tableName );
    }
    auto schema = IndexlibPartitionCreator::CreateSchema(_tableName,
            "pk:int32;joinid:uint32;price1:uint64;price2:uint64;"
            "price3:uint64;price5:uint64;join_price:uint64", // fields
            "pk:primarykey64:pk;", //indexs
            "joinid;price1;price2;price3;price5;", //attributes
            "", // summarys
            ""); // truncateProfile
    string docStrs = "cmd=add,pk=0,joinid=1,price1=2,price2=2,price3=1,price5=10;"
                     "cmd=add,pk=1,joinid=2,price1=3,price2=4,price3=2,price5=20;"
                     "cmd=add,pk=2,joinid=3,price1=4,price2=6,price3=3,price5=30;"
                     "cmd=add,pk=3,joinid=4,price1=5,price2=8,price3=4,price5=40;"
                     "cmd=add,pk=4,joinid=5,price1=6,price2=10,price3=5,price5=50;"
                     "cmd=add,pk=5,joinid=6,price1=7,price2=1,price3=5,price5=60;"
                     "cmd=add,pk=6,joinid=7,price1=8,price2=3,price3=4,price5=70;";
    auto indexPartition = IndexlibPartitionCreator::CreateIndexPartition(
            schema, _testPath + _tableName, docStrs);
    string schemaName = indexPartition->GetSchema()->GetSchemaName();

    auto joinSchema = IndexlibPartitionCreator::CreateSchema(_joinTableName,
            "pk:uint32;join_price:uint64", // fields
            "pk:primarykey64:pk;", // indexs
            "join_price;", // attributes
            "", // summarys
            ""); //truncateProfile

    string joinStrs = "cmd=add,pk=0,join_price=100;"
                      "cmd=add,pk=1,join_price=200;"
                      "cmd=add,pk=2,join_price=300;"
                      "cmd=add,pk=-1,join_price=400;"
                      "cmd=add,pk=4,join_price=500;";
    auto joinIndexPartition = IndexlibPartitionCreator::CreateIndexPartition(
            joinSchema, _testPath + _joinTableName, joinStrs);
    string joinSchemaName = joinIndexPartition->GetSchema()->GetSchemaName();

    _indexPartitionMap.insert(make_pair(schemaName, indexPartition));
    _indexPartitionMap.insert(make_pair(joinSchemaName, joinIndexPartition));
    JoinRelationMap joinRelations;
    joinRelations[_tableName] = { JoinField(
                "joinid", _joinTableName, true, true) };
    _indexApp.reset(new IndexApplication);
    _indexApp->Init(_indexPartitionMap, joinRelations);
    _snapshotPtr = _indexApp->CreateSnapshot();
}

void CacheHitSearchStrategyTest::tearDown() {
    HA3_LOG(DEBUG, "tearDown!");
    _matchDocAllocator.reset();
    DELETE_AND_SET_NULL(_pool);
    _snapshotPtr.reset();
    _indexPartitionMap.clear();
    _indexApp.reset();
    if (!_testPath.empty()) {
        FileSystemWrapper::DeleteIfExist(_testPath);
        FileSystemWrapper::DeleteIfExist(_testPath + _tableName);
        FileSystemWrapper::DeleteIfExist(_testPath + _joinTableName);
    }
}

TEST_F(CacheHitSearchStrategyTest, testRefreshAttributes) {
    ResultPtr result = prepareResult();
    RequestPtr request = RequestCreator::prepareRequest("config=cluster:" + _tableName + "&&searcher_cache=key:1,refresh_attributes:price1;price2;price5;join_price");
    DocCountLimits docCountLimits;

    CacheHitSearchStrategy processor(docCountLimits, request.get(),
            NULL, NULL, _pool, _tableName, _snapshotPtr.get());

    processor.refreshAttributes(result.get());

    checkRefreshedValues(result.get(), "price1;price2;price3;join_price", "2,3,4,5,6;2,4,6,8,10;0,1,2,3,4;200,300,0,500,0");
}

ResultPtr CacheHitSearchStrategyTest::prepareResult() {
    Result* result = new Result;
    MatchDocs *matchDocs = new MatchDocs;
    result->setMatchDocs(matchDocs);
    result->setTotalMatchDocs(100);
    result->setActualMatchDocs(100);
    auto ref1 = _matchDocAllocator->declare<uint64_t>("price1");
    auto ref2 = _matchDocAllocator->declare<uint64_t>("price2");
    auto ref3 = _matchDocAllocator->declare<uint64_t>("price3");
    auto ref4 = _matchDocAllocator->declare<uint64_t>("price4");
    auto ref5 = _matchDocAllocator->declare<uint64_t>("join_price");
    _matchDocAllocator->extend();
    for (uint32_t i = 0; i < 5; i++) {
        matchdoc::MatchDoc matchDoc = _matchDocAllocator->allocate(i);
        ref1->set(matchDoc, i);
        ref2->set(matchDoc, i);
        ref3->set(matchDoc, i);
        ref4->set(matchDoc, i);
        ref5->set(matchDoc, i);
        matchDocs->addMatchDoc(matchDoc);
    }
    common::Ha3MatchDocAllocatorPtr matchDocAllocatorPtr(_matchDocAllocator);
    matchDocs->setMatchDocAllocator(matchDocAllocatorPtr);
    return ResultPtr(result);
}

void CacheHitSearchStrategyTest::checkRefreshedValues(
        Result *result, const string &attrNamesStr, const string &attrValuesStr)
{
    MatchDocs *matchDocs = result->getMatchDocs();
    vector<string> attrNames;
    StringUtil::fromString(attrNamesStr, attrNames, ";");
    vector<vector<uint64_t> > attrValues;
    StringUtil::fromString(attrValuesStr, attrValues, ",", ";");

    assert(attrNames.size() == attrValues.size());
    for (size_t i = 0; i < attrNames.size(); i++) {
        auto ref = _matchDocAllocator->findReference<uint64_t>(attrNames[i]);
        if (!(ref != NULL)) {
            HA3_LOG(ERROR, "string(reference for %s)  does not exist", attrNames[i].c_str());
            assert(false);
        }
        assert((size_t)matchDocs->size() == attrValues[i].size());
        for (uint32_t j = 0; j < matchDocs->size(); j++) {
            matchdoc::MatchDoc matchDoc = matchDocs->getMatchDoc(j);
            uint64_t value = ref->getReference(matchDoc);
            ASSERT_EQ(attrValues[i][j], value);
        }
    }
}

END_HA3_NAMESPACE(search);
