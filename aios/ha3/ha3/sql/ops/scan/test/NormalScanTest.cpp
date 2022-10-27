#include <ha3/sql/ops/test/OpTestBase.h>
#include <ha3/sql/ops/scan/NormalScan.h>
#include <ha3/turing/ops/SqlSessionResource.h>
#include <build_service/analyzer/AnalyzerFactory.h>
#include <build_service/analyzer/AnalyzerInfos.h>
#include <build_service/analyzer/TokenizerManager.h>
#include <ha3/common/TermQuery.h>
#include <ha3/sql/data/TableUtil.h>
#include <matchdoc/MatchDocAllocator.h>
#include <matchdoc/SubDocAccessor.h>

using namespace std;
using namespace suez::turing;
using namespace matchdoc;
using namespace build_service::analyzer;
using namespace autil;
using namespace autil::legacy;
using namespace autil::legacy::json;

USE_HA3_NAMESPACE(common);

BEGIN_HA3_NAMESPACE(sql);

class NormalScanTest : public OpTestBase {
public:
    NormalScanTest();
    ~NormalScanTest();
public:
    void setUp() override {
        _needBuildIndex = true;
        _needExprResource = true;
    }
    void tearDown() override {
    }
    void prepareUserIndex() override {
        prepareInvertedTable();
    }
    void prepareInvertedTable() override {
        string tableName = _tableName;
        std::string testPath = _testPath + tableName;
        auto indexPartition = makeIndexPartition(testPath, tableName);
        std::string schemaName = indexPartition->GetSchema()->GetSchemaName();
        _bizInfo->_itemTableName = schemaName;
        _indexPartitionMap.insert(make_pair(schemaName, indexPartition));
    }

private:
    void prepareAttributeMap() {
        _attributeMap.clear();
        _attributeMap["table_type"] = string("normal");
        _attributeMap["table_name"] = _tableName;
        _attributeMap["db_name"] = string("default");
        _attributeMap["catalog_name"] = string("default");
        _attributeMap["output_fields"] = ParseJson(string(R"json(["$attr1", "$attr2", "$id"])json"));
        _attributeMap["use_nest_table"] = Any(false);
        _attributeMap["limit"] = Any(1000);
    }
    void checkExpr(const vector<suez::turing::AttributeExpression *> &attributeExpressionVec,
                   const vector<string> &exprStrVec) {
        ASSERT_EQ(attributeExpressionVec.size(), exprStrVec.size());
        for (size_t i = 0; i < exprStrVec.size(); i++) {
            ASSERT_EQ(attributeExpressionVec[i]->getOriginalString(), exprStrVec[i]);
        }
    }

    IE_NAMESPACE(partition)::IndexPartitionPtr makeIndexPartition(const std::string &rootPath,
            const std::string &tableName)
    {
        int64_t ttl = std::numeric_limits<int64_t>::max();
        auto mainSchema = IE_NAMESPACE(testlib)::IndexlibPartitionCreator::CreateSchema(
                tableName,
                "attr1:uint32;attr2:int32:true;id:int64;name:string;index2:text", //fields
                "id:primarykey64:id;index_2:text:index2;name:string:name", //fields
                "attr1;attr2;id", //attributes
                "name", // summary
                "");// truncateProfile

        auto subSchema = IE_NAMESPACE(testlib)::IndexlibPartitionCreator::CreateSchema(
                "sub_" + tableName,
                "sub_id:int64;sub_attr1:int32;sub_index2:string", // fields
                "sub_id:primarykey64:sub_id;sub_index_2:string:sub_index2", //indexs
                "sub_attr1;sub_id;sub_index2", //attributes
                "", // summarys
                ""); // truncateProfile
        string docsStr = "cmd=add,attr1=0,attr2=0 10,id=1,name=aa,index2=a a a,sub_id=1^2^3,sub_attr1=1^2^2,sub_index2=aa^ab^ac;"
                         "cmd=add,attr1=1,attr2=1 11,id=2,name=bb,index2=a b c,sub_id=4,sub_attr1=1,sub_index2=aa;"
                         "cmd=add,attr1=2,attr2=2 22,id=3,name=cc,index2=a c d stop,sub_id=5,sub_attr1=1,sub_index2=ab;"
                         "cmd=add,attr1=3,attr2=3 33,id=4,name=dd,index2=b c d,sub_id=6^7,sub_attr1=2^1,sub_index2=abc^ab";

        IE_NAMESPACE(config)::IndexPartitionOptions options;
        options.GetOnlineConfig().ttl = ttl;
        auto schema = IE_NAMESPACE(testlib)::IndexlibPartitionCreator::CreateSchemaWithSub(mainSchema, subSchema);

        IE_NAMESPACE(partition)::IndexPartitionPtr indexPartition =
            IE_NAMESPACE(testlib)::IndexlibPartitionCreator::CreateIndexPartition(
                    schema, rootPath, docsStr, options, "", true);
        assert(indexPartition.get() != nullptr);
        return indexPartition;
    }

private:
    autil::legacy::json::JsonMap _attributeMap;
};

NormalScanTest::NormalScanTest() {
}

NormalScanTest::~NormalScanTest() {
}

TEST_F(NormalScanTest, testInit) {
    { // init ouput failed
        autil::legacy::json::JsonMap attributeMap;
        attributeMap["table_type"] = string("normal");
        attributeMap["table_name"] = _tableName;
        attributeMap["db_name"] = string("default");
        attributeMap["catalog_name"] = string("default");
        attributeMap["output_fields"] = ParseJson(string(R"json(["$attr1", "$attr2", "$id"])json"));
        attributeMap["hash_fields"] = ParseJson(string(R"json(["id"])json"));
        attributeMap["output_field_exprs"] = ParseJson(R"json({"$attr1":{"op":"+", "paramsxxx":["$attr1", 1]}})json");
        string jsonStr = autil::legacy::ToJsonString(attributeMap);
        JsonMap jsonMap;
        FromJsonString(jsonMap, jsonStr);
        autil::legacy::Jsonizable::JsonWrapper wrapper(jsonMap);
        ScanInitParam param;
        ASSERT_TRUE(param.initFromJson(wrapper));
        param.bizResource = _sqlBizResource.get();
        param.queryResource = _sqlQueryResource.get();
        param.memoryPoolResource = &_memPoolResource;
        NormalScan normalScan;
        ASSERT_FALSE(normalScan.init(param));
    }
    { // create iterator failed
        autil::legacy::json::JsonMap attributeMap;
        attributeMap["table_type"] = string("normal");
        attributeMap["table_name"] = _tableName;
        attributeMap["db_name"] = string("default");
        attributeMap["catalog_name"] = string("default");
        attributeMap["hash_fields"] = ParseJson(string(R"json(["id"])json"));
        attributeMap["output_fields"] = ParseJson(string(R"json(["$attr1", "$attr2", "$id"])json"));
        attributeMap["condition"] = ParseJson(R"json({"op":"LIKE", "paramsxxx":["$index2", "a"]})json");
        string jsonStr = autil::legacy::ToJsonString(attributeMap);
        JsonMap jsonMap;
        FromJsonString(jsonMap, jsonStr);
        autil::legacy::Jsonizable::JsonWrapper wrapper(jsonMap);
        ScanInitParam param;
        ASSERT_TRUE(param.initFromJson(wrapper));
        param.bizResource = _sqlBizResource.get();
        param.queryResource = _sqlQueryResource.get();
        param.memoryPoolResource = &_memPoolResource;
        NormalScan normalScan;
        ASSERT_FALSE(normalScan.init(param));
    }
    { // default
        autil::legacy::json::JsonMap attributeMap;
        attributeMap["table_type"] = string("normal");
        attributeMap["table_name"] = _tableName;
        attributeMap["db_name"] = string("default");
        attributeMap["catalog_name"] = string("default");
        attributeMap["hash_fields"] = ParseJson(string(R"json(["id"])json"));
        attributeMap["output_fields"] = ParseJson(string(R"json(["$attr1"])json"));
        string jsonStr = autil::legacy::ToJsonString(attributeMap);
        JsonMap jsonMap;
        FromJsonString(jsonMap, jsonStr);
        autil::legacy::Jsonizable::JsonWrapper wrapper(jsonMap);
        ScanInitParam param;
        ASSERT_TRUE(param.initFromJson(wrapper));
        param.bizResource = _sqlBizResource.get();
        param.queryResource = _sqlQueryResource.get();
        param.memoryPoolResource = &_memPoolResource;
        NormalScan normalScan;
        ASSERT_TRUE(normalScan.init(param));
        ASSERT_EQ(1<<18, normalScan._batchSize);
    }
    { // default with sub
        autil::legacy::json::JsonMap attributeMap;
        attributeMap["table_type"] = string("normal");
        attributeMap["table_name"] = _tableName;
        attributeMap["db_name"] = string("default");
        attributeMap["catalog_name"] = string("default");
        attributeMap["hash_fields"] = ParseJson(string(R"json(["id"])json"));
        attributeMap["use_nest_table"] = true;
        attributeMap["output_fields"] = ParseJson(string(R"json(["$attr1"])json"));
        string jsonStr = autil::legacy::ToJsonString(attributeMap);
        JsonMap jsonMap;
        FromJsonString(jsonMap, jsonStr);
        autil::legacy::Jsonizable::JsonWrapper wrapper(jsonMap);
        ScanInitParam param;
        ASSERT_TRUE(param.initFromJson(wrapper));
        param.bizResource = _sqlBizResource.get();
        param.queryResource = _sqlQueryResource.get();
        param.memoryPoolResource = &_memPoolResource;
        NormalScan normalScan;
        ASSERT_TRUE(normalScan.init(param));
    }
    { // default with sub
        autil::legacy::json::JsonMap attributeMap;
        attributeMap["table_type"] = string("normal");
        attributeMap["table_name"] = _tableName;
        attributeMap["db_name"] = string("default");
        attributeMap["catalog_name"] = string("default");
        attributeMap["hash_fields"] = ParseJson(string(R"json(["id"])json"));
        attributeMap["use_nest_table"] = true;
        attributeMap["output_fields"] = ParseJson(string(R"json(["$attr1", "$sub_attr1"])json"));
        string jsonStr = autil::legacy::ToJsonString(attributeMap);
        JsonMap jsonMap;
        FromJsonString(jsonMap, jsonStr);
        autil::legacy::Jsonizable::JsonWrapper wrapper(jsonMap);
        ScanInitParam param;
        ASSERT_TRUE(param.initFromJson(wrapper));
        param.bizResource = _sqlBizResource.get();
        param.queryResource = _sqlQueryResource.get();
        param.memoryPoolResource = &_memPoolResource;
        NormalScan normalScan;
        ASSERT_TRUE(normalScan.init(param));
    }

    { // batch size 100
        autil::legacy::json::JsonMap attributeMap;
        attributeMap["table_type"] = string("normal");
        attributeMap["table_name"] = _tableName;
        attributeMap["db_name"] = string("default");
        attributeMap["catalog_name"] = string("default");
        attributeMap["hash_fields"] = ParseJson(string(R"json(["id"])json"));
        attributeMap["output_fields"] = ParseJson(string(R"json(["$attr1"])json"));
        attributeMap["batch_size"] = Any(100);
        string jsonStr = autil::legacy::ToJsonString(attributeMap);
        JsonMap jsonMap;
        FromJsonString(jsonMap, jsonStr);
        autil::legacy::Jsonizable::JsonWrapper wrapper(jsonMap);
        ScanInitParam param;
        ASSERT_TRUE(param.initFromJson(wrapper));
        param.bizResource = _sqlBizResource.get();
        param.queryResource = _sqlQueryResource.get();
        param.memoryPoolResource = &_memPoolResource;
        NormalScan normalScan;
        ASSERT_TRUE(normalScan.init(param));
        ASSERT_EQ(100, normalScan._batchSize);
    }
}

TEST_F(NormalScanTest, testDoBatchScan) {
    { // one batch
        autil::legacy::json::JsonMap attributeMap;
        attributeMap["table_type"] = string("normal");
        attributeMap["table_name"] = _tableName;
        attributeMap["db_name"] = string("default");
        attributeMap["catalog_name"] = string("default");
        attributeMap["hash_fields"] = ParseJson(string(R"json(["id"])json"));
        attributeMap["output_fields"] = ParseJson(string(R"json(["$attr1"])json"));
        string jsonStr = autil::legacy::ToJsonString(attributeMap);
        JsonMap jsonMap;
        FromJsonString(jsonMap, jsonStr);
        autil::legacy::Jsonizable::JsonWrapper wrapper(jsonMap);
        ScanInitParam param;
        ASSERT_TRUE(param.initFromJson(wrapper));
        param.bizResource = _sqlBizResource.get();
        param.queryResource = _sqlQueryResource.get();
        param.memoryPoolResource = &_memPoolResource;
        NormalScan normalScan;
        ASSERT_TRUE(normalScan.init(param));
        TablePtr table;
        bool eof = false;
        ASSERT_TRUE(normalScan.doBatchScan(table, eof));
        ASSERT_TRUE(table != nullptr);
        EXPECT_EQ("{}", normalScan._outputExprsJson);
        ASSERT_EQ(4, table->getRowCount());
        ASSERT_EQ("0", table->toString(0, 0));
        ASSERT_EQ("1", table->toString(1, 0));
        ASSERT_EQ("2", table->toString(2, 0));
        ASSERT_EQ("3", table->toString(3, 0));
        ASSERT_TRUE(eof);
        ASSERT_EQ(4, normalScan._seekCount);
        ASSERT_EQ(4, normalScan._scanInfo.totalscancount());
        table.reset();
        eof = false;
        ASSERT_TRUE(normalScan.doBatchScan(table, eof));
        ASSERT_TRUE(table != nullptr);
        ASSERT_EQ(0, table->getRowCount());
        ASSERT_TRUE(eof);
        ASSERT_EQ(4, normalScan._seekCount);
        ASSERT_EQ(4, normalScan._scanInfo.totalscancount());
    }
    { // limit 3
        autil::legacy::json::JsonMap attributeMap;
        attributeMap["table_type"] = string("normal");
        attributeMap["table_name"] = _tableName;
        attributeMap["db_name"] = string("default");
        attributeMap["catalog_name"] = string("default");
        attributeMap["hash_fields"] = ParseJson(string(R"json(["id"])json"));
        attributeMap["output_fields"] = ParseJson(string(R"json(["$attr1"])json"));
        attributeMap["limit"] = Any(3);
        string jsonStr = autil::legacy::ToJsonString(attributeMap);
        JsonMap jsonMap;
        FromJsonString(jsonMap, jsonStr);
        autil::legacy::Jsonizable::JsonWrapper wrapper(jsonMap);
        ScanInitParam param;
        ASSERT_TRUE(param.initFromJson(wrapper));
        param.bizResource = _sqlBizResource.get();
        param.queryResource = _sqlQueryResource.get();
        param.memoryPoolResource = &_memPoolResource;
        NormalScan normalScan;
        ASSERT_TRUE(normalScan.init(param));
        TablePtr table;
        bool eof = false;
        ASSERT_TRUE(normalScan.doBatchScan(table, eof));
        ASSERT_TRUE(table != nullptr);
        ASSERT_EQ(3, table->getRowCount());
        ASSERT_EQ("0", table->toString(0, 0));
        ASSERT_EQ("1", table->toString(1, 0));
        ASSERT_EQ("2", table->toString(2, 0));

        ASSERT_TRUE(eof);
        ASSERT_EQ(3, normalScan._seekCount);
        ASSERT_EQ(3, normalScan._scanInfo.totalscancount());
        table.reset();
        eof = false;
        ASSERT_TRUE(normalScan.doBatchScan(table, eof));
        ASSERT_TRUE(table != nullptr);
        ASSERT_EQ(0, table->getRowCount());
        ASSERT_TRUE(eof);
        ASSERT_EQ(3, normalScan._seekCount);
        ASSERT_EQ(3, normalScan._scanInfo.totalscancount());
    }
    { // limit 2, seek more than 2, need delete doc
        autil::legacy::json::JsonMap attributeMap;
        attributeMap["table_type"] = string("normal");
        attributeMap["table_name"] = _tableName;
        attributeMap["db_name"] = string("default");
        attributeMap["catalog_name"] = string("default");
        attributeMap["hash_fields"] = ParseJson(string(R"json(["id"])json"));
        attributeMap["output_fields"] = ParseJson(string(R"json(["$attr1"])json"));
        attributeMap["condition"] = ParseJson(R"json({"op":"!=", "params":["$attr1", 1]})json");
        attributeMap["limit"] = Any(2);
        string jsonStr = autil::legacy::ToJsonString(attributeMap);
        JsonMap jsonMap;
        FromJsonString(jsonMap, jsonStr);
        autil::legacy::Jsonizable::JsonWrapper wrapper(jsonMap);
        ScanInitParam param;
        ASSERT_TRUE(param.initFromJson(wrapper));
        param.bizResource = _sqlBizResource.get();
        param.queryResource = _sqlQueryResource.get();
        param.memoryPoolResource = &_memPoolResource;
        NormalScan normalScan;
        ASSERT_TRUE(normalScan.init(param));
        TablePtr table;
        bool eof = false;
        ASSERT_TRUE(normalScan.doBatchScan(table, eof));
        ASSERT_TRUE(table != nullptr);
        ASSERT_EQ(2, table->getRowCount());
        ASSERT_EQ("0", table->toString(0, 0));
        ASSERT_EQ("2", table->toString(1, 0));
        ASSERT_TRUE(eof);
        ASSERT_EQ(3, normalScan._seekCount);
        ASSERT_EQ(4, normalScan._scanInfo.totalscancount());
    }
    { // batch size 2
        autil::legacy::json::JsonMap attributeMap;
        attributeMap["table_type"] = string("normal");
        attributeMap["table_name"] = _tableName;
        attributeMap["db_name"] = string("default");
        attributeMap["catalog_name"] = string("default");
        attributeMap["hash_fields"] = ParseJson(string(R"json(["id"])json"));
        attributeMap["output_fields"] = ParseJson(string(R"json(["$attr1"])json"));
        attributeMap["batch_size"] = Any(2);
        string jsonStr = autil::legacy::ToJsonString(attributeMap);
        JsonMap jsonMap;
        FromJsonString(jsonMap, jsonStr);
        autil::legacy::Jsonizable::JsonWrapper wrapper(jsonMap);
        ScanInitParam param;
        ASSERT_TRUE(param.initFromJson(wrapper));
        param.bizResource = _sqlBizResource.get();
        param.queryResource = _sqlQueryResource.get();
        param.memoryPoolResource = &_memPoolResource;
        NormalScan normalScan;
        ASSERT_TRUE(normalScan.init(param));
        TablePtr table;
        bool eof = false;
        ASSERT_TRUE(normalScan.doBatchScan(table, eof));
        ASSERT_TRUE(table != nullptr);
        ASSERT_EQ(2, table->getRowCount());
        ASSERT_EQ("0", table->toString(0, 0));
        ASSERT_EQ("1", table->toString(1, 0));
        ASSERT_FALSE(eof);
        ASSERT_EQ(0, normalScan._scanInfo.totalscancount());
        ASSERT_EQ(2, normalScan._seekCount);

        ASSERT_TRUE(normalScan.doBatchScan(table, eof));
        ASSERT_TRUE(table != nullptr);
        ASSERT_EQ(2, table->getRowCount());
        ASSERT_EQ("2", table->toString(0, 0));
        ASSERT_EQ("3", table->toString(1, 0));
        ASSERT_FALSE(eof);
        ASSERT_EQ(0, normalScan._scanInfo.totalscancount());
        ASSERT_EQ(4, normalScan._seekCount);

        ASSERT_TRUE(normalScan.doBatchScan(table, eof));
        ASSERT_TRUE(table != nullptr);
        ASSERT_EQ(0, table->getRowCount());
        ASSERT_EQ(4, normalScan._seekCount);
        ASSERT_EQ(4, normalScan._scanInfo.totalscancount());
    }
    { // batch size 2 limit 3
        autil::legacy::json::JsonMap attributeMap;
        attributeMap["table_type"] = string("normal");
        attributeMap["table_name"] = _tableName;
        attributeMap["db_name"] = string("default");
        attributeMap["catalog_name"] = string("default");
        attributeMap["hash_fields"] = ParseJson(string(R"json(["id"])json"));
        attributeMap["output_fields"] = ParseJson(string(R"json(["$attr1"])json"));
        attributeMap["batch_size"] = Any(2);
        attributeMap["limit"] = Any(3);
        string jsonStr = autil::legacy::ToJsonString(attributeMap);
        JsonMap jsonMap;
        FromJsonString(jsonMap, jsonStr);
        autil::legacy::Jsonizable::JsonWrapper wrapper(jsonMap);
        ScanInitParam param;
        ASSERT_TRUE(param.initFromJson(wrapper));
        param.bizResource = _sqlBizResource.get();
        param.queryResource = _sqlQueryResource.get();
        param.memoryPoolResource = &_memPoolResource;
        NormalScan normalScan;
        ASSERT_TRUE(normalScan.init(param));
        TablePtr table;
        bool eof = false;
        ASSERT_TRUE(normalScan.doBatchScan(table, eof));
        ASSERT_TRUE(table != nullptr);
        ASSERT_EQ(2, table->getRowCount());
        ASSERT_EQ("0", table->toString(0, 0));
        ASSERT_EQ("1", table->toString(1, 0));
        ASSERT_FALSE(eof);
        ASSERT_EQ(0, normalScan._scanInfo.totalscancount());
        ASSERT_EQ(2, normalScan._seekCount);

        ASSERT_TRUE(normalScan.doBatchScan(table, eof));
        ASSERT_TRUE(table != nullptr);
        ASSERT_EQ(1, table->getRowCount());
        ASSERT_EQ("2", table->toString(0, 0));
        ASSERT_TRUE(eof);
        ASSERT_EQ(3, normalScan._seekCount);
        ASSERT_EQ(3, normalScan._scanInfo.totalscancount());
    }
}

TEST_F(NormalScanTest, testDoBatchScanWithSub) {
    { // one batch
        autil::legacy::json::JsonMap attributeMap;
        attributeMap["table_type"] = string("normal");
        attributeMap["table_name"] = _tableName;
        attributeMap["db_name"] = string("default");
        attributeMap["catalog_name"] = string("default");
        attributeMap["hash_fields"] = ParseJson(string(R"json(["id"])json"));
        attributeMap["nest_table_attrs"] = ParseJson(string(R"json([{"table_name":"sub_invertedTable"}])json"));
        attributeMap["use_nest_table"] = true;
        attributeMap["output_fields"] = ParseJson(string(R"json(["$attr1", "$sub_attr1"])json"));
        string jsonStr = autil::legacy::ToJsonString(attributeMap);
        JsonMap jsonMap;
        FromJsonString(jsonMap, jsonStr);
        autil::legacy::Jsonizable::JsonWrapper wrapper(jsonMap);
        ScanInitParam param;
        ASSERT_TRUE(param.initFromJson(wrapper));
        param.bizResource = _sqlBizResource.get();
        param.queryResource = _sqlQueryResource.get();
        param.memoryPoolResource = &_memPoolResource;
        NormalScan normalScan;
        ASSERT_TRUE(normalScan.init(param));
        TablePtr table;
        bool eof = false;
        ASSERT_TRUE(normalScan.doBatchScan(table, eof));
        ASSERT_TRUE(table != nullptr);

        ASSERT_EQ(2, table->getColumnCount());
        ASSERT_EQ(7, table->getRowCount());
        ASSERT_EQ("0", table->toString(0, 0));
        ASSERT_EQ("0", table->toString(1, 0));
        ASSERT_EQ("0", table->toString(2, 0));
        ASSERT_EQ("1", table->toString(3, 0));
        ASSERT_EQ("2", table->toString(4, 0));
        ASSERT_EQ("3", table->toString(5, 0));
        ASSERT_EQ("3", table->toString(6, 0));

        ASSERT_EQ("1", table->toString(0, 1));
        ASSERT_EQ("2", table->toString(1, 1));
        ASSERT_EQ("2", table->toString(2, 1));
        ASSERT_EQ("1", table->toString(3, 1));
        ASSERT_EQ("1", table->toString(4, 1));
        ASSERT_EQ("2", table->toString(5, 1));
        ASSERT_EQ("1", table->toString(6, 1));

        ASSERT_TRUE(eof);
        ASSERT_EQ(4, normalScan._seekCount);
        ASSERT_EQ(4, normalScan._scanInfo.totalscancount());
        table.reset();
        eof = false;
        ASSERT_TRUE(normalScan.doBatchScan(table, eof));
        ASSERT_TRUE(table != nullptr);
        ASSERT_EQ(0, table->getRowCount());
        ASSERT_TRUE(eof);
        ASSERT_EQ(4, normalScan._seekCount);
        ASSERT_EQ(4, normalScan._scanInfo.totalscancount());
    }
    { // sub attr only
        autil::legacy::json::JsonMap attributeMap;
        attributeMap["table_type"] = string("normal");
        attributeMap["table_name"] = _tableName;
        attributeMap["db_name"] = string("default");
        attributeMap["catalog_name"] = string("default");
        attributeMap["hash_fields"] = ParseJson(string(R"json(["id"])json"));
        attributeMap["use_nest_table"] = true;
        attributeMap["nest_table_attrs"] = ParseJson(string(R"json([{"table_name":"sub_invertedTable"}])json"));
        attributeMap["output_fields"] = ParseJson(string(R"json(["$sub_attr1"])json"));
        string jsonStr = autil::legacy::ToJsonString(attributeMap);
        JsonMap jsonMap;
        FromJsonString(jsonMap, jsonStr);
        autil::legacy::Jsonizable::JsonWrapper wrapper(jsonMap);
        ScanInitParam param;
        ASSERT_TRUE(param.initFromJson(wrapper));
        param.bizResource = _sqlBizResource.get();
        param.queryResource = _sqlQueryResource.get();
        param.memoryPoolResource = &_memPoolResource;
        NormalScan normalScan;
        ASSERT_TRUE(normalScan.init(param));
        TablePtr table;
        bool eof = false;
        ASSERT_TRUE(normalScan.doBatchScan(table, eof));
        ASSERT_TRUE(table != nullptr);

        ASSERT_EQ(1, table->getColumnCount());
        ASSERT_EQ(7, table->getRowCount());
        ASSERT_EQ("1", table->toString(0, 0));
        ASSERT_EQ("2", table->toString(1, 0));
        ASSERT_EQ("2", table->toString(2, 0));
        ASSERT_EQ("1", table->toString(3, 0));
        ASSERT_EQ("1", table->toString(4, 0));
        ASSERT_EQ("2", table->toString(5, 0));
        ASSERT_EQ("1", table->toString(6, 0));

        ASSERT_TRUE(eof);
        ASSERT_EQ(4, normalScan._seekCount);
        ASSERT_EQ(4, normalScan._scanInfo.totalscancount());
        table.reset();
        eof = false;
        ASSERT_TRUE(normalScan.doBatchScan(table, eof));
        ASSERT_TRUE(table != nullptr);
        ASSERT_EQ(0, table->getRowCount());
        ASSERT_TRUE(eof);
        ASSERT_EQ(4, normalScan._seekCount);
        ASSERT_EQ(4, normalScan._scanInfo.totalscancount());
    }
}

TEST_F(NormalScanTest, testUpdateScanQuery) {
    { //default
        autil::legacy::json::JsonMap attributeMap;
        attributeMap["table_type"] = string("normal");
        attributeMap["table_name"] = _tableName;
        attributeMap["db_name"] = string("default");
        attributeMap["catalog_name"] = string("default");
        attributeMap["hash_fields"] = ParseJson(string(R"json(["id"])json"));
        attributeMap["output_fields"] = ParseJson(string(R"json(["$attr1"])json"));
        string jsonStr = autil::legacy::ToJsonString(attributeMap);
        JsonMap jsonMap;
        FromJsonString(jsonMap, jsonStr);
        autil::legacy::Jsonizable::JsonWrapper wrapper(jsonMap);
        ScanInitParam param;
        ASSERT_TRUE(param.initFromJson(wrapper));
        param.bizResource = _sqlBizResource.get();
        param.queryResource = _sqlQueryResource.get();
        param.memoryPoolResource = &_memPoolResource;
        NormalScan normalScan;
        ASSERT_TRUE(normalScan.init(param));
        TablePtr table;
        StreamQueryPtr inputQuery(new StreamQuery());
        QueryPtr query(new TermQuery("a", "index_2", RequiredFields(), ""));
        inputQuery->query = query;
        ASSERT_TRUE(normalScan.updateScanQuery(inputQuery));
        bool eof = false;
        ASSERT_TRUE(normalScan.doBatchScan(table, eof));
        ASSERT_TRUE(table != nullptr);
        ASSERT_TRUE(eof);
        ASSERT_EQ(3, table->getRowCount());
        ASSERT_EQ("0", table->toString(0, 0));
        ASSERT_EQ("1", table->toString(1, 0));
        ASSERT_EQ("2", table->toString(2, 0));
        ASSERT_EQ(3, normalScan._seekCount);
        ASSERT_EQ(3, normalScan._scanInfo.totalscancount());

        ASSERT_TRUE(normalScan.updateScanQuery(inputQuery));
        eof = false;
        ASSERT_TRUE(normalScan.doBatchScan(table, eof));
        ASSERT_TRUE(table != nullptr);
        ASSERT_TRUE(eof);
        ASSERT_EQ(3, table->getRowCount());
        ASSERT_EQ("0", table->toString(0, 0));
        ASSERT_EQ("1", table->toString(1, 0));
        ASSERT_EQ("2", table->toString(2, 0));
        ASSERT_EQ(6, normalScan._seekCount);
        ASSERT_EQ(6, normalScan._scanInfo.totalscancount());

        ASSERT_TRUE(normalScan.updateScanQuery(StreamQueryPtr()));
        eof = false;
        ASSERT_TRUE(normalScan.doBatchScan(table, eof));
        ASSERT_TRUE(eof);
        ASSERT_EQ(0, table->getRowCount());
        ASSERT_EQ(6, normalScan._seekCount);
        ASSERT_EQ(6, normalScan._scanInfo.totalscancount());
    }
}

TEST_F(NormalScanTest, testUpdateScanQueryWithSub) {
    { //with sub attr
        autil::legacy::json::JsonMap attributeMap;
        attributeMap["table_type"] = string("normal");
        attributeMap["table_name"] = _tableName;
        attributeMap["db_name"] = string("default");
        attributeMap["catalog_name"] = string("default");
        attributeMap["hash_fields"] = ParseJson(string(R"json(["id"])json"));
        attributeMap["nest_table_attrs"] = ParseJson(string(R"json([{"table_name":"sub_invertedTable"}])json"));
        attributeMap["use_nest_table"] = Any(true);
        attributeMap["output_fields"] = ParseJson(string(R"json(["$sub_attr1", "attr1"])json"));
        string jsonStr = autil::legacy::ToJsonString(attributeMap);
        JsonMap jsonMap;
        FromJsonString(jsonMap, jsonStr);
        autil::legacy::Jsonizable::JsonWrapper wrapper(jsonMap);
        ScanInitParam param;
        ASSERT_TRUE(param.initFromJson(wrapper));
        param.bizResource = _sqlBizResource.get();
        param.queryResource = _sqlQueryResource.get();
        param.memoryPoolResource = &_memPoolResource;
        NormalScan normalScan;
        ASSERT_TRUE(normalScan.init(param));
        TablePtr table;
        StreamQueryPtr inputQuery(new StreamQuery());
        QueryPtr query(new TermQuery("a", "index_2", RequiredFields(), ""));
        inputQuery->query = query;
        ASSERT_TRUE(normalScan.updateScanQuery(inputQuery));
        bool eof = false;
        ASSERT_TRUE(normalScan.doBatchScan(table, eof));
        ASSERT_TRUE(table != nullptr);
        ASSERT_TRUE(eof);
        ASSERT_EQ(2, table->getColumnCount());
        ASSERT_EQ(5, table->getRowCount());
        ASSERT_EQ("1", table->toString(0, 0));
        ASSERT_EQ("2", table->toString(1, 0));
        ASSERT_EQ("2", table->toString(2, 0));
        ASSERT_EQ("1", table->toString(3, 0));
        ASSERT_EQ("1", table->toString(4, 0));

        ASSERT_EQ("0", table->toString(0, 1));
        ASSERT_EQ("0", table->toString(1, 1));
        ASSERT_EQ("0", table->toString(2, 1));
        ASSERT_EQ("1", table->toString(3, 1));
        ASSERT_EQ("2", table->toString(4, 1));
        ASSERT_EQ(3, normalScan._seekCount);
        ASSERT_EQ(3, normalScan._scanInfo.totalscancount());

        ASSERT_TRUE(normalScan.updateScanQuery(inputQuery));
        eof = false;
        ASSERT_TRUE(normalScan.doBatchScan(table, eof));
        ASSERT_TRUE(table != nullptr);
        ASSERT_TRUE(eof);
        ASSERT_EQ(2, table->getColumnCount());
        ASSERT_EQ(5, table->getRowCount());
        ASSERT_EQ("1", table->toString(0, 0));
        ASSERT_EQ("2", table->toString(1, 0));
        ASSERT_EQ("2", table->toString(2, 0));
        ASSERT_EQ("1", table->toString(3, 0));
        ASSERT_EQ("1", table->toString(4, 0));

        ASSERT_EQ("0", table->toString(0, 1));
        ASSERT_EQ("0", table->toString(1, 1));
        ASSERT_EQ("0", table->toString(2, 1));
        ASSERT_EQ("1", table->toString(3, 1));
        ASSERT_EQ("2", table->toString(4, 1));
    }
    { //with sub attr empty result
        autil::legacy::json::JsonMap attributeMap;
        attributeMap["table_type"] = string("normal");
        attributeMap["table_name"] = _tableName;
        attributeMap["db_name"] = string("default");
        attributeMap["catalog_name"] = string("default");
        attributeMap["hash_fields"] = ParseJson(string(R"json(["id"])json"));
        attributeMap["nest_table_attrs"] = ParseJson(string(R"json([{"table_name":"sub_invertedTable"}])json"));
        attributeMap["use_nest_table"] = Any(true);
        attributeMap["output_fields"] = ParseJson(string(R"json(["$sub_attr1", "$attr1"])json"));
        string jsonStr = autil::legacy::ToJsonString(attributeMap);
        JsonMap jsonMap;
        FromJsonString(jsonMap, jsonStr);
        autil::legacy::Jsonizable::JsonWrapper wrapper(jsonMap);
        ScanInitParam param;
        ASSERT_TRUE(param.initFromJson(wrapper));
        param.bizResource = _sqlBizResource.get();
        param.queryResource = _sqlQueryResource.get();
        param.memoryPoolResource = &_memPoolResource;
        NormalScan normalScan;
        ASSERT_TRUE(normalScan.init(param));
        TablePtr table;
        StreamQueryPtr inputQuery(new StreamQuery());
        QueryPtr query(new TermQuery("aa", "index_2", RequiredFields(), ""));
        inputQuery->query = query;
        ASSERT_TRUE(normalScan.updateScanQuery(inputQuery));
        bool eof = false;
        ASSERT_TRUE(normalScan.doBatchScan(table, eof));
        ASSERT_TRUE(table != nullptr);
        ASSERT_TRUE(eof);
        ASSERT_EQ(2, table->getColumnCount());
        ASSERT_EQ(0, table->getRowCount());
    }
}

TEST_F(NormalScanTest, testUpdateScanQueryLimitBatch) {
    { // limit 2
        autil::legacy::json::JsonMap attributeMap;
        attributeMap["table_type"] = string("normal");
        attributeMap["table_name"] = _tableName;
        attributeMap["db_name"] = string("default");
        attributeMap["catalog_name"] = string("default");
        attributeMap["hash_fields"] = ParseJson(string(R"json(["id"])json"));
        attributeMap["output_fields"] = ParseJson(string(R"json(["$attr1"])json"));
        attributeMap["limit"] = Any(2);
        string jsonStr = autil::legacy::ToJsonString(attributeMap);
        JsonMap jsonMap;
        FromJsonString(jsonMap, jsonStr);
        autil::legacy::Jsonizable::JsonWrapper wrapper(jsonMap);
        ScanInitParam param;
        ASSERT_TRUE(param.initFromJson(wrapper));
        param.bizResource = _sqlBizResource.get();
        param.queryResource = _sqlQueryResource.get();
        param.memoryPoolResource = &_memPoolResource;
        NormalScan normalScan;
        ASSERT_TRUE(normalScan.init(param));
        TablePtr table;
        StreamQueryPtr inputQuery(new StreamQuery());
        QueryPtr query(new TermQuery("a", "index_2", RequiredFields(), ""));
        inputQuery->query = query;
        ASSERT_TRUE(normalScan.updateScanQuery(inputQuery));
        bool eof = false;
        ASSERT_TRUE(normalScan.doBatchScan(table, eof));
        ASSERT_TRUE(table != nullptr);
        ASSERT_TRUE(eof);
        ASSERT_EQ(2, table->getRowCount());
        ASSERT_EQ("0", table->toString(0, 0));
        ASSERT_EQ("1", table->toString(1, 0));
        ASSERT_EQ(2, normalScan._seekCount);
        ASSERT_EQ(2, normalScan._scanInfo.totalscancount());

        ASSERT_TRUE(normalScan.updateScanQuery(inputQuery));
        eof = false;
        ASSERT_TRUE(normalScan.doBatchScan(table, eof));
        ASSERT_TRUE(table != nullptr);
        ASSERT_TRUE(eof);
        ASSERT_EQ(2, normalScan._seekCount);
        ASSERT_EQ(2, normalScan._scanInfo.totalscancount());
    }
    { //batch size 2
        autil::legacy::json::JsonMap attributeMap;
        attributeMap["table_type"] = string("normal");
        attributeMap["table_name"] = _tableName;
        attributeMap["db_name"] = string("default");
        attributeMap["catalog_name"] = string("default");
        attributeMap["hash_fields"] = ParseJson(string(R"json(["id"])json"));
        attributeMap["output_fields"] = ParseJson(string(R"json(["$attr1"])json"));
        attributeMap["batch_size"] = Any(2);
        string jsonStr = autil::legacy::ToJsonString(attributeMap);
        JsonMap jsonMap;
        FromJsonString(jsonMap, jsonStr);
        autil::legacy::Jsonizable::JsonWrapper wrapper(jsonMap);
        ScanInitParam param;
        ASSERT_TRUE(param.initFromJson(wrapper));
        param.bizResource = _sqlBizResource.get();
        param.queryResource = _sqlQueryResource.get();
        param.memoryPoolResource = &_memPoolResource;
        NormalScan normalScan;
        ASSERT_TRUE(normalScan.init(param));
        TablePtr table;
        StreamQueryPtr inputQuery(new StreamQuery());
        QueryPtr query(new TermQuery("a", "index_2", RequiredFields(), ""));
        inputQuery->query = query;
        ASSERT_TRUE(normalScan.updateScanQuery(inputQuery));
        bool eof = false;
        ASSERT_TRUE(normalScan.doBatchScan(table, eof));
        ASSERT_TRUE(table != nullptr);
        ASSERT_FALSE(eof);

        ASSERT_EQ(2, table->getRowCount());
        ASSERT_EQ("0", table->toString(0, 0));
        ASSERT_EQ("1", table->toString(1, 0));
        ASSERT_EQ(2, normalScan._seekCount);
        ASSERT_EQ(0, normalScan._scanInfo.totalscancount());

        ASSERT_TRUE(normalScan.doBatchScan(table, eof));
        ASSERT_TRUE(table != nullptr);
        ASSERT_TRUE(eof);
        ASSERT_EQ(1, table->getRowCount());
        ASSERT_EQ("2", table->toString(0, 0));
        ASSERT_EQ(3, normalScan._seekCount);
        ASSERT_EQ(3, normalScan._scanInfo.totalscancount());
    }
    { //batch size 2 limit 2
        autil::legacy::json::JsonMap attributeMap;
        attributeMap["table_type"] = string("normal");
        attributeMap["table_name"] = _tableName;
        attributeMap["db_name"] = string("default");
        attributeMap["catalog_name"] = string("default");
        attributeMap["hash_fields"] = ParseJson(string(R"json(["id"])json"));
        attributeMap["output_fields"] = ParseJson(string(R"json(["$attr1"])json"));
        attributeMap["batch_size"] = Any(2);
        attributeMap["limit"] = Any(2);
        string jsonStr = autil::legacy::ToJsonString(attributeMap);
        JsonMap jsonMap;
        FromJsonString(jsonMap, jsonStr);
        autil::legacy::Jsonizable::JsonWrapper wrapper(jsonMap);
        ScanInitParam param;
        ASSERT_TRUE(param.initFromJson(wrapper));
        param.bizResource = _sqlBizResource.get();
        param.queryResource = _sqlQueryResource.get();
        param.memoryPoolResource = &_memPoolResource;
        NormalScan normalScan;
        ASSERT_TRUE(normalScan.init(param));
        TablePtr table;
        StreamQueryPtr inputQuery(new StreamQuery());
        QueryPtr query(new TermQuery("a", "index_2", RequiredFields(), ""));
        inputQuery->query = query;
        ASSERT_TRUE(normalScan.updateScanQuery(inputQuery));
        bool eof = false;
        ASSERT_TRUE(normalScan.doBatchScan(table, eof));
        ASSERT_TRUE(table != nullptr);
        ASSERT_TRUE(eof);
        ASSERT_EQ(2, table->getRowCount());
        ASSERT_EQ("0", table->toString(0, 0));
        ASSERT_EQ("1", table->toString(1, 0));
        ASSERT_EQ(2, normalScan._seekCount);
        ASSERT_EQ(2, normalScan._scanInfo.totalscancount());
    }
}

TEST_F(NormalScanTest, testInitOutputColumnUseSubTrue) {
    auto tableInfo = TableInfoConfigurator::createFromIndexApp(_tableName, _indexApp);
    bool useSubFlag = true;
    auto indexAppSnapshot = _indexApp->CreateSnapshot();
    std::vector<suez::turing::VirtualAttribute*> virtualAttributes;
    { // output expr is empty
        MatchDocAllocatorPtr matchDocAllocator(new matchdoc::MatchDocAllocator(&_pool, useSubFlag));
        AttributeExpressionCreatorPtr attributeExpressionCreator(new AttributeExpressionCreator(
                        &_pool, matchDocAllocator.get(), _tableName, indexAppSnapshot.get(),
                        tableInfo, virtualAttributes, NULL, suez::turing::CavaPluginManagerPtr(), NULL));
        NormalScan normalScan;
        normalScan._outputFields = {"attr1", "sub_attr1"};
        normalScan._outputExprsJson = "";
        normalScan._tableInfo = tableInfo;
        normalScan._hashFields = {"id"};
        normalScan._attributeExpressionCreator = attributeExpressionCreator;
        normalScan._pool = &_pool;
        normalScan._matchDocAllocator = matchDocAllocator;
        ASSERT_TRUE(normalScan.initOutputColumn());
        ASSERT_EQ(0, normalScan._copyFieldMap.size());
        checkExpr(normalScan._attributeExpressionVec, {"attr1", "sub_attr1"});
        auto refVec = matchDocAllocator->getRefBySerializeLevel(SL_ATTRIBUTE);
        ASSERT_EQ(1, refVec.size());
        refVec = matchDocAllocator->getAllSubNeedSerializeReferences(SL_ATTRIBUTE);
        ASSERT_EQ(1, refVec.size());
    }
    { // copy field
        MatchDocAllocatorPtr matchDocAllocator(new matchdoc::MatchDocAllocator(&_pool, useSubFlag));
        AttributeExpressionCreatorPtr attributeExpressionCreator(new AttributeExpressionCreator(
                        &_pool, matchDocAllocator.get(), _tableName, indexAppSnapshot.get(),
                        tableInfo, virtualAttributes, NULL, suez::turing::CavaPluginManagerPtr(), NULL));
        NormalScan normalScan;
        normalScan._hashFields = {"id"};
        normalScan._outputFields = {"attr1", "attr1_copy", "sub_attr1", "sub_attr1_copy" };
        autil::legacy::json::ToString(ParseJson(R"json({"$attr1_copy":"$attr1", "$sub_attr1_copy":"$sub_attr1"})json"), normalScan._outputExprsJson);
        normalScan._tableInfo = tableInfo;
        normalScan._attributeExpressionCreator = attributeExpressionCreator;
        normalScan._matchDocAllocator = matchDocAllocator;
        normalScan._pool = &_pool;
        ASSERT_TRUE(normalScan.initOutputColumn());
        ASSERT_EQ(2, normalScan._copyFieldMap.size());
        ASSERT_EQ("attr1", normalScan._copyFieldMap["attr1_copy"]);
        ASSERT_EQ("sub_attr1", normalScan._copyFieldMap["sub_attr1_copy"]);
        checkExpr(normalScan._attributeExpressionVec, {"attr1", "sub_attr1"});
        auto refVec = matchDocAllocator->getRefBySerializeLevel(SL_ATTRIBUTE);
        ASSERT_EQ(3, refVec.size());
        refVec = matchDocAllocator->getAllSubNeedSerializeReferences(SL_ATTRIBUTE);
        ASSERT_EQ(1, refVec.size());
    }
    { // const field
        MatchDocAllocatorPtr matchDocAllocator(new matchdoc::MatchDocAllocator(&_pool, useSubFlag));
        AttributeExpressionCreatorPtr attributeExpressionCreator(new AttributeExpressionCreator(
                        &_pool, matchDocAllocator.get(), _tableName, indexAppSnapshot.get(),
                        tableInfo, virtualAttributes, NULL, suez::turing::CavaPluginManagerPtr(), NULL));
        NormalScan normalScan;
        normalScan._hashFields = {"id"};
        normalScan._outputFields = {"attr1", "$f0", "$f1","$f2"};
        autil::legacy::json::ToString(ParseJson(R"json({"$attr1":1, "$$f0":0, "$$f1":0.1, "$$f2":"st1"})json"), normalScan._outputExprsJson);
        normalScan._tableInfo = tableInfo;
        normalScan._attributeExpressionCreator = attributeExpressionCreator;
        normalScan._matchDocAllocator = matchDocAllocator;
        normalScan._pool = &_pool;
        ASSERT_TRUE(normalScan.initOutputColumn());
        ASSERT_EQ(0, normalScan._copyFieldMap.size());
        ASSERT_EQ(6, matchDocAllocator->_fastReferenceMap.size());
        checkExpr(normalScan._attributeExpressionVec, {"attr1", "$f0", "$f1", "$f2"});
        for (auto expr : normalScan._attributeExpressionVec) {
            ASSERT_EQ(suez::turing::ET_CONST, expr->getExpressionType());
        }
        ReferenceBase *refBase = matchDocAllocator->findReferenceWithoutType("$f0");
        ASSERT_EQ(bt_int32, refBase->getValueType().getBuiltinType());
        auto refVec = matchDocAllocator->getRefBySerializeLevel(SL_ATTRIBUTE);
        ASSERT_EQ(4, refVec.size());
    }
    { // const field with type
        MatchDocAllocatorPtr matchDocAllocator(new matchdoc::MatchDocAllocator(&_pool, useSubFlag));
        AttributeExpressionCreatorPtr attributeExpressionCreator(new AttributeExpressionCreator(
                        &_pool, matchDocAllocator.get(), _tableName, indexAppSnapshot.get(),
                        tableInfo, virtualAttributes, NULL, suez::turing::CavaPluginManagerPtr(), NULL));
        NormalScan normalScan;
        normalScan._hashFields = {"id"};
        normalScan._outputFields = {"$f0", "$f1","$f2"};
        normalScan._outputFieldsType = {"BIGINT", "DOUBLE","VARCHAR"};
        autil::legacy::json::ToString(ParseJson(R"json({"$$f0":0, "$$f1":0.1, "$$f2":"st1"})json"), normalScan._outputExprsJson);
        normalScan._tableInfo = tableInfo;
        normalScan._attributeExpressionCreator = attributeExpressionCreator;
        normalScan._matchDocAllocator = matchDocAllocator;
        normalScan._pool = &_pool;
        ASSERT_TRUE(normalScan.initOutputColumn());
        ASSERT_EQ(0, normalScan._copyFieldMap.size());
        ASSERT_EQ(5, matchDocAllocator->_fastReferenceMap.size());
        checkExpr(normalScan._attributeExpressionVec, {"$f0", "$f1", "$f2"});
        for (auto expr : normalScan._attributeExpressionVec) {
            ASSERT_EQ(suez::turing::ET_CONST, expr->getExpressionType());
        }
        ReferenceBase *refBase = matchDocAllocator->findReferenceWithoutType("$f0");
        ASSERT_EQ(bt_int64, refBase->getValueType().getBuiltinType());
        auto refVec = matchDocAllocator->getRefBySerializeLevel(SL_ATTRIBUTE);
        ASSERT_EQ(3, refVec.size());
    }

    { // null field
        MatchDocAllocatorPtr matchDocAllocator(new matchdoc::MatchDocAllocator(&_pool, useSubFlag));
        AttributeExpressionCreatorPtr attributeExpressionCreator(new AttributeExpressionCreator(
                        &_pool, matchDocAllocator.get(), _tableName, indexAppSnapshot.get(),
                        tableInfo, virtualAttributes, NULL, suez::turing::CavaPluginManagerPtr(), NULL));
        NormalScan normalScan;
        normalScan._hashFields = {"id"};
        normalScan._outputFields = {"attr1", "attr3"};
        normalScan._outputExprsJson = "";
        normalScan._tableInfo = tableInfo;
        normalScan._attributeExpressionCreator = attributeExpressionCreator;
        normalScan._matchDocAllocator = matchDocAllocator;
        normalScan._pool = &_pool;
        ASSERT_TRUE(normalScan.initOutputColumn());
        ASSERT_EQ(0, normalScan._copyFieldMap.size());
        checkExpr(normalScan._attributeExpressionVec, {"attr1", "attr3"});
        auto refVec = matchDocAllocator->getRefBySerializeLevel(SL_ATTRIBUTE);
        ASSERT_EQ(2, refVec.size());
        ASSERT_EQ(suez::turing::ET_CONST, normalScan._attributeExpressionVec[1]->getExpressionType());
    }
    { // case op
        MatchDocAllocatorPtr matchDocAllocator(new matchdoc::MatchDocAllocator(&_pool, useSubFlag));
        AttributeExpressionCreatorPtr attributeExpressionCreator(new AttributeExpressionCreator(
                        &_pool, matchDocAllocator.get(), _tableName, indexAppSnapshot.get(),
                        tableInfo, virtualAttributes, NULL, suez::turing::CavaPluginManagerPtr(), NULL));
        NormalScan normalScan;
        normalScan._hashFields = {"id"};
        normalScan._outputFields = {"attr1", "$f0", "$f1","$f2", "case"};
        normalScan._outputExprsJson = R"({"$attr1":1, "$$f0":0, "$$f1":0.1, "$$f2":"st1", "$case":{"op":"CASE","params":[{"op":"=","params":["$id",{"op":"CAST","cast_type":"BIGINT","params":[48],"type":"UDF"}],"type":"OTHER"},"$attr1",{"op":"=","params":["$id",{"op":"CAST","cast_type":"BIGINT","params":[24],"type":"UDF"}],"type":"OTHER"},"$attr1","$attr1"],"type":"OTHER"}})";
        normalScan._outputFieldsType = {"BIGINT", "BIGINT", "DECIMAL", "VARCHAR", "BIGINT"};
        normalScan._tableInfo = tableInfo;
        normalScan._attributeExpressionCreator = attributeExpressionCreator;
        normalScan._matchDocAllocator = matchDocAllocator;
        normalScan._pool = &_pool;
        ASSERT_TRUE(normalScan.initOutputColumn());
        ASSERT_EQ(0, normalScan._copyFieldMap.size());
        ASSERT_EQ(7, matchDocAllocator->_fastReferenceMap.size());
        ASSERT_EQ(normalScan._attributeExpressionVec.size(), 5);
        ReferenceBase *refBase = matchDocAllocator->findReferenceWithoutType("case");
        ASSERT_EQ(bt_int64, refBase->getValueType().getBuiltinType());
        auto refVec = matchDocAllocator->getRefBySerializeLevel(SL_ATTRIBUTE);
        ASSERT_EQ(5, refVec.size());
    }
    { // case op boolean
        MatchDocAllocatorPtr matchDocAllocator(new matchdoc::MatchDocAllocator(&_pool, useSubFlag));
        AttributeExpressionCreatorPtr attributeExpressionCreator(new AttributeExpressionCreator(
                        &_pool, matchDocAllocator.get(), _tableName, indexAppSnapshot.get(),
                        tableInfo, virtualAttributes, NULL, suez::turing::CavaPluginManagerPtr(), NULL));
        NormalScan normalScan;
        normalScan._hashFields = {"id"};
        normalScan._outputFields = {"attr1", "$f0", "$f1","$f2", "case"};
        normalScan._outputExprsJson = R"({"$attr1":1, "$$f0":0, "$$f1":0.1, "$$f2":"st1", "$case":{"op":"CASE","params":[{"op":"=","params":[{"op":"*","params":[{"op":"+","params":["$attr1",1],"type":"OTHER"},2],"type":"OTHER"},{"op":"CAST","cast_type":"BIGINT","params":[48],"type":"UDF"}],"type":"OTHER"},{"op":">","params":["$attr1",10],"type":"OTHER"},{"op":"=","params":[{"op":"*","params":[{"op":"+","params":["$attr1",1],"type":"OTHER"},2],"type":"OTHER"},{"op":"CAST","cast_type":"BIGINT","params":[24],"type":"UDF"}],"type":"OTHER"},{"op":">","params":["$id",10],"type":"OTHER"},{"op":"CAST","cast_type":"BOOLEAN","params":[false],"type":"UDF"}],"type":"OTHER"}})";
        normalScan._outputFieldsType = {"BIGINT", "BIGINT", "DECIMAL", "VARCHAR", "BOOLEAN"};
        normalScan._tableInfo = tableInfo;
        normalScan._attributeExpressionCreator = attributeExpressionCreator;
        normalScan._matchDocAllocator = matchDocAllocator;
        normalScan._pool = &_pool;
        ASSERT_TRUE(normalScan.initOutputColumn());
        ASSERT_EQ(0, normalScan._copyFieldMap.size());
        ASSERT_EQ(7, matchDocAllocator->_fastReferenceMap.size());
        ASSERT_EQ(normalScan._attributeExpressionVec.size(), 5);
        ReferenceBase *refBase = matchDocAllocator->findReferenceWithoutType("case");
        ASSERT_EQ(bt_bool, refBase->getValueType().getBuiltinType());
        auto refVec = matchDocAllocator->getRefBySerializeLevel(SL_ATTRIBUTE);
        ASSERT_EQ(5, refVec.size());
    }
    { // case op string
        MatchDocAllocatorPtr matchDocAllocator(new matchdoc::MatchDocAllocator(&_pool, useSubFlag));
        AttributeExpressionCreatorPtr attributeExpressionCreator(new AttributeExpressionCreator(
                        &_pool, matchDocAllocator.get(), _tableName, indexAppSnapshot.get(),
                        tableInfo, virtualAttributes, NULL, suez::turing::CavaPluginManagerPtr(), NULL));
        NormalScan normalScan;
        normalScan._hashFields = {"id"};
        normalScan._outputFields = {"attr1", "$f0", "$f1","$f2", "case"};
        normalScan._outputExprsJson = R"({"$attr1":1,"$$f0":0,"$$f1":0.1,"$$f2":"st1","$case":{"op":"CASE","params":[{"op":"=","params":[{"op":"*","params":[{"op":"+","params":["$attr1",1],"type":"OTHER"},2],"type":"OTHER"},{"op":"CAST","cast_type":"BIGINT","params":[48],"type":"UDF"}],"type":"OTHER"},"abc",{"op":"=","params":[{"op":"*","params":[{"op":"+","params":["$attr1",1],"type":"OTHER"},2],"type":"OTHER"},{"op":"CAST","cast_type":"BIGINT","params":[24],"type":"UDF"}],"type":"OTHER"},"def","ghi"],"type":"OTHER"}})";
        normalScan._outputFieldsType = {"BIGINT", "BIGINT", "DECIMAL", "VARCHAR", "VARCHAR"};
        normalScan._tableInfo = tableInfo;
        normalScan._attributeExpressionCreator = attributeExpressionCreator;
        normalScan._matchDocAllocator = matchDocAllocator;
        normalScan._pool = &_pool;
        ASSERT_TRUE(normalScan.initOutputColumn());
        ASSERT_EQ(0, normalScan._copyFieldMap.size());
        ASSERT_EQ(7, matchDocAllocator->_fastReferenceMap.size());
        ASSERT_EQ(normalScan._attributeExpressionVec.size(), 5);
        ReferenceBase *refBase = matchDocAllocator->findReferenceWithoutType("case");
        ASSERT_EQ(bt_string, refBase->getValueType().getBuiltinType());
        auto refVec = matchDocAllocator->getRefBySerializeLevel(SL_ATTRIBUTE);
        ASSERT_EQ(5, refVec.size());
    }

}

TEST_F(NormalScanTest, testInitOutputColumnUseSubFalse) {
    auto tableInfo = TableInfoConfigurator::createFromIndexApp(_tableName, _indexApp);
    bool useSubFlag = false;
    auto indexAppSnapshot = _indexApp->CreateSnapshot();
    std::vector<suez::turing::VirtualAttribute*> virtualAttributes;
    { // output expr is empty
        MatchDocAllocatorPtr matchDocAllocator(new matchdoc::MatchDocAllocator(&_pool, useSubFlag));
        AttributeExpressionCreatorPtr attributeExpressionCreator(new AttributeExpressionCreator(
                        &_pool, matchDocAllocator.get(), _tableName, indexAppSnapshot.get(),
                        tableInfo, virtualAttributes, NULL, suez::turing::CavaPluginManagerPtr(), NULL));
        NormalScan normalScan;
        normalScan._hashFields = {"id"};
        normalScan._outputFields = {"attr1", "attr2"};
        normalScan._outputExprsJson = "";
        normalScan._tableInfo = tableInfo;
        normalScan._attributeExpressionCreator = attributeExpressionCreator;
        normalScan._pool = &_pool;
        normalScan._matchDocAllocator = matchDocAllocator;
        ASSERT_TRUE(normalScan.initOutputColumn());
        ASSERT_EQ(0, normalScan._copyFieldMap.size());
        checkExpr(normalScan._attributeExpressionVec, {"attr1", "attr2"});
        auto refVec = matchDocAllocator->getRefBySerializeLevel(SL_ATTRIBUTE);
        ASSERT_EQ(2, refVec.size());
    }
    { // copy field
        MatchDocAllocatorPtr matchDocAllocator(new matchdoc::MatchDocAllocator(&_pool, useSubFlag));
        AttributeExpressionCreatorPtr attributeExpressionCreator(new AttributeExpressionCreator(
                        &_pool, matchDocAllocator.get(), _tableName, indexAppSnapshot.get(),
                        tableInfo, virtualAttributes, NULL, suez::turing::CavaPluginManagerPtr(), NULL));
        NormalScan normalScan;
        normalScan._hashFields = {"id"};
        normalScan._outputFields = {"attr1", "attr1_copy"};
        autil::legacy::json::ToString(ParseJson(R"json({"$attr1_copy":"$attr1"})json"), normalScan._outputExprsJson);
        normalScan._tableInfo = tableInfo;
        normalScan._attributeExpressionCreator = attributeExpressionCreator;
        normalScan._matchDocAllocator = matchDocAllocator;
        normalScan._pool = &_pool;
        ASSERT_TRUE(normalScan.initOutputColumn());
        ASSERT_EQ(1, normalScan._copyFieldMap.size());
        ASSERT_EQ("attr1", normalScan._copyFieldMap["attr1_copy"]);
        ASSERT_EQ(2, matchDocAllocator->_fastReferenceMap.size());
        checkExpr(normalScan._attributeExpressionVec, {"attr1"});
        auto refVec = matchDocAllocator->getRefBySerializeLevel(SL_ATTRIBUTE);
        ASSERT_EQ(2, refVec.size());
    }
    { // const field
        MatchDocAllocatorPtr matchDocAllocator(new matchdoc::MatchDocAllocator(&_pool, useSubFlag));
        AttributeExpressionCreatorPtr attributeExpressionCreator(new AttributeExpressionCreator(
                        &_pool, matchDocAllocator.get(), _tableName, indexAppSnapshot.get(),
                        tableInfo, virtualAttributes, NULL, suez::turing::CavaPluginManagerPtr(), NULL));
        NormalScan normalScan;
        normalScan._hashFields = {"id"};
        normalScan._outputFields = {"attr1", "$f0", "$f1","$f2"};
        autil::legacy::json::ToString(ParseJson(R"json({"$attr1":1, "$$f0":0, "$$f1":0.1, "$$f2":"st1"})json"), normalScan._outputExprsJson);
        normalScan._tableInfo = tableInfo;
        normalScan._attributeExpressionCreator = attributeExpressionCreator;
        normalScan._matchDocAllocator = matchDocAllocator;
        normalScan._pool = &_pool;
        ASSERT_TRUE(normalScan.initOutputColumn());
        ASSERT_EQ(0, normalScan._copyFieldMap.size());
        ASSERT_EQ(4, matchDocAllocator->_fastReferenceMap.size());
        checkExpr(normalScan._attributeExpressionVec, {"attr1", "$f0", "$f1", "$f2"});
        for (auto expr : normalScan._attributeExpressionVec) {
            ASSERT_EQ(suez::turing::ET_CONST, expr->getExpressionType());
        }
        ReferenceBase *refBase = matchDocAllocator->findReferenceWithoutType("$f0");
        ASSERT_EQ(bt_int32, refBase->getValueType().getBuiltinType());
        auto refVec = matchDocAllocator->getRefBySerializeLevel(SL_ATTRIBUTE);
        ASSERT_EQ(4, refVec.size());
    }
    { // null field
        MatchDocAllocatorPtr matchDocAllocator(new matchdoc::MatchDocAllocator(&_pool, useSubFlag));
        AttributeExpressionCreatorPtr attributeExpressionCreator(new AttributeExpressionCreator(
                        &_pool, matchDocAllocator.get(), _tableName, indexAppSnapshot.get(),
                        tableInfo, virtualAttributes, NULL, suez::turing::CavaPluginManagerPtr(), NULL));
        NormalScan normalScan;
        normalScan._hashFields = {"id"};
        normalScan._outputFields = {"attr1", "attr3"};
        normalScan._outputExprsJson = "";
        normalScan._tableInfo = tableInfo;
        normalScan._attributeExpressionCreator = attributeExpressionCreator;
        normalScan._matchDocAllocator = matchDocAllocator;
        normalScan._pool = &_pool;
        ASSERT_TRUE(normalScan.initOutputColumn());
        ASSERT_EQ(0, normalScan._copyFieldMap.size());
        checkExpr(normalScan._attributeExpressionVec, {"attr1", "attr3"});
        auto refVec = matchDocAllocator->getRefBySerializeLevel(SL_ATTRIBUTE);
        ASSERT_EQ(2, refVec.size());
        ASSERT_EQ(suez::turing::ET_CONST, normalScan._attributeExpressionVec[1]->getExpressionType());
    }
    { // case op
        MatchDocAllocatorPtr matchDocAllocator(new matchdoc::MatchDocAllocator(&_pool, useSubFlag));
        AttributeExpressionCreatorPtr attributeExpressionCreator(new AttributeExpressionCreator(
                        &_pool, matchDocAllocator.get(), _tableName, indexAppSnapshot.get(),
                        tableInfo, virtualAttributes, NULL, suez::turing::CavaPluginManagerPtr(), NULL));
        NormalScan normalScan;
        normalScan._hashFields = {"id"};
        normalScan._outputFields = {"attr1", "$f0", "$f1","$f2", "case"};
        normalScan._outputExprsJson = R"({"$attr1":1, "$$f0":0, "$$f1":0.1, "$$f2":"st1", "$case":{"op":"CASE","params":[{"op":"=","params":["$id",{"op":"CAST","cast_type":"BIGINT","params":[48],"type":"UDF"}],"type":"OTHER"},"$attr1",{"op":"=","params":["$id",{"op":"CAST","cast_type":"BIGINT","params":[24],"type":"UDF"}],"type":"OTHER"},"$attr1","$attr1"],"type":"OTHER"}})";
        normalScan._outputFieldsType = {"BIGINT", "BIGINT", "DECIMAL", "VARCHAR", "BIGINT"};
        normalScan._tableInfo = tableInfo;
        normalScan._attributeExpressionCreator = attributeExpressionCreator;
        normalScan._matchDocAllocator = matchDocAllocator;
        normalScan._pool = &_pool;
        ASSERT_TRUE(normalScan.initOutputColumn());
        ASSERT_EQ(0, normalScan._copyFieldMap.size());
        ASSERT_EQ(5, matchDocAllocator->_fastReferenceMap.size());
        ASSERT_EQ(normalScan._attributeExpressionVec.size(), 5);
        ReferenceBase *refBase = matchDocAllocator->findReferenceWithoutType("case");
        ASSERT_EQ(bt_int64, refBase->getValueType().getBuiltinType());
        auto refVec = matchDocAllocator->getRefBySerializeLevel(SL_ATTRIBUTE);
        ASSERT_EQ(5, refVec.size());
    }
    { // case op boolean
        MatchDocAllocatorPtr matchDocAllocator(new matchdoc::MatchDocAllocator(&_pool, useSubFlag));
        AttributeExpressionCreatorPtr attributeExpressionCreator(new AttributeExpressionCreator(
                        &_pool, matchDocAllocator.get(), _tableName, indexAppSnapshot.get(),
                        tableInfo, virtualAttributes, NULL, suez::turing::CavaPluginManagerPtr(), NULL));
        NormalScan normalScan;
        normalScan._hashFields = {"id"};
        normalScan._outputFields = {"attr1", "$f0", "$f1","$f2", "case"};
        normalScan._outputExprsJson = R"({"$attr1":1, "$$f0":0, "$$f1":0.1, "$$f2":"st1", "$case":{"op":"CASE","params":[{"op":"=","params":[{"op":"*","params":[{"op":"+","params":["$attr1",1],"type":"OTHER"},2],"type":"OTHER"},{"op":"CAST","cast_type":"BIGINT","params":[48],"type":"UDF"}],"type":"OTHER"},{"op":">","params":["$attr1",10],"type":"OTHER"},{"op":"=","params":[{"op":"*","params":[{"op":"+","params":["$attr1",1],"type":"OTHER"},2],"type":"OTHER"},{"op":"CAST","cast_type":"BIGINT","params":[24],"type":"UDF"}],"type":"OTHER"},{"op":">","params":["$id",10],"type":"OTHER"},{"op":"CAST","cast_type":"BOOLEAN","params":[false],"type":"UDF"}],"type":"OTHER"}})";
        normalScan._outputFieldsType = {"BIGINT", "BIGINT", "DECIMAL", "VARCHAR", "BOOLEAN"};
        normalScan._tableInfo = tableInfo;
        normalScan._attributeExpressionCreator = attributeExpressionCreator;
        normalScan._matchDocAllocator = matchDocAllocator;
        normalScan._pool = &_pool;
        ASSERT_TRUE(normalScan.initOutputColumn());
        ASSERT_EQ(0, normalScan._copyFieldMap.size());
        ASSERT_EQ(5, matchDocAllocator->_fastReferenceMap.size());
        ASSERT_EQ(normalScan._attributeExpressionVec.size(), 5);
        ReferenceBase *refBase = matchDocAllocator->findReferenceWithoutType("case");
        ASSERT_EQ(bt_bool, refBase->getValueType().getBuiltinType());
        auto refVec = matchDocAllocator->getRefBySerializeLevel(SL_ATTRIBUTE);
        ASSERT_EQ(5, refVec.size());
    }
    { // case op string
        MatchDocAllocatorPtr matchDocAllocator(new matchdoc::MatchDocAllocator(&_pool, useSubFlag));
        AttributeExpressionCreatorPtr attributeExpressionCreator(new AttributeExpressionCreator(
                        &_pool, matchDocAllocator.get(), _tableName, indexAppSnapshot.get(),
                        tableInfo, virtualAttributes, NULL, suez::turing::CavaPluginManagerPtr(), NULL));
        NormalScan normalScan;
        normalScan._hashFields = {"id"};
        normalScan._outputFields = {"attr1", "$f0", "$f1","$f2", "case"};
        normalScan._outputExprsJson = R"({"$attr1":1,"$$f0":0,"$$f1":0.1,"$$f2":"st1","$case":{"op":"CASE","params":[{"op":"=","params":[{"op":"*","params":[{"op":"+","params":["$attr1",1],"type":"OTHER"},2],"type":"OTHER"},{"op":"CAST","cast_type":"BIGINT","params":[48],"type":"UDF"}],"type":"OTHER"},"abc",{"op":"=","params":[{"op":"*","params":[{"op":"+","params":["$attr1",1],"type":"OTHER"},2],"type":"OTHER"},{"op":"CAST","cast_type":"BIGINT","params":[24],"type":"UDF"}],"type":"OTHER"},"def","ghi"],"type":"OTHER"}})";
        normalScan._outputFieldsType = {"BIGINT", "BIGINT", "DECIMAL", "VARCHAR", "VARCHAR"};
        normalScan._tableInfo = tableInfo;
        normalScan._attributeExpressionCreator = attributeExpressionCreator;
        normalScan._matchDocAllocator = matchDocAllocator;
        normalScan._pool = &_pool;
        ASSERT_TRUE(normalScan.initOutputColumn());
        ASSERT_EQ(0, normalScan._copyFieldMap.size());
        ASSERT_EQ(5, matchDocAllocator->_fastReferenceMap.size());
        ASSERT_EQ(normalScan._attributeExpressionVec.size(), 5);
        ReferenceBase *refBase = matchDocAllocator->findReferenceWithoutType("case");
        ASSERT_EQ(bt_string, refBase->getValueType().getBuiltinType());
        auto refVec = matchDocAllocator->getRefBySerializeLevel(SL_ATTRIBUTE);
        ASSERT_EQ(5, refVec.size());
    }

}

TEST_F(NormalScanTest, testCreateStreamScanIterator) {
    autil::legacy::json::JsonMap attributeMap;
    attributeMap["hash_fields"] = ParseJson(string(R"json(["id"])json"));
    attributeMap["table_type"] = string("normal");
    attributeMap["table_name"] = _tableName;
    attributeMap["db_name"] = string("default");
    attributeMap["catalog_name"] = string("default");
    attributeMap["output_fields"] = ParseJson(string(R"json(["$attr1", "$attr2", "$id"])json"));
    attributeMap["limit"] = Any(1000);
    string jsonStr = autil::legacy::ToJsonString(attributeMap);
    JsonMap jsonMap;
    FromJsonString(jsonMap, jsonStr);
    autil::legacy::Jsonizable::JsonWrapper wrapper(jsonMap);
    ScanInitParam param;
    int64_t timeout = TimeUtility::currentTime()/ 1000 + 1000;
    _sqlQueryResource->setTimeoutMs(timeout);
    ASSERT_TRUE(param.initFromJson(wrapper));
    param.bizResource = _sqlBizResource.get();
    param.queryResource = _sqlQueryResource.get();
    param.memoryPoolResource = &_memPoolResource;
    { // empty query
        NormalScan normalScan;
        normalScan._hashFields = {"id"};
        ASSERT_TRUE(normalScan.init(param));
        ScanIteratorPtr scanIter;
        ASSERT_TRUE(normalScan.updateScanQuery(StreamQueryPtr()));
        ASSERT_TRUE(normalScan._scanIter == nullptr);
    }
    {
        NormalScan normalScan;
        normalScan._hashFields = {"id"};
        ASSERT_TRUE(normalScan.init(param));
        ScanIteratorPtr scanIter;
        StreamQueryPtr inputQuery(new StreamQuery());
        QueryPtr query(new TermQuery("aa", "name", RequiredFields(), ""));
        inputQuery->query = query;
        ASSERT_TRUE(normalScan.updateScanQuery(inputQuery));
        ASSERT_TRUE(normalScan._scanIter != nullptr);
        vector<MatchDoc> matchDocs;
        bool ret = normalScan._scanIter->batchSeek(10, matchDocs);
        ASSERT_TRUE(ret);
        ASSERT_EQ(1, matchDocs.size());
    }
    { //emtpy term
        NormalScan normalScan;
        normalScan._hashFields = {"id"};
        ASSERT_TRUE(normalScan.init(param));
        ScanIteratorPtr scanIter;
        StreamQueryPtr inputQuery(new StreamQuery());
        QueryPtr query(new TermQuery("aaa", "name", RequiredFields(), ""));
        inputQuery->query = query;
        ASSERT_TRUE(normalScan.updateScanQuery(inputQuery));
        ASSERT_TRUE(normalScan._scanIter == nullptr);
    }
    {// and query
        NormalScan normalScan;
        normalScan._hashFields = {"id"};
        ASSERT_TRUE(normalScan.init(param));
        ScanIteratorPtr scanIter;
        StreamQueryPtr inputQuery(new StreamQuery());
        QueryPtr query(new TermQuery("aa", "name", RequiredFields(), ""));
        inputQuery->query = query;
        normalScan._baseCreateScanIterInfo.query = query;
        ASSERT_TRUE(normalScan.updateScanQuery(inputQuery));
        ASSERT_TRUE(normalScan._scanIter != nullptr);
        vector<MatchDoc> matchDocs;
        bool ret = normalScan._scanIter->batchSeek(10, matchDocs);
        ASSERT_TRUE(ret);
        ASSERT_EQ(1, matchDocs.size());
    }
}

TEST_F(NormalScanTest, testCreateTable) {
    { //reuse allocator
        MatchDocAllocatorPtr allocator(new MatchDocAllocator(&_pool, false));
        auto ref1 = allocator->declare<int32_t>("aa", SL_ATTRIBUTE);
        vector<MatchDoc> matchDocVec;
        MatchDoc doc = allocator->allocate(1);
        matchDocVec.push_back(doc);
        ref1->set(doc, 10);
        NormalScan normalScan;
        normalScan._hashFields = {"id"};
        normalScan._pool = &_pool;
        normalScan._memoryPoolResource = &_memPoolResource;
        TablePtr table = normalScan.createTable(matchDocVec, allocator, true);
        ASSERT_TRUE(table != nullptr);
        ASSERT_EQ(1, table->getRowCount());
        ASSERT_EQ(1, table->getColumnCount());
        ASSERT_EQ("10", table->toString(0, 0));
    }
    { //copy allocator
        MatchDocAllocatorPtr allocator(new MatchDocAllocator(&_pool, true));
        auto ref1 = allocator->declare<int32_t>("aa", SL_ATTRIBUTE);
        vector<MatchDoc> matchDocVec;
        MatchDoc doc = allocator->allocate(1);
        matchDocVec.push_back(doc);
        ref1->set(doc, 10);
        NormalScan normalScan;
        normalScan._hashFields = {"id"};
        normalScan._pool = &_pool;
        normalScan._memoryPoolResource = &_memPoolResource;
        TablePtr table = normalScan.createTable(matchDocVec, allocator, false);
        ASSERT_TRUE(table != nullptr);
        ASSERT_EQ(1, table->getRowCount());
        ASSERT_EQ(1, table->getColumnCount());
        ASSERT_EQ("10", table->toString(0, 0));
    }

    { // has subdoc ,reuse allocator
        MatchDocAllocatorPtr allocator(new MatchDocAllocator(&_pool, true));
        auto ref1 = allocator->declare<int32_t>("aa", SL_ATTRIBUTE);
        auto subRef1 = allocator->declareSub<int32_t>("sub_aa", SL_ATTRIBUTE);
        vector<MatchDoc> matchDocVec;
        MatchDoc doc = allocator->allocate(1);
        allocator->allocateSub(doc, 20);
        subRef1->set(doc, 20);
        allocator->allocateSub(doc, 21);
        subRef1->set(doc, 21);
        allocator->allocateSub(doc, 22);
        subRef1->set(doc, 22);
        matchDocVec.push_back(doc);
        ref1->set(doc, 10);
        NormalScan normalScan;
        normalScan._hashFields = {"id"};
        normalScan._useSub = true;
        normalScan._pool = &_pool;
        normalScan._memoryPoolResource = &_memPoolResource;
        normalScan._outputFields = {"aa", "sub_aa"};
        TablePtr table = normalScan.createTable(matchDocVec, allocator, true);
        ASSERT_TRUE(table != nullptr);
        ASSERT_EQ(3, table->getRowCount());
        ASSERT_EQ(2, table->getColumnCount());
        ASSERT_EQ("10", table->toString(0, 0));
        ASSERT_EQ("20", table->toString(0, 1));
        ASSERT_EQ("10", table->toString(1, 0));
        ASSERT_EQ("21", table->toString(1, 1));
        ASSERT_EQ("10", table->toString(2, 0));
        ASSERT_EQ("22", table->toString(2, 1));
    }
    { // has subdoc , copy allocator
        MatchDocAllocatorPtr allocator(new MatchDocAllocator(&_pool, true));
        auto ref1 = allocator->declare<int32_t>("aa", SL_ATTRIBUTE);
        auto subRef1 = allocator->declareSub<int32_t>("sub_aa", SL_ATTRIBUTE);
        vector<MatchDoc> matchDocVec;
        MatchDoc doc = allocator->allocate(1);
        allocator->allocateSub(doc, 20);
        subRef1->set(doc, 20);
        allocator->allocateSub(doc, 21);
        subRef1->set(doc, 21);
        allocator->allocateSub(doc, 22);
        subRef1->set(doc, 22);
        matchDocVec.push_back(doc);
        ref1->set(doc, 10);
        NormalScan normalScan;
        normalScan._hashFields = {"id"};
        normalScan._useSub = true;
        normalScan._pool = &_pool;
        normalScan._memoryPoolResource = &_memPoolResource;
        normalScan._outputFields = {"aa", "sub_aa"};
        TablePtr table = normalScan.createTable(matchDocVec, allocator, false);
        ASSERT_TRUE(table != nullptr);
        ASSERT_EQ(3, table->getRowCount());
        ASSERT_EQ(2, table->getColumnCount());
        ASSERT_EQ("10", table->toString(0, 0));
        ASSERT_EQ("20", table->toString(0, 1));
        ASSERT_EQ("10", table->toString(1, 0));
        ASSERT_EQ("21", table->toString(1, 1));
        ASSERT_EQ("10", table->toString(2, 0));
        ASSERT_EQ("22", table->toString(2, 1));
    }
    { // has subdoc , copy allocator , has empty sub
        MatchDocAllocatorPtr allocator(new MatchDocAllocator(&_pool, true));
        auto ref1 = allocator->declare<int32_t>("aa", SL_ATTRIBUTE);
        auto subRef1 = allocator->declareSub<int32_t>("sub_aa", SL_ATTRIBUTE);
        vector<MatchDoc> matchDocVec;
        MatchDoc doc = allocator->allocate(1);
        allocator->allocateSub(doc, 20);
        subRef1->set(doc, 20);
        allocator->allocateSub(doc, 21);
        subRef1->set(doc, 21);
        matchDocVec.push_back(doc);
        ref1->set(doc, 10);

        MatchDoc doc2 = allocator->allocate(2);
        ref1->set(doc2, 12);
        matchDocVec.push_back(doc2);

        NormalScan normalScan;
        normalScan._hashFields = {"id"};
        normalScan._useSub = true;
        normalScan._pool = &_pool;
        normalScan._memoryPoolResource = &_memPoolResource;
        normalScan._outputFields = {"aa", "sub_aa"};
        TablePtr table = normalScan.createTable(matchDocVec, allocator, false);
        ASSERT_TRUE(table != nullptr);
        ASSERT_EQ(3, table->getRowCount());
        ASSERT_EQ(2, table->getColumnCount());
        ASSERT_EQ("10", table->toString(0, 0));
        ASSERT_EQ("20", table->toString(0, 1));
        ASSERT_EQ("10", table->toString(1, 0));
        ASSERT_EQ("21", table->toString(1, 1));
        ASSERT_EQ("12", table->toString(2, 0));
        ASSERT_EQ("0", table->toString(2, 1));
    }
}

TEST_F(NormalScanTest, testFlattenSubLeftJoin) {
    // has subdoc , copy allocator , has empty sub
    MatchDocAllocatorPtr allocator(new MatchDocAllocator(&_pool, true));
    auto ref1 = allocator->declare<int32_t>("aa", SL_ATTRIBUTE);
    auto subRef1 = allocator->declareSub<int32_t>("sub_aa", SL_ATTRIBUTE);
    vector<MatchDoc> matchDocVec;
    MatchDoc doc1 = allocator->allocate(1);
    auto *firstRef = allocator->_firstSub;
    auto *nextRef = allocator->_nextSub;
    ASSERT_NE(nullptr, firstRef);
    ASSERT_NE(nullptr, nextRef);

    auto subDoc1 = allocator->allocateSub(doc1, 20);
    subRef1->set(doc1, 20);
    auto subDoc2 = allocator->allocateSub(doc1, 21);
    subRef1->set(doc1, 21);
    matchDocVec.push_back(doc1);
    ref1->set(doc1, 10);
    ASSERT_EQ(subDoc1, firstRef->get(doc1));
    ASSERT_EQ(subDoc2, nextRef->get(subDoc1));
    ASSERT_EQ(INVALID_MATCHDOC, nextRef->get(subDoc2));

    MatchDoc doc2 = allocator->allocate(2);
    ref1->set(doc2, 12);
    matchDocVec.push_back(doc2);
    ASSERT_EQ(INVALID_MATCHDOC, firstRef->get(doc2));

    NormalScan normalScan;
    ASSERT_EQ(LEFT_JOIN, normalScan._nestTableJoinType);

    TablePtr table;
    normalScan.flattenSub(allocator, matchDocVec, table);
    ASSERT_TRUE(table != nullptr);
    ASSERT_EQ(3, table->getRowCount());
    ASSERT_EQ(2, table->getColumnCount());
    ASSERT_EQ("10", table->toString(0, 0));
    ASSERT_EQ("20", table->toString(0, 1));
    ASSERT_EQ("10", table->toString(1, 0));
    ASSERT_EQ("21", table->toString(1, 1));
    ASSERT_EQ("12", table->toString(2, 0));
    ASSERT_EQ("0", table->toString(2, 1));

    ASSERT_EQ(20, firstRef->get(table->_rows[0]).docid);
    ASSERT_EQ(21, firstRef->get(table->_rows[1]).docid);
    ASSERT_EQ(-1, firstRef->get(table->_rows[2]).docid);

    ASSERT_EQ(INVALID_MATCHDOC, nextRef->get(firstRef->get(table->_rows[0])));
    ASSERT_EQ(INVALID_MATCHDOC, nextRef->get(firstRef->get(table->_rows[1])));
    ASSERT_EQ(INVALID_MATCHDOC, nextRef->get(firstRef->get(table->_rows[2])));
}

TEST_F(NormalScanTest, testFlattenSubInnerJoin) {
    // has subdoc , copy allocator , has empty sub
    MatchDocAllocatorPtr allocator(new MatchDocAllocator(&_pool, true));
    auto ref1 = allocator->declare<int32_t>("aa", SL_ATTRIBUTE);
    auto subRef1 = allocator->declareSub<int32_t>("sub_aa", SL_ATTRIBUTE);
    vector<MatchDoc> matchDocVec;
    MatchDoc doc1 = allocator->allocate(1);
    auto *firstRef = allocator->_firstSub;
    auto *nextRef = allocator->_nextSub;
    ASSERT_NE(nullptr, firstRef);
    ASSERT_NE(nullptr, nextRef);

    auto subDoc1 = allocator->allocateSub(doc1, 20);
    subRef1->set(doc1, 20);
    auto subDoc2 = allocator->allocateSub(doc1, 21);
    subRef1->set(doc1, 21);
    matchDocVec.push_back(doc1);
    ref1->set(doc1, 10);
    ASSERT_EQ(subDoc1, firstRef->get(doc1));
    ASSERT_EQ(subDoc2, nextRef->get(subDoc1));
    ASSERT_EQ(INVALID_MATCHDOC, nextRef->get(subDoc2));

    MatchDoc doc2 = allocator->allocate(2);
    ref1->set(doc2, 12);
    matchDocVec.push_back(doc2);
    ASSERT_EQ(INVALID_MATCHDOC, firstRef->get(doc2));

    NormalScan normalScan;
    normalScan._nestTableJoinType = INNER_JOIN;

    TablePtr table;
    normalScan.flattenSub(allocator, matchDocVec, table);
    ASSERT_TRUE(table != nullptr);
    ASSERT_EQ(2, table->getRowCount());
    ASSERT_EQ(2, table->getColumnCount());
    ASSERT_EQ("10", table->toString(0, 0));
    ASSERT_EQ("20", table->toString(0, 1));
    ASSERT_EQ("10", table->toString(1, 0));
    ASSERT_EQ("21", table->toString(1, 1));

    ASSERT_EQ(20, firstRef->get(table->_rows[0]).docid);
    ASSERT_EQ(21, firstRef->get(table->_rows[1]).docid);

    ASSERT_EQ(INVALID_MATCHDOC, nextRef->get(firstRef->get(table->_rows[0])));
    ASSERT_EQ(INVALID_MATCHDOC, nextRef->get(firstRef->get(table->_rows[1])));
}

TEST_F(NormalScanTest, testAppendColumns) {
    MatchDocAllocatorPtr allocator(new MatchDocAllocator(&_pool));
    vector<MatchDoc> leftDocs = move(getMatchDocs(allocator, 1));
    ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator<uint32_t>(allocator, leftDocs, "fid", {1}));
    ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator(allocator, leftDocs, "path", {"abc"}));
    map<string, string> fieldMap;
    fieldMap["fid0"] = "fid";
    fieldMap["path0"] = "path";
    NormalScan normalScan;
    for (auto pair : fieldMap) {
        ASSERT_TRUE(normalScan.appendCopyColumn(pair.second, pair.first, allocator));
    }
    allocator->extend();
    fieldMap["path00"] = "path";
    normalScan.copyColumns(fieldMap, leftDocs, allocator);
    Reference<uint32_t> *fid0 = allocator->findReference<uint32_t>("fid0");
    Reference<MultiChar> *path0 = allocator->findReference<MultiChar>("path0");
    ASSERT_TRUE(fid0 != nullptr);
    ASSERT_TRUE(path0 != nullptr);
    MatchDoc matchDoc = leftDocs[0];
    ASSERT_EQ(1, fid0->get(matchDoc));
    ASSERT_EQ("abc", string(path0->get(matchDoc).data(), path0->get(matchDoc).size()));
}

TEST_F(NormalScanTest, testCopyColumns) {
    { // empty
        MatchDocAllocatorPtr allocator(new MatchDocAllocator(&_pool));
        map<string, string> fieldMap;
        NormalScan normalScan;
        for (auto pair : fieldMap) {
            ASSERT_TRUE(normalScan.appendCopyColumn(pair.second, pair.first, allocator));
        }
        ASSERT_EQ(0, allocator->_fastReferenceMap.size());
    }
    { // ref not found
        MatchDocAllocatorPtr allocator(new MatchDocAllocator(&_pool));
        map<string, string> fieldMap;
        fieldMap["not_exist"] = "not_exist1";
        NormalScan normalScan;
        for (auto pair : fieldMap) {
            ASSERT_FALSE(normalScan.appendCopyColumn(pair.second, pair.first, allocator));
        }
    }
    { // src dest is same
        MatchDocAllocatorPtr allocator(new MatchDocAllocator(&_pool));
        vector<MatchDoc> leftDocs = move(getMatchDocs(allocator, 1));
        ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator<uint32_t>(allocator, leftDocs, "fid", {1}));
        map<string, string> fieldMap;
        fieldMap["fid"] = "fid";
        NormalScan normalScan;
        for (auto pair : fieldMap) {
            ASSERT_FALSE(normalScan.appendCopyColumn(pair.second, pair.first, allocator));
        }
    }
    { //
        MatchDocAllocatorPtr allocator(new MatchDocAllocator(&_pool));
        vector<MatchDoc> leftDocs = move(getMatchDocs(allocator, 1));
        ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator<uint32_t>(allocator, leftDocs, "fid", {1}));
        ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator(allocator, leftDocs, "path", {"abc"}));
        map<string, string> fieldMap;
        fieldMap["fid"] = "path";
        NormalScan normalScan;
        for (auto pair : fieldMap) {
            ASSERT_FALSE(normalScan.appendCopyColumn(pair.second, pair.first, allocator));
        }
    }
    { // ok
        MatchDocAllocatorPtr allocator(new MatchDocAllocator(&_pool));
        vector<MatchDoc> leftDocs = move(getMatchDocs(allocator, 1));
        ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator<uint32_t>(allocator, leftDocs, "fid", {1}));
        ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator(allocator, leftDocs, "path", {"abc"}));
        map<string, string> fieldMap;
        fieldMap["fid0"] = "fid";
        fieldMap["path0"] = "path";
        NormalScan normalScan;
        for (auto pair : fieldMap) {
            ASSERT_TRUE(normalScan.appendCopyColumn(pair.second, pair.first, allocator));
        }
        ASSERT_EQ(4, allocator->_fastReferenceMap.size());
        ASSERT_TRUE(allocator->findReference<uint32_t>("fid0") != nullptr);
        ASSERT_TRUE(allocator->findReference<MultiChar>("path0") != nullptr);
    }
}

END_HA3_NAMESPACE(sql);
