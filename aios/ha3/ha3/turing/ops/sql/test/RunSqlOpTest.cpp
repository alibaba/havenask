#include <unittest/unittest.h>
#include <ha3/test/test.h>
#include <tensorflow/core/kernels/ops_testutil.h>
#include <tensorflow/core/framework/fake_input.h>
#include <tensorflow/core/framework/node_def_builder.h>
#include <tensorflow/core/framework/variant.h>
#include <tensorflow/core/framework/graph.pb.h>
#include <basic_ops/ops/test/OpTestBase.h>
#include <suez/turing/expression/util/TableInfoConfigurator.h>
#include <ha3/turing/variant/SqlTableVariant.h>
#include <ha3/turing/ops/SqlSessionResource.h>
#include <ha3/turing/ops/sql/proto/SqlSearch.pb.h>
#include <ha3/sql/data/Table.h>
#include <ha3/sql/resource/SqlBizResource.h>
#include <ha3/sql/common/common.h>
#include <build_service/analyzer/AnalyzerFactory.h>
#include <build_service/analyzer/AnalyzerInfos.h>
#include <build_service/analyzer/TokenizerManager.h>
#include <build_service/config/ResourceReader.h>
#include <navi/engine/Navi.h>
#include <navi/builder/GraphBuilder.h>

using namespace std;
using namespace tensorflow;
using namespace suez::turing;
using namespace matchdoc;
using namespace build_service::analyzer;
using namespace autil::legacy;
using namespace autil::legacy::json;

USE_HA3_NAMESPACE(sql);
BEGIN_HA3_NAMESPACE(turing);


class RunSqlOpTest : public OpTestBase {
public:
    void SetUp() override {
        _navi.reset(new navi::Navi());
        _navi->init();
        _needBuildIndex = true;
        _needExprResource = true;
        OpTestBase::SetUp();
    }

    void TearDown() override {
        OpTestBase::TearDown();
        gtl::STLDeleteElements(&tensors_); // clear output for using pool
    }

    void prepareUserIndex() override {
        prepareInvertedTable();
    }

    void prepareInvertedTableData(std::string &tableName,
                                  std::string &fields,
                                  std::string &indexes,
                                  std::string &attributes,
                                  std::string &summarys,
                                  std::string &truncateProfileStr,
                                  std::string &docs,
                                  int64_t &ttl) override
    {
        tableName = "invertedTable";
        fields = "attr1:uint32;attr2:int32:true;id:int64;name:string;index2:string";
        indexes = "id:primarykey64:id;index_2:string:index2;name:string:name";
        attributes = "attr1;attr2;id";
        summarys = "name";
        truncateProfileStr = "";
        docs = "cmd=add,attr1=0,attr2=0 10,id=1,name=aa,index2=a;"
               "cmd=add,attr1=1,attr2=1 11,id=2,name=bb,index2=a;"
               "cmd=add,attr1=2,attr2=2 22,id=3,name=cc,index2=a;"
               "cmd=add,attr1=3,attr2=3 33,id=4,name=dd,index2=b;"
               "cmd=add,attr1=2,attr2=1 11,id=5,name=bb,index2=a;"
               "cmd=add,attr1=2,attr2=2 22,id=6,name=cc,index2=a;"
               "cmd=add,attr1=1,attr2=3 33,id=7,name=dd,index2=b;"
               "cmd=add,attr1=1,attr2=1 11,id=8,name=bb,index2=a;"
               "cmd=add,attr1=2,attr2=2 22,id=9,name=cc,index2=a;"
               "cmd=add,attr1=3,attr2=3 33,id=10,name=dd,index2=b";
        ttl = std::numeric_limits<int64_t>::max();
    }

    void addSqlSessionResource() {
        _sqlBizResource.reset(new SqlBizResource(_sessionResource));
        _navi->setResource(_sqlBizResource->getResourceName(), _sqlBizResource.get());
        turing::SqlSessionResource *sqlResource = new turing::SqlSessionResource();
        sqlResource->naviPtr = _navi;
        sqlResource->analyzerFactory = initAnalyzerFactory();
        sqlResource->queryInfo.setDefaultIndexName("index_2");
        _sessionResource->sharedObjectMap.setWithDelete(turing::SqlSessionResource::name(),
                sqlResource, suez::turing::DeleteLevel::Delete);
    }

    AnalyzerFactoryPtr initAnalyzerFactory() {
        AnalyzerInfosPtr infosPtr(new AnalyzerInfos());
        unique_ptr<AnalyzerInfo> taobaoInfo(new AnalyzerInfo());
        taobaoInfo->setTokenizerConfig(TOKENIZER_TYPE, SIMPLE_ANALYZER);
        taobaoInfo->setTokenizerConfig(DELIMITER, " ");
        taobaoInfo->addStopWord(string("stop"));
        infosPtr->addAnalyzerInfo("taobao_analyzer", *taobaoInfo);

        string basePath = ALIWSLIB_DATA_PATH;
        string aliwsConfig = basePath + "/conf/";

        infosPtr->makeCompatibleWithOldConfig();
        TokenizerManagerPtr tokenizerManagerPtr(
                new TokenizerManager(build_service::config::ResourceReaderPtr(new build_service::config::ResourceReader(aliwsConfig))));
        tokenizerManagerPtr->init(infosPtr->getTokenizerConfig());
        AnalyzerFactory *analyzerFactory = new AnalyzerFactory();
        analyzerFactory->init(infosPtr, tokenizerManagerPtr);

        AnalyzerFactoryPtr ptr(analyzerFactory);
        return ptr;
    }

public:
    void makeOp() {
        TF_ASSERT_OK(
                NodeDefBuilder("myRunSqlOp", "RunSqlOp")
                .Input(FakeInput(DT_STRING))
                .Finalize(node_def()));
        TF_ASSERT_OK(InitOp());
    }

    template <typename T>
    void checkOutput(const vector<T> &values, const string &attrName) {
        auto outputTensor = GetOutput(0);
        ASSERT_TRUE(outputTensor != nullptr);
        string outputStr = outputTensor->scalar<string>()();
        SqlGraphResponse sqlResponse;
        ASSERT_TRUE(sqlResponse.ParseFromString(outputStr));
        ASSERT_EQ(3, sqlResponse.outputs_size());
        auto output0 = sqlResponse.outputs(0);
        ASSERT_EQ("sort", output0.name());
        ASSERT_EQ("output0", output0.port());
        ASSERT_TRUE(output0.has_data());

        auto output1 = sqlResponse.outputs(1);
        ASSERT_EQ(navi::TERMINATOR_NODE_NAME, output1.name());
        ASSERT_EQ(navi::TERMINATOR_OUTPUT_PORT, output1.port());
        ASSERT_TRUE(output1.has_data());

        auto output2 = sqlResponse.outputs(2);
        ASSERT_EQ("sql_search_info", output2.name());
        ASSERT_EQ("0", output2.port());
        ASSERT_TRUE(output2.has_data());

        Tensor tableTensor(DT_VARIANT, {1});
        ASSERT_TRUE(tableTensor.FromProto(output0.data()));
        auto tableVariant = tableTensor.scalar<Variant>()().get<SqlTableVariant>();
        ASSERT_TRUE(tableVariant != nullptr);
        std::shared_ptr<autil::mem_pool::Pool> pool(new autil::mem_pool::Pool());
        ASSERT_TRUE(tableVariant->construct(pool, pool.get()));
        auto table = tableVariant->getTable();
        ASSERT_TRUE(table != nullptr);
        ASSERT_EQ(values.size(), table->getRowCount());

        ColumnPtr column = table->getColumn(attrName);
        ASSERT_TRUE(column != NULL);
        ColumnData<T>* columnData = column->getColumnData<T>();
        for (size_t i = 0; i < values.size(); i++) {
            ASSERT_EQ(values[i], columnData->get(i));
        }
  }

private:
    void prepareAttributeMap() {
        _attributeMap.clear();
        JsonMap scanAttriMap;
        scanAttriMap["condition"] = ParseJson(R"json({"op":"=", "params":["$attr1",1]})json");
        scanAttriMap["table_name"] = string("invertedTable");
        scanAttriMap["table_type"] = string("normal");
        scanAttriMap["hash_fields"] = ParseJson(R"json(["$id"])json");
        scanAttriMap["index_infos"] =
            ParseJson(R"json({"$id" : {"type":"primarykey64","name":"id"} , "$index2" : {"type":"string", "name":"index_2"}, "$name" : {"type":"string", "name":"name"}})json");
        scanAttriMap["output_field_exprs"] =
            ParseJson(R"json({"$attr1":"$attr1", "$id": "$id"})json");
        scanAttriMap["output_fields"] = ParseJson(R"json(["$attr1", "$id"])json");
        scanAttriMap["use_nest_table"] = false;
        scanAttriMap["limit"] = Any(100);
        _attributeMap["scan"] = scanAttriMap;

        JsonMap sortAttriMap;
        sortAttriMap["order_fields"] = ParseJson(R"json(["$id"])json");
        sortAttriMap["directions"] = ParseJson(R"json(["DESC"])json");
        sortAttriMap["limit"] = Any(100);
        sortAttriMap["offset"] = Any(0);
        _attributeMap["sort"] = sortAttriMap;
    }

    string prepareInput() {
        SqlGraphRequest sqlRequest;
        auto graphInfo = sqlRequest.mutable_graphinfo();
        auto param = graphInfo->mutable_rungraphparams();
        (*param)[SUB_GRAPH_RUN_PARAM_TIMEOUT] = "1000";
        navi::GraphBuilder builder(graphInfo->mutable_graphdef());
        builder.node("scan").kernel("ScanKernel");
        builder.jsonAttrs(autil::legacy::ToJsonString(_attributeMap["scan"]));
        builder.node("sort").kernel("SortKernel");
        builder.in("input0").from("scan", "output0");
        builder.jsonAttrs(autil::legacy::ToJsonString(_attributeMap["sort"]));
        Target* target = graphInfo->add_targets();
        target->set_name("sort");
        target->set_port("output0");
        string requestStr;
        sqlRequest.SerializeToString(&requestStr);
        return requestStr;
    }

private:
    std::map<std::string, JsonMap> _attributeMap;
    navi::NaviPtr _navi;
    SqlBizResourcePtr _sqlBizResource;
private:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(turing, RunSqlOpTest);

TEST_F(RunSqlOpTest, testSimpleProcess) {
    addSqlSessionResource();
    prepareAttributeMap();
    string inputStr = prepareInput();
    makeOp();
    AddInputFromArray<string>(TensorShape({}), {inputStr});
    Status status = RunOpKernel();
    ASSERT_TRUE(status.ok());
    checkOutput<int64_t>({8, 7, 2}, "id");
}


END_HA3_NAMESPACE();
