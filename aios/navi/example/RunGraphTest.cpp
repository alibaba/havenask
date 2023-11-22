#include "unittest/unittest.h"
#include "autil/EnvUtil.h"
#include "navi/builder/GraphBuilder.h"
#include "navi/test_cluster/NaviTestCluster.h"
#include "navi/example/TestData.h"
#include "navi/example/TestResource.h"

using namespace std;
using namespace testing;

namespace navi {

struct NaviTestData {
    NaviTestData(const DataPtr &data, bool eof)
        : data(dynamic_pointer_cast<HelloData>(data))
        , eof(eof)
    {
    }
    HelloDataPtr data;
    bool eof;
};

class NaviDataResult {
public:
    void add(const NaviUserData &data) {
        auto &edgeMap = _result[data.name];
        auto &partResult = edgeMap[data.partId];
        partResult.emplace_back(data.data, data.eof);
    }
    void show() const {
        for (const auto &edgePair : _result) {
            std::cout << "edge: " << edgePair.first
                      << ", part num: " << edgePair.second.size() << std::endl;
            for (const auto &partPair : edgePair.second) {
                std::cout << "   part: " << partPair.first
                          << " result: " << std::endl;
                const auto &dataVec = partPair.second;
                for (size_t i = 0; i < dataVec.size(); i++) {
                    const auto &data = dataVec[i];
                    std::cout << "      data " << i << ", eof: " << data.eof
                              << std::endl;
                    std::cout << "        ";
                    data.data->show();
                }
            }
        }
    }
public:
    std::map<std::string, std::map<NaviPartId, std::vector<NaviTestData> > >
        _result;
};

class RunGraphTest : public TESTBASE {
public:
    void setUp();
    void tearDown();
    bool addBiz(const std::string &host, const std::string &biz,
                NaviPartId partCount, NaviPartId partId);
    bool addServer(const std::string &host);
    void runGraph(const std::string &host,
                  GraphDef *graphDef,
                  NaviDataResult &dataResult,
                  NaviResultPtr &naviResult);
private:
    void getNavi(const std::string &host, NaviPtr &navi);
    void collectResult(const NaviUserResultPtr &result,
                       NaviDataResult &dataResult);
    std::string getConfigPath() const {
        return NAVI_TEST_DATA_PATH + "config/" + _configName;
    }
    std::string getBizConfig() const {
        return getConfigPath() + "/" + _bizConfig;
    }
protected:
    std::string _loader;
    std::string _configName;
    std::string _bizConfig;
    ResourceMapPtr _rootResourceMap;
    NaviTestCluster *_cluster;
private:
    std::unique_ptr<autil::EnvGuard> _naviPythonHome;
};

void RunGraphTest::setUp() {
    _naviPythonHome.reset(
        new autil::EnvGuard("NAVI_PYTHON_HOME", NAVI_TEST_PYTHON_HOME));
    _loader = NAVI_TEST_DATA_PATH + "test_config_loader.py";
    _configName = "run_graph_test";
    _bizConfig = "biz1.py";
    _rootResourceMap.reset(new ResourceMap());
    _cluster = new NaviTestCluster();
}

void RunGraphTest::tearDown() {
    delete _cluster;
}

bool RunGraphTest::addBiz(const std::string &host, const std::string &biz,
                          NaviPartId partCount, NaviPartId partId)
{
    if (!_cluster->addServer(host, _loader, getConfigPath(), _rootResourceMap)) {
        return false;
    }
    if (!_cluster->addBiz(host, biz, partCount, partId, getBizConfig())) {
        return false;
    }
    return true;
}

bool RunGraphTest::addServer(const std::string &host) {
    return !_cluster->addServer(host, _loader, getConfigPath(), _rootResourceMap);
}

void RunGraphTest::getNavi(const std::string &host, NaviPtr &navi) {
    ASSERT_TRUE(_cluster->start());
    navi = _cluster->getNavi(host);
}

void RunGraphTest::collectResult(const NaviUserResultPtr &result,
                                 NaviDataResult &dataResult)
{
    while (true) {
        NaviUserData data;
        bool eof = false;
        if (result->nextData(data, eof)) {
            dataResult.add(data);
        }
        if (eof) {
            break;
        }
    }
}

void RunGraphTest::runGraph(const std::string &host,
                            GraphDef *graphDef,
                            NaviDataResult &dataResult,
                            NaviResultPtr &naviResult)
{
    RunGraphParams params;
    params.setTimeoutMs(1000000000);
    params.setTraceLevel("ERROR");
    params.setCollectMetric(true);
    NaviPtr navi;
    ASSERT_NO_FATAL_FAILURE(getNavi(host, navi));
    auto naviUserResult = navi->runGraph(graphDef, params);
    collectResult(naviUserResult, dataResult);
    naviResult = naviUserResult->getNaviResult();
}

TEST_F(RunGraphTest, testGraph_EmptyGraph) {
    ASSERT_TRUE(addBiz("host_0", "biz", 1, 0));
    auto graphDef = new GraphDef();
    NaviDataResult dataResult;
    NaviResultPtr result;
    ASSERT_NO_FATAL_FAILURE(runGraph("host_0", graphDef, dataResult, result));
    ASSERT_EQ(EC_NO_GRAPH_OUTPUT, result->getErrorCode());
    ASSERT_EQ("graph has no output", result->getErrorMessage());
    ASSERT_TRUE(dataResult._result.empty());
}

TEST_F(RunGraphTest, testGraph_NoOutput) {
    ASSERT_TRUE(addBiz("host_0", "biz", 1, 0));
    auto graphDef = new GraphDef();
    GraphBuilder builder(graphDef);
    builder.newSubGraph("biz");
    auto n1 = builder.node("node1").kernel("SourceKernel").jsonAttrs(R"json({"times" : 20})json");
    builder.newSubGraph("biz");
    builder.node("node2").kernel("HelloKernel").in("input1").from(n1.out("output1"));
    ASSERT_TRUE(builder.ok());

    NaviDataResult dataResult;
    NaviResultPtr result;
    ASSERT_NO_FATAL_FAILURE(runGraph("host_0", graphDef, dataResult, result));
    ASSERT_EQ(EC_NO_GRAPH_OUTPUT, result->getErrorCode());
    EXPECT_EQ("graph has no output", result->getErrorMessage());
    EXPECT_TRUE(dataResult._result.empty());
}

TEST_F(RunGraphTest, testGraph_KernelNotExist) {
    ASSERT_TRUE(addBiz("host_0", "biz", 1, 0));
    auto graphDef = new GraphDef();
    GraphBuilder builder(graphDef);
    builder.newSubGraph("biz");
    auto n1 = builder.node("node1").kernel("SourceKernel").jsonAttrs(R"json({"times" : 20})json");
    builder.newSubGraph("biz");
    builder.node("node2").kernel("NotExistKernel").in("input1").from(n1.out("output1"));
    ASSERT_TRUE(builder.ok());

    NaviDataResult dataResult;
    NaviResultPtr result;
    ASSERT_NO_FATAL_FAILURE(runGraph("host_0", graphDef, dataResult, result));
    ASSERT_EQ(EC_CREATE_KERNEL, result->getErrorCode());
    EXPECT_EQ("kernel [NotExistKernel] not registered",
              result->getErrorMessage());
    EXPECT_TRUE(dataResult._result.empty());
}

TEST_F(RunGraphTest, testGraph_OpenKernelPort) {
    ASSERT_TRUE(addBiz("host_0", "biz", 1, 0));
    auto graphDef = new GraphDef();
    GraphBuilder builder(graphDef);
    builder.newSubGraph("biz");
    builder.node("hello").kernel("HelloKernel").out("output1").asGraphOutput("o");
    builder.node("world").kernel("WorldKernel");
    ASSERT_TRUE(builder.ok());

    NaviDataResult dataResult;
    NaviResultPtr result;
    ASSERT_NO_FATAL_FAILURE(runGraph("host_0", graphDef, dataResult, result));
    ASSERT_EQ(EC_INIT_GRAPH, result->getErrorCode());
    ASSERT_EQ("input port[input1] not connected", result->getErrorMessage());
    ASSERT_TRUE(dataResult._result.empty());
}

TEST_F(RunGraphTest, testGraph_NodeDuplicated) {
    // todo GraphBuilder
}

TEST_F(RunGraphTest, testGraph_PortNotExist) {
    ASSERT_TRUE(addBiz("host_0", "biz", 1, 0));
    {
        auto graphDef = new GraphDef();
        GraphBuilder builder(graphDef);
        builder.newSubGraph("biz");
        auto source = builder.node("source").kernel("SourceKernel").jsonAttrs(R"json({"times" : 20})json");
        auto hello = builder.node("hello").kernel("HelloKernel");
        source.out("out").to(hello.in("input1"));
        hello.out("output1").asGraphOutput("o");
        ASSERT_TRUE(builder.ok());

        NaviDataResult dataResult;
        NaviResultPtr result;
        ASSERT_NO_FATAL_FAILURE(runGraph("host_0", graphDef, dataResult, result));
        ASSERT_EQ(EC_INIT_GRAPH, result->getErrorCode());
        EXPECT_EQ(
            "no output port named [out], kernel def[version: 196608\nkernel_name: \"SourceKernel\"\noutputs {\n  name: \"output1\"\n  "
            "data_type {\n    name: \"CharArrayType\"\n  }\n}\nresource_depend_info {\n  depend_resources {\n    name: "
            "\"navi.buildin.resource.graph_mem_pool\"\n    require: true\n  }\n}\n]",
            result->getErrorMessage());
        EXPECT_EQ(0u, dataResult._result.size());
    }
    {
        auto graphDef = new GraphDef();
        GraphBuilder builder(graphDef);
        builder.newSubGraph("biz");
        auto source = builder.node("source").kernel("SourceKernel").jsonAttrs(R"json({"times" : 20})json");
        auto hello = builder.node("hello").kernel("HelloKernel");
        source.out("output1").to(hello.in("in"));
        hello.out("output1").asGraphOutput("o");
        ASSERT_TRUE(builder.ok());

        NaviDataResult dataResult;
        NaviResultPtr result;
        ASSERT_NO_FATAL_FAILURE(runGraph("host_0", graphDef, dataResult, result));
        ASSERT_EQ(EC_INIT_GRAPH, result->getErrorCode());
        EXPECT_EQ("no input port named [in], check port name or group index, kernel def[version: 196608\nkernel_name: "
                  "\"HelloKernel\"\ninputs {\n  name: \"input1\"\n  data_type {\n    name: \"CharArrayType\"\n  "
                  "}\n}\noutputs {\n  name: \"output1\"\n  data_type {\n    name: \"CharArrayType\"\n  "
                  "}\n}\nresource_depend_info {\n  depend_resources {\n    name: \"A\"\n    require: true\n  }\n  "
                  "depend_resources {\n    name: \"External\"\n  }\n  depend_resources {\n    name: "
                  "\"navi.buildin.resource.graph_mem_pool\"\n    require: true\n  }\n}\n]",
                  result->getErrorMessage());
        EXPECT_EQ(0u, dataResult._result.size());
    }
}

TEST_F(RunGraphTest, testGraph_PortMultiConnect) {
    ASSERT_TRUE(addBiz("host_0", "biz", 1, 0));
    {
        try {
            std::unique_ptr<GraphDef> graphDef(new GraphDef());
            GraphBuilder builder(graphDef.get());
            builder.newSubGraph("biz");
            auto source1 = builder.node("source1").kernel("SourceKernel").jsonAttrs(R"json({"times" : 20})json");
            auto source2 = builder.node("source2").kernel("SourceKernel").jsonAttrs(R"json({"times" : 29})json");
            auto hello = builder.node("hello").kernel("HelloKernel");
            auto inputPort = hello.in("input1").autoNext();
            source1.out("output1").to(inputPort);
            source2.out("output1").to(inputPort);
            hello.out("output1").asGraphOutput("o");
            ASSERT_TRUE(builder.ok());
            ASSERT_FALSE("can't reach");
        } catch (const autil::legacy::ExceptionBase &e) {
            EXPECT_EQ("error, double connection to: graph 0 node hello kernel HelloKernel port input1 index 0, first: "
                      "graph 0 node source1 kernel SourceKernel port output1, second: graph 0 node source2 kernel "
                      "SourceKernel port output1",
                      e.GetMessage());
        }
    }
}

TEST_F(RunGraphTest, testGraph_TypeNotRegistered) {
    ASSERT_TRUE(addBiz("host_0", "biz", 1, 0));
    {
        // edge type not registered
    }
    {
        // port type not registered
        auto graphDef = new GraphDef();
        GraphBuilder builder(graphDef);
        builder.newSubGraph("biz");
        auto source = builder.node("source").kernel("BadOutputTypeKernel");

        builder.newSubGraph("biz");
        auto hello = builder.node("hello").kernel("HelloKernel");
        hello.in("input1").from(source.out("output1"));

        hello.out("output1").asGraphOutput("o1");
        ASSERT_TRUE(builder.ok());

        NaviDataResult dataResult;
        NaviResultPtr result;
        ASSERT_NO_FATAL_FAILURE(runGraph("host_0", graphDef, dataResult, result));
        ASSERT_TRUE(EC_NONE != result->getErrorCode());
        ASSERT_EQ(
            "port data type [not_exist_output_type] not registered, node "
            "[source] kernel [BadOutputTypeKernel], output port [output1]",
            result->getErrorMessage());
    }
}

TEST_F(RunGraphTest, testGraph_TypeMismatch) {
    ASSERT_TRUE(addBiz("host_0", "biz", 1, 0));
    {
        // edge type mismatch
        auto graphDef = new GraphDef();
        GraphBuilder builder(graphDef);
        builder.newSubGraph("biz");
        auto source = builder.node("source").kernel("BadOutputTypeKernel");
        auto hello = builder.node("hello").kernel("HelloKernel");
        hello.in("input1").from(source.out("output1"));

        hello.out("output1").asGraphOutput("o1");
        ASSERT_TRUE(builder.ok());

        NaviDataResult dataResult;
        NaviResultPtr result;
        ASSERT_NO_FATAL_FAILURE(runGraph("host_0", graphDef, dataResult, result));
        EXPECT_NE(EC_NONE, result->getErrorCode());
        EXPECT_EQ("edge data type mismatch, input kernel[BadOutputTypeKernel] "
                  "type[not_exist_output_type], output [node hello "
                  "kernel HelloKernel] port[input1] type[CharArrayType]",
                  result->getErrorMessage());
        EXPECT_EQ("edge source:output1", result->getErrorPrefix());
    }
    // {
    //     // cross SubGraph type mismatch
    //     auto graphDef = new GraphDef();
    //     GraphBuilder builder(graphDef);
    //     auto mainGraph = builder.newSubGraph("biz");
    //     builder.subGraph(mainGraph)
    //         .node("source")
    //         .kernel("IntArrayTypeKernel");

    //     auto graph1 = builder.newSubGraph("biz");

    //     builder.subGraph(graph1)
    //         .node("hello")
    //         .kernel("HelloKernel")
    //         .in("input1")
    //         .from(mainGraph, "source", "output1", true, "", "")

    //         .graphOutput("o1", graph1, "hello", "output1");
    //     ASSERT_TRUE(builder.ok());

    //     NaviDataResult dataResult;
    //     auto result = runGraph("host_0", graphDef, dataResult);
    //     ASSERT_TRUE(EC_NONE != result->ec);
    //     ASSERT_EQ("port data type mismatch, input node[source] "
    //               "kernel[BadOutputTypeKernel] port[output1] "
    //               "type[not_exist_output_type], output node[hello] "
    //               "kernel[HelloKernel] port[input1] type[CharArrayType]",
    //               result->getErrorMessage());
    // }
    {
        // distribute port type mismatch
    }
}

TEST_F(RunGraphTest, testGig_BizNotExist) {
    ASSERT_TRUE(addBiz("host_0", "biz", 1, 0));
    auto graphDef = new GraphDef();
    GraphBuilder builder(graphDef);
    builder.newSubGraph("biz2");
    builder.node("source")
        .kernel("SourceKernel")
        .jsonAttrs(R"json({"times" : 20})json")
        .out("output1")
        .asGraphOutput("o1");
    ASSERT_TRUE(builder.ok());

    NaviDataResult dataResult;
    NaviResultPtr result;
    ASSERT_NO_FATAL_FAILURE(runGraph("host_0", graphDef, dataResult, result));
    EXPECT_TRUE(EC_NONE != result->getErrorCode());
    EXPECT_EQ("init gig stream failed, biz: biz2", result->getErrorMessage());
    EXPECT_EQ("domain GDT_CLIENT biz: biz2", result->getErrorPrefix());
}

}
