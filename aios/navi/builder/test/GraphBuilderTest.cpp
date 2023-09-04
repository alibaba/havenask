#include "navi/builder/GraphBuilder.h"
#include "unittest/unittest.h"
#include "google/protobuf/util/json_util.h"
#include <fstream>

using namespace std;
using namespace testing;

namespace navi {

class GraphBuilderTest : public TESTBASE {
public:
    void setUp();
    void tearDown();
};

void GraphBuilderTest::setUp() {
}

void GraphBuilderTest::tearDown() {
}

TEST_F(GraphBuilderTest, testSimple) {
    std::unique_ptr<GraphDef> def(new GraphDef());
    GraphBuilder builder(def.get());
    builder.newSubGraph("bizName");
    auto n1 = builder.node("node1").kernel("kernel1").jsonAttrsFromMap({{"node1_key1", "node1_value1"}});
    auto n2 = builder.node("node2")
                  .kernel("kernel2")
                  .jsonAttrsFromMap({{"node2_key1", "node2_value1"}})
                  .binaryAttrsFromMap({{"node2_bin_key", "node2_bin_value"}});

    n2.in("node2_input1").from(n1.out("node1_output1"));
    n2.out("node2_output2").to(n1.in("node1_input2"));
    ASSERT_TRUE(builder.ok());
    std::cout << def->DebugString() << endl;
}

TEST_F(GraphBuilderTest, testMultiSubGraph) {
    std::unique_ptr<GraphDef> def(new GraphDef());
    GraphBuilder builder(def.get());
    builder.newSubGraph("main");
    auto n0 = builder.node("node4").kernel("kernel4");
    auto n1 = builder.node("node1").kernel("kernel1").jsonAttrsFromMap({{"node1_key1", "node1_value1"}});

    builder.newSubGraph("biz1");
    auto n2 = builder.node("node2")
                  .kernel("kernel2")
                  .jsonAttrsFromMap({{"node2_key1", "node2_value1"}})
                  .binaryAttrsFromMap({{"node2_bin_key", "node2_bin_value"}});

    builder.newSubGraph("biz2");
    auto n3 = builder.node("node3")
                  .kernel("kernel3")
                  .jsonAttrsFromMap({{"node3_key1", "node3_value1"}})
                  .binaryAttrsFromMap({{"node3_bin_key", "node3_bin_value"}});

    n0.out("output1").asGraphOutput("out");
    n2.out("node2_output1").to(n1.in("node1_input1"));
    n2.out("node2_output2").to(n3.in("node3_input1"));
    n3.out("node3_output1").to(n1.in("node1_input2"));

    ASSERT_TRUE(builder.ok());
    google::protobuf::util::JsonOptions options;
    options.add_whitespace = true;
    string graphStr;
    google::protobuf::util::MessageToJsonString(*def, &graphStr, options);
    std::cout << graphStr << endl;
}

TEST_F(GraphBuilderTest, testMultiSubGraph2) {
    std::unique_ptr<GraphDef> def(new GraphDef());
    GraphBuilder builder(def.get());
    std::string splitKernel("StringSplitKernel");
    std::string mergeKernel("StringMergeKernel");
    builder.newSubGraph("server_biz_1");
    auto n1 = builder.node("client_hello_2").kernel("WorldKernel");
    auto n2 = builder.node("client_world_2").kernel("WorldKernel");
    n2.out("output1").asGraphOutput("graph_out");

    builder.newSubGraph("server_biz");
    auto n3 = builder.node("source_node_2").kernel("SourceKernel").jsonAttrs(R"json({"times" : 20})json");

    builder.newSubGraph("server_biz_2");
    auto n4 = builder.node("remote_hello_2").kernel("WorldKernel");
    auto n5 = builder.node("remote_world_2").kernel("WorldKernel");

    builder.newSubGraph("client_biz");
    auto n6 = builder.node("client_remote_hello_2").kernel("WorldKernel");
    auto n7 = builder.node("client_remote_world_2").kernel("WorldKernel");

    auto p1 = n1.in("input1");
    auto e1 = n3.out("output1").autoNext().to(p1.autoNext());
    e1.split("split1").attr("key", "value");
    e1.merge("merge1").jsonAttrs(R"json({"times" : 21})json");
    auto e11 = n5.out("output2").to(p1.autoNext());
    auto e12 = n3.out("output1").autoNext().to(n2.in("input1").autoNext());
    auto e2 = n4.in("input1").from(n1.out("output1"));
    e2.split(splitKernel);
    e2.merge(mergeKernel);
    auto e3 = n5.in("input1").from(n4.out("output1"));
    e3.split(splitKernel);
    e3.merge(mergeKernel);
    auto e4 = n6.in("input1").from(n5.out("output1"));
    e4.split(splitKernel);
    e4.merge(mergeKernel);
    auto e5 = n7.in("input1").from(n6.out("output1"));
    e5.split(splitKernel);
    e5.merge(mergeKernel);
    auto e6 = n7.out("output1").to(n2.in("input1").autoNext());
    e6.split(splitKernel);
    e6.merge(mergeKernel);

    ASSERT_TRUE(builder.ok());

    std::string graphFileName("/home/zhang7/aios/navi/http_server/test_graph.def");
    std::ofstream graphFile(graphFileName);
    graphFile << def->SerializeAsString();

    google::protobuf::util::JsonOptions options;
    options.add_whitespace = true;
    string graphStr;
    google::protobuf::util::MessageToJsonString(*def, &graphStr, options);
    std::cout << graphStr << endl;
}

}
