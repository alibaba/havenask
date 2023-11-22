/*
 * Copyright 2014-present Alibaba Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#include "autil/StringUtil.h"
#include "autil/EnvUtil.h"
#include "autil/ThreadPool.h"
#include "fslib/fslib.h"
#include "navi/builder/GraphBuilder.h"
#include "navi/engine/Navi.h"
#include "navi/example/TestData.h"
#include "navi/example/TestResource.h"
#include "navi/proto/GraphDef.pb.h"
#include "navi/proto/GraphVis.pb.h"
#include "navi/test_cluster/NaviTestCluster.h"
#include "navi/util/CommonUtil.h"
#include "unittest/unittest.h"
#include <arpc/common/LockFreeQueue.h>
#include <fstream>
#include <iostream>
#include <unistd.h>

using namespace std;

namespace navi {

class ModuleManager;

class NaviExampleTest : public TESTBASE {
public:
    void setUp();
    void tearDown();
protected:
    std::string _loader;
    GraphDef *_graphDef = nullptr;
private:
    std::unique_ptr<autil::EnvGuard> _naviPythonHome;
public:
    static const size_t TEST_COUNT = 1;
};

void NaviExampleTest::setUp() {
    // for aios test
    _naviPythonHome.reset(
        new autil::EnvGuard("NAVI_PYTHON_HOME", NAVI_TEST_PYTHON_HOME));
    _loader = NAVI_TEST_DATA_PATH + "test_config_loader.py";
    _graphDef = new GraphDef();
}

void NaviExampleTest::tearDown() {
    DELETE_AND_SET_NULL(_graphDef);
}

NaviResultPtr showData(const NaviUserResultPtr &result, bool show, int64_t expectCount,
                       int64_t &dataCount, std::vector<NaviUserData> &dataVec,
                       std::vector<NaviResultPtr> &resultVec)
{
    while (true) {
        NaviUserData data;
        bool eof = false;
        if (result->nextData(data, eof)) {
            // if (data.data) {
            //     std::cout << data.name << ": ";
            //     data.data->show();
            // }
            dataVec.push_back(data);
        }
        if (eof) {
            break;
        }
    }
    auto naviResult = result->getNaviResult();
    resultVec.push_back(naviResult);
    return naviResult;
}

void showResultAndSaveVis(const NaviUserResultPtr &userResult) {
    // auto visProto = userResult->getNaviResult()->getVisProto();
    // std::ofstream ofile(
    //     "/home/zhang7/alibaba/aios/navi/http_server/test_graph.vis");
    // ofile << visProto->SerializeAsString();
    auto rpcInfoMap = userResult->getNaviResult()->getRpcInfoMap();
    ASSERT_TRUE(rpcInfoMap);
    std::cout << "rpcInfo: " << std::endl;
    for (const auto &pair : *rpcInfoMap) {
        std::cout << "[" << pair.first.first << "->" << pair.first.second
                  << "] " << std::endl
                  << autil::StringUtil::toString(pair.second, "\n")
                  << std::endl;
    }
}

static std::string randItem(const std::vector<std::string> &itemList) {
    return itemList[0];
    return "server_biz_1";
    auto rand = CommonUtil::random64();
    return itemList[rand % itemList.size()];
}

bool buildGraph1(const std::string &outputName,
                 GraphDef *graphDef,
                 const std::vector<std::string> &bizList,
                 OverrideData &overrideData)
{
    std::string splitKernel("StringSplitKernel");
    std::string mergeKernel("StringMergeKernel");
    GraphBuilder builder(graphDef);
    builder.newSubGraph(randItem(bizList));
    auto clientWorld1 = builder.node("client_world_1").kernel("HelloKernel");
    auto clientHello1 = builder.node("client_hello_1").kernel("WorldKernel");
    auto clientHelloDup1 = builder.node("client_hello_dup_1").kernel("WorldKernel");
    auto fork = builder.node("fork").kernel("SubGraphKernel");
    auto afterFork = builder.node("after_fork").kernel("WorldKernel");
    clientWorld1.out("output1").to(clientHello1.in("input1"));
    fork.in("input1").from(clientHelloDup1.out("output1"));
    afterFork.in("input1").from(fork.out("output1"));
    clientHello1.out("output1").asGraphOutput(outputName);
    afterFork.out("output1").asGraphOutput(outputName + "_dup");
    fork.out("output2").asGraphOutput(outputName + "_dupdup");

    builder.newSubGraph(randItem(bizList));
    auto source1 = builder.node("source_node_1").kernel("SourceKernel").jsonAttrs(R"json({"times" : 10})json");
    source1.out("output1").to(clientWorld1.in("input1")).merge(mergeKernel);
    source1.out("output1").to(clientHelloDup1.in("input1")).merge(mergeKernel);

    auto sleep = builder.node("sleep").kernel("SleepKernel");
    sleep.in("input1").from(source1.out("output1"));

    clientHelloDup1.dependOn(sleep.out("output1"));
    return builder.ok();
}

bool buildGraph2(const std::string &outputName,
                 GraphDef *graphDef,
                 const std::vector<std::string> &bizList)
{
    std::string splitKernel("StringSplitKernel");
    std::string mergeKernel("StringMergeKernel");
    GraphBuilder builder(graphDef);
    builder.newSubGraph("server_biz_1");
    auto clientHello2 = builder.node("client_hello_2").kernel(randItem({"WorldKernel", "SubGraphKernel"}));
    auto clientWorld2 = builder.node("client_world_2").kernel(randItem({"WorldKernel", "SubGraphKernel"}));
    clientWorld2.out("output1").asGraphOutput(outputName);

    builder.newSubGraph("server_biz");
    auto source2 = builder.node("source_node_2")
                   .kernel("SourceKernel")
                   .jsonAttrs(R"json({"times" : 20})json");
    auto sourceE = source2.out("output1").to(clientHello2.in("input1"));
    sourceE.split(splitKernel);
    sourceE.merge(mergeKernel);

    builder.newSubGraph("server_biz_2");
    auto remoteHello2 = builder.node("remote_hello_2")
                        .kernel(randItem({"WorldKernel", "SubGraphKernel"}));
    auto remoteE = remoteHello2.in("input1").from(clientHello2.out("output1"));
    remoteE.split(splitKernel);
    remoteE.merge(mergeKernel);

    auto remoteWorld2 = builder.node("remote_world_2").kernel(randItem({"WorldKernel", "SubGraphKernel"}));
    remoteWorld2.in("input1").from(remoteHello2.out("output1"));

    builder.newSubGraph("client_biz");
    auto remoteHello22 = builder.node("remote_hello_2")
                         .kernel(randItem({"WorldKernel", "SubGraphKernel"}));
    auto remote22E = remoteHello22.in("input1").from(remoteWorld2.out("output1"));
    remote22E.split(splitKernel);
    remote22E.merge(mergeKernel);

    auto remoteWorld22 = builder.node("remote_world_2").kernel(randItem({"WorldKernel", "SubGraphKernel"}));
    remoteWorld22.in("input1").from(remoteHello22.out("output1"));

    auto e3 = remoteWorld22.out("output1").to(clientWorld2.in("input1"));
    e3.split(splitKernel);
    e3.merge(mergeKernel);
    return builder.ok();
}

bool buildGraph3(const std::string &outputName,
                 GraphDef *graphDef,
                 const std::vector<std::string> &bizList)
{
    std::string splitKernel("StringSplitKernel");
    std::string mergeKernel("StringMergeKernel");
    GraphBuilder builder(graphDef);
    builder.newSubGraph("server_biz_1");
    auto source1 = builder.node("source_node_1").kernel("TFSourceKernel").jsonAttrs(R"json({"times" : 20})json");
    auto clientWorld3 = builder.node("client_world_3").kernel("tf.TFIdentityOp");
    clientWorld3.in("input_content").from(source1.out("output1"));
    clientWorld3.in("input_list").autoNext().from(source1.out("output1"));
    clientWorld3.in("input_list").autoNext().from(source1.out("output1"));
    clientWorld3.in("input_list").autoNext().from(source1.out("output1"));

    size_t count = 20 + 10000;
    for (size_t i = 4; i < count; i++) {
        std::string current = "client_world_" + autil::StringUtil::toString(i);
        auto pre = builder.node("client_world_" + autil::StringUtil::toString(i - 1));
        auto n = builder.node(current).kernel("tf.TFIdentityOp");
        n.in("input_content").from(pre.out("output_content"));
        n.in("input_list").autoNext().from(pre.out("output_list").autoNext());
        n.in("input_list").autoNext().from(pre.out("output_list").autoNext());
        n.in("input_list").autoNext().from(pre.out("output_list").autoNext());
    }

    std::string outputNode = "client_world_" + autil::StringUtil::toString(count - 1);
    auto outN = builder.node(outputNode);
    outN.out("output_content").asGraphOutput(outputName);
    outN.out("output_list").index(2).asGraphOutput(outputName + "_ccc");
    outN.out("output_list").index(0).asGraphOutput(outputName + "_aaa");
    outN.out("output_list").index(1).asGraphOutput(outputName + "_bbb");
    outN.out("output_list").index(1).asGraphOutput(outputName + "_ddd");

    return builder.ok();
}

bool buildGraph4(const std::string &outputName,
                 GraphDef *graphDef,
                 const std::vector<std::string> &bizList)
{
    std::string splitKernel("StringSplitKernel");
    std::string mergeKernel("StringMergeKernel");
    GraphBuilder builder(graphDef);
    builder.newSubGraph("server_biz_1");
    auto clientWorld1 = builder.node("client_world_1").kernel("WorldKernel");
    auto clientHello1 = builder.node("client_hello_1").kernel("HelloKernel");

    clientWorld1.out("output1").asGraphOutput(outputName);

    builder.newSubGraph(randItem(bizList));
    auto source1 = builder.node("source_node_1").kernel("SourceKernel").jsonAttrs(R"json({"times" : 20})json");
    source1.out("output1").to(clientHello1.in("input1"));

    auto remoteHello1 = builder.node("remote_hello_1").kernel("HelloKernel");
    auto e1 = remoteHello1.in("input1").from(clientHello1.out("output1"));
    e1.split(splitKernel);
    e1.merge(mergeKernel);
    auto e2 = remoteHello1.out("output1").to(clientWorld1.in("input1"));
    e2.split(splitKernel);
    e2.merge(mergeKernel);

    return builder.ok();
}

TEST_F(NaviExampleTest, testDistributeChain) {
    ResourceMapPtr rootResourceMap(new ResourceMap());

    std::string configPath = NAVI_TEST_DATA_PATH + "config/cluster/";
    std::string biz1Config = NAVI_TEST_DATA_PATH + "config/cluster/biz1.py";
    NaviTestCluster cluster;
    ASSERT_TRUE(
            cluster.addServer("host_0", _loader, configPath, rootResourceMap));
    ASSERT_TRUE(
            cluster.addServer("host_1", _loader, configPath, rootResourceMap));
    ASSERT_TRUE(
            cluster.addServer("host_2", _loader, configPath, rootResourceMap));
    ASSERT_TRUE(
            cluster.addServer("host_3", _loader, configPath, rootResourceMap));
    ASSERT_TRUE(
            cluster.addServer("host_4", _loader, configPath, rootResourceMap));
    ASSERT_TRUE(
            cluster.addServer("host_5", _loader, configPath, rootResourceMap));
    ASSERT_TRUE(
            cluster.addServer("host_6", _loader, configPath, rootResourceMap));
    ASSERT_TRUE(
            cluster.addServer("host_7", _loader, configPath, rootResourceMap));

    std::vector<std::string> hostList = { "host_0", "host_1", "host_2",
                                          "host_3", "host_4", "host_5",
                                          "host_6", "host_7" };
    ASSERT_TRUE(cluster.addBiz("host_1", "server_biz", 1, 0, biz1Config));
    // ASSERT_TRUE(cluster.addBiz("host_2", "server_biz", 3, 1, biz1Config));
    // ASSERT_TRUE(cluster.addBiz("host_3", "server_biz", 3, 2, biz1Config));

    ASSERT_TRUE(cluster.addBiz("host_4", "server_biz_2", 2, 0, biz1Config));
    ASSERT_TRUE(cluster.addBiz("host_5", "server_biz_2", 2, 1, biz1Config));

    ASSERT_TRUE(cluster.addBiz("host_5", "server_biz_1", 4, 0, biz1Config));
    ASSERT_TRUE(cluster.addBiz("host_6", "server_biz_1", 4, 1, biz1Config));
    ASSERT_TRUE(cluster.addBiz("host_3", "server_biz_1", 4, 2, biz1Config));
    ASSERT_TRUE(cluster.addBiz("host_4", "server_biz_1", 4, 3, biz1Config));

    ASSERT_TRUE(cluster.addBiz("host_7", "client_biz", 1, 0, biz1Config));
    ASSERT_TRUE(cluster.start());

    rootResourceMap.reset();

    auto bizList = cluster.getBizList();
    EXPECT_TRUE(buildGraph2("o1", _graphDef, bizList));

    RunGraphParams params;
    params.setTimeoutMs(10000000);
    params.setTraceLevel("debug");
    params.setCollectMetric(true);
    params.setCollectPerf(true);

    enum RunMode {
        RM_SINGLE = 1,
        RM_PERF = 2,
    };
    RunMode mode= RM_PERF;
    size_t queryCount = 1;
    size_t threadCount = 1;
    bool show = false;
    if (RM_SINGLE == mode) {
        show = true;
    } else if (RM_PERF == mode) {
        string perfParamEnvStr = autil::EnvUtil::getEnv("perf_param");
        if (!perfParamEnvStr.empty()) {
            auto vec = autil::StringUtil::split(perfParamEnvStr, ";");
            ASSERT_EQ(2u, vec.size());
            queryCount = autil::StringUtil::fromString<int32_t>(vec[0]);
            threadCount = autil::StringUtil::fromString<int32_t>(vec[1]);
        } else {
            queryCount = 1;
            threadCount = 1;
        }
    }
    NaviLoggerProvider logProvider("error");
    bool abort = false;
    atomic64_t counter;
    atomic_set(&counter, 0);
    // std::cout << _graphDef->DebugString() << std::endl;
    std::ofstream ofile("/home/zhang7/aios/navi/http_server/test_graph.def");
    ofile << _graphDef->SerializeAsString();
    auto func = std::bind([&]() {
        for (size_t i = 0; i < queryCount; i++) {
            // std::cout << "begin round " << i << std::endl;
            if (abort) {
                break;
            }
            auto thisShow = show;
            thisShow = true;
            auto current = atomic_inc_return(&counter);
            if (current == 2000) {
                // thisShow = true;
                std::cout << "show true" << std::endl;
                // params.setTraceLevel("");
            }
            // if (i % 200 == 0) {
            //     std::cout << "thread: " << pthread_self() << ", round: " << i << std::endl;
            // }
            // std::string fileContent;
            // EXPECT_EQ(fslib::EC_OK,
            //           fslib::fs::FileSystem::readFile(
            //               "/home/zhang7/aios/aios/navi/graph.serialize",
            //               fileContent));
            std::unique_ptr<GraphDef> defPtr(new GraphDef());
            auto def = defPtr.get();
            // def->CopyFrom(*_graphDef);
            bool load = false;
            OverrideData overrideData;
            if (load) {
                // ASSERT_TRUE(def->ParseFromString(fileContent));
            } else {
                // EXPECT_TRUE(buildGraph1("o1", def, bizList, overrideData));
                // EXPECT_TRUE(buildGraph2("o2", def, bizList));
                // EXPECT_TRUE(buildGraph2("o22", def, bizList));
                // EXPECT_TRUE(buildGraph2("o3", def, bizList));
                // EXPECT_TRUE(buildGraph2("o4", def, bizList));
                // EXPECT_TRUE(buildGraph2("o5", def, bizList));
                // EXPECT_TRUE(buildGraph2("o6", def, bizList));
                // EXPECT_TRUE(buildGraph2("o7", def, bizList));
                // EXPECT_TRUE(buildGraph1("o8", def, bizList, overrideData));
                // EXPECT_TRUE(buildGraph2("o9", def, bizList));
                // EXPECT_TRUE(buildGraph1("o10", def, bizList, overrideData));
                // EXPECT_TRUE(buildGraph2("o11", def, bizList));
                // EXPECT_TRUE(buildGraph2("o12", def, bizList));
                EXPECT_TRUE(buildGraph4("o13", def, bizList));
                EXPECT_TRUE(buildGraph4("o14", def, bizList));
            }
            int64_t expectCount = 20;
            int64_t dataCount = 0;
            auto graphStr = def->SerializeAsString();
            auto host = randItem(hostList);
            // std::cout << host << std::endl;
            auto navi = cluster.getNavi("host_1");
            auto runParams = params;
            // runParams.overrideEdgeData(overrideData);
            auto t1 = autil::TimeUtility::currentTime();
            auto userResult = navi->runGraph(defPtr.release(), runParams);
            auto t2 = autil::TimeUtility::currentTime();
            std::cout << "t2 - t1: " << t2 - t1 << std::endl;
            std::vector<NaviUserData> dataVec;
            std::vector<NaviResultPtr> resultVec;
            auto naviResult = showData(userResult, thisShow, expectCount,
                                       dataCount, dataVec, resultVec);
            dataVec.clear();
            resultVec.clear();
            showResultAndSaveVis(userResult);
        }
        std::cout << "thread exited" << std::endl;
    });
    std::vector<autil::ThreadPtr> threads;
    for (size_t i = 0; i < threadCount; i++) {
        threads.push_back(autil::Thread::createThread(
                        func, "test_thread_" + autil::StringUtil::toString(i)));
    }
    threads.clear();
    // usleep(20000 * 1000);
    // for (const auto &data : dataVec) {
    //     std::cout << "session: "
    //               << CommonUtil::formatSessionId(data.id)
    //               << std::endl;
    //     if (data.data) {
    //         std::cout << "node: " << data.node << ", port: " << data.port
    //                   << ": ";
    //         data.data->show();
    //     } else {
    //         std::cout << "null data" << std::endl;
    //     }
    // }
    // for (const auto &result : resultVec) {
    //     result->show();
    // }
    std::cout << "begin destruct" << std::endl;
    // usleep(1 * 1000 * 1000);
}

bool buildGraphFork(const std::string &outputName,
                    GraphDef *graphDef,
                    const std::vector<std::string> &bizList)
{
    GraphBuilder builder(graphDef);
    builder.newSubGraph(randItem(bizList));
    auto sourceFork = builder.node("source_node_fork").kernel("SourceKernel").jsonAttrs(R"json({"times" : 20})json");
    auto currentScope = builder.getCurrentScope();
    builder.addScope();
    auto fork = builder.node("fork").kernel("SubGraphKernel");
    builder.scope(currentScope);
    auto clientWorldFork = builder.node("client_world_fork").kernel("WorldKernel");

    fork.in("input1").from(sourceFork.out("output1"));
    clientWorldFork.in("input1").from(fork.out("output2"));
    clientWorldFork.out("output1").asGraphOutput(outputName);

    return builder.ok();
}

TEST_F(NaviExampleTest, testFork) {
    ResourceMapPtr rootResourceMap(new ResourceMap());

    std::string configPath = NAVI_TEST_DATA_PATH + "config/cluster/";
    std::string biz1Config = NAVI_TEST_DATA_PATH + "config/cluster/biz1.py";
    NaviTestCluster cluster;
    ASSERT_TRUE(
            cluster.addServer("host_0", _loader, configPath, rootResourceMap));
    ASSERT_TRUE(
            cluster.addServer("host_1", _loader, configPath, rootResourceMap));
    ASSERT_TRUE(
            cluster.addServer("host_2", _loader, configPath, rootResourceMap));
    ASSERT_TRUE(
            cluster.addServer("host_3", _loader, configPath, rootResourceMap));

    std::vector<std::string> hostList = { "host_0", "host_1", "host_2",
                                          "host_3", "host_4", "host_5",
                                          "host_6", "host_7" };
    ASSERT_TRUE(cluster.addBiz("host_0", "client_biz", 1, 0, biz1Config));
    ASSERT_TRUE(cluster.addBiz("host_1", "server_biz_1", 3, 0, biz1Config));
    ASSERT_TRUE(cluster.addBiz("host_2", "server_biz_1", 3, 1, biz1Config));
    ASSERT_TRUE(cluster.addBiz("host_3", "server_biz_1", 3, 2, biz1Config));
    ASSERT_TRUE(cluster.start());

    rootResourceMap.reset();

    auto bizList = cluster.getBizList();
    EXPECT_TRUE(buildGraphFork("o1", _graphDef, { "client_biz" }));

    RunGraphParams params;
    params.setTimeoutMs(1000000000);
    params.setTraceLevel("schedule1");
    params.setCollectMetric(true);
    params.setCollectPerf(true);

    enum RunMode {
        RM_SINGLE = 1,
        RM_PERF = 2,
    };
    RunMode mode= RM_SINGLE;

    size_t queryCount = 1;
    size_t threadCount = 1;
    bool show = false;
    if (RM_SINGLE == mode) {
        show = true;
    } else if (RM_PERF == mode) {
        string perfParamEnvStr = autil::EnvUtil::getEnv("perf_param");
        if (!perfParamEnvStr.empty()) {
            auto vec = autil::StringUtil::split(perfParamEnvStr, ";");
            ASSERT_EQ(2u, vec.size());
            queryCount = autil::StringUtil::fromString<int32_t>(vec[0]);
            threadCount = autil::StringUtil::fromString<int32_t>(vec[1]);
        } else {
            queryCount = 1000000;
            threadCount = 1;
        }
    }
    bool abort = false;
    atomic64_t counter;
    atomic_set(&counter, 0);
    // std::cout << _graphDef->DebugString() << std::endl;
    std::ofstream ofile("./graph.serialize");
    ofile << _graphDef->SerializeAsString();
    auto func = std::bind([&]() {
        for (size_t i = 0; i < queryCount; i++) {
            // std::cout << "begin round " << i << std::endl;
            if (abort) {
                break;
            }
            auto thisShow = show;
            thisShow = true;
            auto current = atomic_inc_return(&counter);
            if (current == 2000) {
                // thisShow = true;
                std::cout << "show true" << std::endl;
                // params.setTraceLevel("");
            }
            // if (i % 200 == 0) {
            //     std::cout << "thread: " << pthread_self() << ", round: " << i << std::endl;
            // }
            // std::string fileContent;
            // EXPECT_EQ(fslib::EC_OK,
            //           fslib::fs::FileSystem::readFile(
            //               "/ha3_develop/source_code/navi/graph_31445_3917",
            //               fileContent));
            auto def = new GraphDef();
            def->CopyFrom(*_graphDef);
            // bool load = false;
            // if (load) {
            //     ASSERT_TRUE(def->ParseFromString(fileContent));
            // } else {
            //     EXPECT_TRUE(buildGraph2("o1", def, bizList));
            //     EXPECT_TRUE(buildGraph1("o2", def, bizList));
            //     EXPECT_TRUE(buildGraph2("o3", def, bizList));
            //     EXPECT_TRUE(buildGraph1("o4", def, bizList));
            //     EXPECT_TRUE(buildGraph2("o4", def, bizList));
            //     EXPECT_TRUE(buildGraph3("o6", def, bizList));
            //     EXPECT_TRUE(buildGraph3("o7", def, bizList));
            // }

            // if (current % 100 == 0) {
            //     std::cout << def->DebugString() << std::endl;
            // }
            int64_t expectCount = 20;
            int64_t dataCount = 0;
            auto graphStr = def->SerializeAsString();
            // auto host = randItem(hostList);
            // std::cout << host << std::endl;
            auto navi = cluster.getNavi("host_0");
            auto userResult = navi->runGraph(def, params);

            // std::string graphFileName(
            //     "./graph_" + autil::StringUtil::toString(current) + "_" +
            //     autil::StringUtil::toString(
            //         userResult->getNaviResult()->id.queryId));
            // std::ofstream graphFile(graphFileName);
            // graphFile << graphStr;

            std::vector<NaviUserData> dataVec;
            std::vector<NaviResultPtr> resultVec;
            auto naviResult = showData(userResult, thisShow, expectCount,
                                       dataCount, dataVec, resultVec);
            EXPECT_EQ(9u, dataVec.size());
            std::cout << "data count: " << dataVec.size() << std::endl;
            dataVec.clear();
            resultVec.clear();
            showResultAndSaveVis(userResult);
            EXPECT_EQ(EC_NONE, naviResult->getErrorCode());
        }
        std::cout << "thread exited" << std::endl;
    });
    std::vector<autil::ThreadPtr> threads;
    for (size_t i = 0; i < threadCount; i++) {
        threads.push_back(autil::Thread::createThread(
                        func, "test_thread_" + autil::StringUtil::toString(i)));
    }
    threads.clear();
    // for (const auto &data : dataVec) {
    //     std::cout << "session: "
    //               << CommonUtil::formatSessionId(data.id)
    //               << std::endl;
    //     if (data.data) {
    //         std::cout << "node: " << data.node << ", port: " << data.port
    //                   << ": ";
    //         data.data->show();
    //     } else {
    //         std::cout << "null data" << std::endl;
    //     }
    // }
    // for (const auto &result : resultVec) {
    //     result->show();
    // }
    std::cout << "begin destruct" << std::endl;
    // usleep(100 * 1000 * 1000);
}

bool buildGraphForkWithTerminator(const std::string &outputName,
                                  GraphDef *graphDef,
                                  const std::vector<std::string> &bizList)
{
    GraphBuilder builder(graphDef);
    builder.newSubGraph(randItem(bizList));
    auto sourceFork = builder.node("source_node_fork").kernel("SourceKernel").jsonAttrs(R"json({"times" : 20})json");
    auto currentScope = builder.getCurrentScope();
    builder.addScope();
    auto fork = builder.node("fork").kernel("SubGraphKernel");
    {
        auto scopeTerminator = builder.getScopeTerminator();
        scopeTerminator.kernel("TestScopeTerminator").out("out").asGraphOutput("scope");
    }
    builder.scope(currentScope);
    auto clientWorldFork = builder.node("client_world_fork").kernel("WorldKernel");

    fork.in("input1").from(sourceFork.out("output1"));
    clientWorldFork.in("input1").from(fork.out("output1"));
    clientWorldFork.out("output1").asGraphOutput(outputName);

    {
        auto scopeTerminator = builder.getScopeTerminator();
        scopeTerminator.kernel("TestScopeTerminator").out("out").asGraphOutput("scope2");
    }
    return builder.ok();
}

TEST_F(NaviExampleTest, testForkWithTerminator) {
    ResourceMapPtr rootResourceMap(new ResourceMap());

    std::string configPath = NAVI_TEST_DATA_PATH + "config/cluster/";
    std::string biz1Config = NAVI_TEST_DATA_PATH + "config/cluster/biz1.py";
    NaviTestCluster cluster;
    ASSERT_TRUE(
            cluster.addServer("host_0", _loader, configPath, rootResourceMap));
    ASSERT_TRUE(
            cluster.addServer("host_1", _loader, configPath, rootResourceMap));
    ASSERT_TRUE(
            cluster.addServer("host_2", _loader, configPath, rootResourceMap));
    ASSERT_TRUE(
            cluster.addServer("host_3", _loader, configPath, rootResourceMap));

    std::vector<std::string> hostList = { "host_0", "host_1", "host_2",
                                          "host_3", "host_4", "host_5",
                                          "host_6", "host_7" };
    ASSERT_TRUE(cluster.addBiz("host_0", "client_biz", 1, 0, biz1Config));
    ASSERT_TRUE(cluster.addBiz("host_1", "server_biz_1", 3, 0, biz1Config));
    ASSERT_TRUE(cluster.addBiz("host_2", "server_biz_1", 3, 1, biz1Config));
    ASSERT_TRUE(cluster.addBiz("host_3", "server_biz_1", 3, 2, biz1Config));
    ASSERT_TRUE(cluster.start());

    rootResourceMap.reset();

    auto bizList = cluster.getBizList();
    EXPECT_TRUE(buildGraphForkWithTerminator("o1", _graphDef, { "client_biz" }));

    RunGraphParams params;
    params.setTimeoutMs(1000000000);
    params.setTraceLevel("ERROR");
    params.setCollectMetric(true);

    enum RunMode {
        RM_SINGLE = 1,
        RM_PERF = 2,
    };
    RunMode mode= RM_SINGLE;

    size_t queryCount = 1;
    size_t threadCount = 1;
    bool show = false;
    if (RM_SINGLE == mode) {
        show = true;
    } else if (RM_PERF == mode) {
        string perfParamEnvStr = autil::EnvUtil::getEnv("perf_param");
        if (!perfParamEnvStr.empty()) {
            auto vec = autil::StringUtil::split(perfParamEnvStr, ";");
            ASSERT_EQ(2u, vec.size());
            queryCount = autil::StringUtil::fromString<int32_t>(vec[0]);
            threadCount = autil::StringUtil::fromString<int32_t>(vec[1]);
        } else {
            queryCount = 1000000;
            threadCount = 1;
        }
    }
    bool abort = false;
    atomic64_t counter;
    atomic_set(&counter, 0);

    // std::cout << _graphDef->DebugString() << std::endl;
    std::ofstream ofile("./graph.serialize");
    ofile << _graphDef->SerializeAsString();
    auto func = std::bind([&]() {
        for (size_t i = 0; i < queryCount; i++) {
            // std::cout << "begin round " << i << std::endl;
            if (abort) {
                break;
            }
            auto thisShow = show;
            thisShow = true;
            auto current = atomic_inc_return(&counter);
            if (current == 2000) {
                // thisShow = true;
                std::cout << "show true" << std::endl;
                // params.setTraceLevel("");
            }
            auto def = new GraphDef();
            def->CopyFrom(*_graphDef);
            int64_t expectCount = 20;
            int64_t dataCount = 0;
            auto graphStr = def->SerializeAsString();
            // auto host = randItem(hostList);
            // std::cout << host << std::endl;
            auto navi = cluster.getNavi("host_0");
            auto userResult = navi->runGraph(def, params);

            std::vector<NaviUserData> dataVec;
            std::vector<NaviResultPtr> resultVec;
            auto naviResult = showData(userResult, thisShow, expectCount,
                                       dataCount, dataVec, resultVec);
            EXPECT_EQ(38u, dataVec.size());
            std::cout << "data count: " << dataVec.size() << std::endl;
            dataVec.clear();
            resultVec.clear();
        }
        std::cout << "thread exited" << std::endl;
    });
    std::vector<autil::ThreadPtr> threads;
    for (size_t i = 0; i < threadCount; i++) {
        threads.push_back(autil::Thread::createThread(
                        func, "test_thread_" + autil::StringUtil::toString(i)));
    }
    threads.clear();
    std::cout << "begin destruct" << std::endl;
}

bool buildGraphForkDownStreamAbort(const std::string &outputName,
                                   GraphDef *graphDef,
                                   const std::vector<std::string> &bizList) {
    GraphBuilder builder(graphDef);
    builder.newSubGraph(randItem(bizList));
    auto sourceFork = builder.node("source_node_fork").kernel("SourceKernel").jsonAttrs(R"json({"times" : 20})json");
    auto currentScope = builder.getCurrentScope();
    builder.addScope();
    auto fork = builder.node("fork").kernel("SubGraphKernel");
    {
        auto scopeTerminator = builder.getScopeTerminator();
        scopeTerminator.kernel("TestScopeTerminator").out("out").asGraphOutput("scope");
    }
    builder.addScope();
    auto clientWorldFork = builder.node("client_world_fork").kernel("AbortKernel");

    fork.in("input1").from(sourceFork.out("output1"));
    clientWorldFork.in("input1").from(fork.out("output1"));
    clientWorldFork.out("output1").asGraphOutput(outputName);

    {
        auto scopeTerminator = builder.getScopeTerminator();
        scopeTerminator.kernel("TestScopeTerminator").out("out").asGraphOutput("scope2");
    }
    builder.scope(currentScope);
    auto identity = builder.node("identity").kernel("IdentityTestKernel");
    identity.in("input1").from(fork.out("output1"));
    identity.out("output1").asGraphOutput("identity");
    return builder.ok();
}

TEST_F(NaviExampleTest, testForkDownStreamAbort) {
    ResourceMapPtr rootResourceMap(new ResourceMap());

    std::string configPath = NAVI_TEST_DATA_PATH + "config/cluster/";
    std::string biz1Config = NAVI_TEST_DATA_PATH + "config/cluster/biz1.py";
    NaviTestCluster cluster;
    ASSERT_TRUE(
            cluster.addServer("host_0", _loader, configPath, rootResourceMap));
    ASSERT_TRUE(
            cluster.addServer("host_1", _loader, configPath, rootResourceMap));
    ASSERT_TRUE(
            cluster.addServer("host_2", _loader, configPath, rootResourceMap));
    ASSERT_TRUE(
            cluster.addServer("host_3", _loader, configPath, rootResourceMap));

    std::vector<std::string> hostList = { "host_0", "host_1", "host_2",
                                          "host_3", "host_4", "host_5",
                                          "host_6", "host_7" };
    ASSERT_TRUE(cluster.addBiz("host_0", "client_biz", 1, 0, biz1Config));
    ASSERT_TRUE(cluster.addBiz("host_1", "server_biz_1", 3, 0, biz1Config));
    ASSERT_TRUE(cluster.addBiz("host_2", "server_biz_1", 3, 1, biz1Config));
    ASSERT_TRUE(cluster.addBiz("host_3", "server_biz_1", 3, 2, biz1Config));
    ASSERT_TRUE(cluster.start());

    rootResourceMap.reset();

    auto bizList = cluster.getBizList();
    EXPECT_TRUE(buildGraphForkDownStreamAbort("o1", _graphDef, { "client_biz" }));

    RunGraphParams params;
    params.setTimeoutMs(1000000000);
    params.setTraceLevel("ERROR");
    params.setCollectMetric(true);

    enum RunMode {
        RM_SINGLE = 1,
        RM_PERF = 2,
    };
    RunMode mode= RM_SINGLE;

    size_t queryCount = 1;
    size_t threadCount = 1;
    bool show = false;
    if (RM_SINGLE == mode) {
        show = true;
    } else if (RM_PERF == mode) {
        string perfParamEnvStr = autil::EnvUtil::getEnv("perf_param");
        if (!perfParamEnvStr.empty()) {
            auto vec = autil::StringUtil::split(perfParamEnvStr, ";");
            ASSERT_EQ(2u, vec.size());
            queryCount = autil::StringUtil::fromString<int32_t>(vec[0]);
            threadCount = autil::StringUtil::fromString<int32_t>(vec[1]);
        } else {
            queryCount = 1000000;
            threadCount = 1;
        }
    }
    bool abort = false;
    atomic64_t counter;
    atomic_set(&counter, 0);

    // std::cout << _graphDef->DebugString() << std::endl;
    std::ofstream ofile("./graph.serialize");
    ofile << _graphDef->SerializeAsString();
    auto func = std::bind([&]() {
        for (size_t i = 0; i < queryCount; i++) {
            // std::cout << "begin round " << i << std::endl;
            if (abort) {
                break;
            }
            auto thisShow = show;
            thisShow = true;
            auto current = atomic_inc_return(&counter);
            if (current == 2000) {
                // thisShow = true;
                std::cout << "show true" << std::endl;
                // params.setTraceLevel("");
            }
            auto def = new GraphDef();
            def->CopyFrom(*_graphDef);
            int64_t expectCount = 20;
            int64_t dataCount = 0;
            auto graphStr = def->SerializeAsString();
            // auto host = randItem(hostList);
            // std::cout << host << std::endl;
            auto navi = cluster.getNavi("host_0");
            auto userResult = navi->runGraph(def, params);

            std::vector<NaviUserData> dataVec;
            std::vector<NaviResultPtr> resultVec;
            auto naviResult = showData(userResult, thisShow, expectCount,
                                       dataCount, dataVec, resultVec);
            EXPECT_EQ(39u, dataVec.size());
            EXPECT_EQ(EC_NONE, naviResult->getErrorCode());
            // EXPECT_EQ("abort", naviResult->errorEvent.message);
            std::cout << "data count: " << dataVec.size() << std::endl;
            dataVec.clear();
            resultVec.clear();
        }
        std::cout << "thread exited" << std::endl;
    });
    std::vector<autil::ThreadPtr> threads;
    for (size_t i = 0; i < threadCount; i++) {
        threads.push_back(autil::Thread::createThread(
                        func, "test_thread_" + autil::StringUtil::toString(i)));
    }
    threads.clear();
    std::cout << "begin destruct" << std::endl;
}

bool buildGraphForkWithStop(const std::string &outputName,
                            GraphDef *graphDef,
                            const std::vector<std::string> &bizList) {
    GraphBuilder builder(graphDef);
    builder.newSubGraph(randItem(bizList));
    auto sourceFork = builder.node("source_node_fork").kernel("SourceKernel").jsonAttrs(R"json({"times" : 20})json");
    auto currentScope = builder.getCurrentScope();
    builder.addScope();
    auto fork = builder.node("fork").kernel("SubGraphKernel");
    {
        auto scopeTerminator = builder.getScopeTerminator();
        scopeTerminator.kernel("TestScopeTerminator").out("out").asGraphOutput("scope");
    }
    builder.scope(currentScope);
    auto clientWorldFork = builder.node("client_world_fork").kernel("WorldKernel");
    auto stop1 = builder.node("stop_node1").kernel("StopKernel");
    auto stop2 = builder.node("stop_node2").kernel("StopKernel");

    fork.in("input1").from(sourceFork.out("output1"));
    stop1.in("input1").from(fork.out("output1"));
    stop2.in("input1").from(sourceFork.out("output1"));
    clientWorldFork.in("input1").from(fork.out("output1"));
    clientWorldFork.out("output1").asGraphOutput(outputName);

    {
        auto scopeTerminator = builder.getScopeTerminator();
        scopeTerminator.kernel("TestScopeTerminator").out("out").asGraphOutput("scope2");
    }
    return builder.ok();
}

TEST_F(NaviExampleTest, testForkWithStop) {
    ResourceMapPtr rootResourceMap(new ResourceMap());

    std::string configPath = NAVI_TEST_DATA_PATH + "config/cluster/";
    std::string biz1Config = NAVI_TEST_DATA_PATH + "config/cluster/biz1.py";
    NaviTestCluster cluster;
    ASSERT_TRUE(
            cluster.addServer("host_0", _loader, configPath, rootResourceMap));
    ASSERT_TRUE(
            cluster.addServer("host_1", _loader, configPath, rootResourceMap));
    ASSERT_TRUE(
            cluster.addServer("host_2", _loader, configPath, rootResourceMap));
    ASSERT_TRUE(
            cluster.addServer("host_3", _loader, configPath, rootResourceMap));

    std::vector<std::string> hostList = { "host_0", "host_1", "host_2",
                                          "host_3", "host_4", "host_5",
                                          "host_6", "host_7" };
    ASSERT_TRUE(cluster.addBiz("host_0", "client_biz", 1, 0, biz1Config));
    ASSERT_TRUE(cluster.addBiz("host_1", "server_biz_1", 3, 0, biz1Config));
    ASSERT_TRUE(cluster.addBiz("host_2", "server_biz_1", 3, 1, biz1Config));
    ASSERT_TRUE(cluster.addBiz("host_3", "server_biz_1", 3, 2, biz1Config));
    ASSERT_TRUE(cluster.start());

    rootResourceMap.reset();

    auto bizList = cluster.getBizList();
    EXPECT_TRUE(buildGraphForkWithStop("o1", _graphDef, { "client_biz" }));

    RunGraphParams params;
    params.setTimeoutMs(1000000000);
    params.setTraceLevel("ERROR");
    params.setCollectMetric(true);

    enum RunMode {
        RM_SINGLE = 1,
        RM_PERF = 2,
    };
    RunMode mode= RM_SINGLE;

    size_t queryCount = 1;
    size_t threadCount = 1;
    bool show = false;
    if (RM_SINGLE == mode) {
        show = true;
    } else if (RM_PERF == mode) {
        string perfParamEnvStr = autil::EnvUtil::getEnv("perf_param");
        if (!perfParamEnvStr.empty()) {
            auto vec = autil::StringUtil::split(perfParamEnvStr, ";");
            ASSERT_EQ(2u, vec.size());
            queryCount = autil::StringUtil::fromString<int32_t>(vec[0]);
            threadCount = autil::StringUtil::fromString<int32_t>(vec[1]);
        } else {
            queryCount = 1000000;
            threadCount = 1;
        }
    }
    bool abort = false;
    atomic64_t counter;
    atomic_set(&counter, 0);
    NaviLoggerProvider logProvider("schedule1");

    // std::cout << _graphDef->DebugString() << std::endl;
    std::ofstream ofile("./graph.serialize");
    ofile << _graphDef->SerializeAsString();
    auto func = std::bind([&]() {
        for (size_t i = 0; i < queryCount; i++) {
            // std::cout << "begin round " << i << std::endl;
            if (abort) {
                break;
            }
            auto thisShow = show;
            thisShow = true;
            auto current = atomic_inc_return(&counter);
            if (current == 2000) {
                // thisShow = true;
                std::cout << "show true" << std::endl;
                // params.setTraceLevel("");
            }
            auto def = new GraphDef();
            def->CopyFrom(*_graphDef);
            int64_t expectCount = 20;
            int64_t dataCount = 0;
            auto graphStr = def->SerializeAsString();
            // auto host = randItem(hostList);
            // std::cout << host << std::endl;
            auto navi = cluster.getNavi("host_0");
            auto userResult = navi->runGraph(def, params);

            std::vector<NaviUserData> dataVec;
            std::vector<NaviResultPtr> resultVec;
            auto naviResult = showData(userResult, thisShow, expectCount,
                                       dataCount, dataVec, resultVec);
            EXPECT_EQ(38u, dataVec.size());
            std::cout << "data count: " << dataVec.size() << std::endl;
            dataVec.clear();
            resultVec.clear();
        }
        std::cout << "thread exited" << std::endl;
    });
    std::vector<autil::ThreadPtr> threads;
    for (size_t i = 0; i < threadCount; i++) {
        threads.push_back(autil::Thread::createThread(
                        func, "test_thread_" + autil::StringUtil::toString(i)));
    }
    threads.clear();
    std::cout << "begin destruct" << std::endl;
}

bool buildScopeTerminatorWithInput(const std::string &outputName,
                                  GraphDef *graphDef,
                                  const std::vector<std::string> &bizList)
{
    GraphBuilder builder(graphDef);
    builder.newSubGraph(randItem(bizList));
    auto sourceFork = builder.node("source_node_fork").kernel("SourceKernel").jsonAttrs(R"json({"times" : 20})json");
    auto abort = builder.node("abort").kernel("WorldKernel");
    abort.in("input1").from(sourceFork.out("output1"));
    auto scopeTerminator = builder.getScopeTerminator();
    scopeTerminator.kernel("TestScopeTerminator").out("out").asGraphOutput("scope");
    scopeTerminator.in(SCOPE_TERMINATOR_INPUT_PORT).autoNext().from(abort.out("output1"));
    return builder.ok();
}

TEST_F(NaviExampleTest, testScopeTerminatorWithInput) {
    ResourceMapPtr rootResourceMap(new ResourceMap());

    std::string configPath = NAVI_TEST_DATA_PATH + "config/cluster/";
    std::string biz1Config = NAVI_TEST_DATA_PATH + "config/cluster/biz1.py";
    NaviTestCluster cluster;
    ASSERT_TRUE(
            cluster.addServer("host_0", _loader, configPath, rootResourceMap));
    ASSERT_TRUE(
            cluster.addServer("host_1", _loader, configPath, rootResourceMap));
    ASSERT_TRUE(
            cluster.addServer("host_2", _loader, configPath, rootResourceMap));
    ASSERT_TRUE(
            cluster.addServer("host_3", _loader, configPath, rootResourceMap));

    std::vector<std::string> hostList = { "host_0", "host_1", "host_2",
                                          "host_3", "host_4", "host_5",
                                          "host_6", "host_7" };
    ASSERT_TRUE(cluster.addBiz("host_0", "client_biz", 1, 0, biz1Config));
    ASSERT_TRUE(cluster.addBiz("host_1", "server_biz_1", 3, 0, biz1Config));
    ASSERT_TRUE(cluster.addBiz("host_2", "server_biz_1", 3, 1, biz1Config));
    ASSERT_TRUE(cluster.addBiz("host_3", "server_biz_1", 3, 2, biz1Config));
    ASSERT_TRUE(cluster.start());

    rootResourceMap.reset();

    auto bizList = cluster.getBizList();
    EXPECT_TRUE(buildScopeTerminatorWithInput("o1", _graphDef, { "client_biz" }));

    RunGraphParams params;
    params.setTimeoutMs(1000000000);
    params.setTraceLevel("ERROR");
    params.setCollectMetric(true);

    enum RunMode {
        RM_SINGLE = 1,
        RM_PERF = 2,
    };
    RunMode mode= RM_SINGLE;

    size_t queryCount = 1;
    size_t threadCount = 1;
    bool show = false;
    if (RM_SINGLE == mode) {
        show = true;
    } else if (RM_PERF == mode) {
        string perfParamEnvStr = autil::EnvUtil::getEnv("perf_param");
        if (!perfParamEnvStr.empty()) {
            auto vec = autil::StringUtil::split(perfParamEnvStr, ";");
            ASSERT_EQ(2u, vec.size());
            queryCount = autil::StringUtil::fromString<int32_t>(vec[0]);
            threadCount = autil::StringUtil::fromString<int32_t>(vec[1]);
        } else {
            queryCount = 1000000;
            threadCount = 1;
        }
    }
    bool abort = false;
    atomic64_t counter;
    atomic_set(&counter, 0);
    // std::cout << _graphDef->DebugString() << std::endl;
    // std::ofstream ofile("./graph.serialize");
    std::ofstream ofile(std::string("/home/zq103303/alibaba/navi/http_server/searcher_graph.def"));
    ofile << _graphDef->SerializeAsString();
    auto func = std::bind([&]() {
        for (size_t i = 0; i < queryCount; i++) {
            // std::cout << "begin round " << i << std::endl;
            if (abort) {
                break;
            }
            auto thisShow = show;
            thisShow = true;
            auto current = atomic_inc_return(&counter);
            if (current == 2000) {
                // thisShow = true;
                std::cout << "show true" << std::endl;
                // params.setTraceLevel("");
            }
            auto def = new GraphDef();
            def->CopyFrom(*_graphDef);
            int64_t expectCount = 20;
            int64_t dataCount = 0;
            auto graphStr = def->SerializeAsString();
            // auto host = randItem(hostList);
            // std::cout << host << std::endl;
            auto navi = cluster.getNavi("host_0");
            auto userResult = navi->runGraph(def, params);

            std::vector<NaviUserData> dataVec;
            std::vector<NaviResultPtr> resultVec;
            auto naviResult = showData(userResult, thisShow, expectCount,
                                       dataCount, dataVec, resultVec);
            EXPECT_EQ(9u, dataVec.size());
            std::cout << "data count: " << dataVec.size() << std::endl;
            dataVec.clear();
            resultVec.clear();
        }
        std::cout << "thread exited" << std::endl;
    });
    std::vector<autil::ThreadPtr> threads;
    for (size_t i = 0; i < threadCount; i++) {
        threads.push_back(autil::Thread::createThread(
                        func, "test_thread_" + autil::StringUtil::toString(i)));
    }
    threads.clear();
    std::cout << "begin destruct" << std::endl;
}

bool buildGraphForkWithError(const std::string &outputName,
                             GraphDef *graphDef,
                             const std::vector<std::string> &bizList)
{
    GraphBuilder builder(graphDef);
    builder.newSubGraph(randItem(bizList));
    auto sourceFork = builder.node("source_node_fork").kernel("SourceKernel").jsonAttrs(R"json({"times" : 2000000})json");
    auto currentScope = builder.getCurrentScope();
    builder.addScope();
    auto fork = builder.node("fork").kernel("SubGraphKernel").integerAttr("abort", 1);
    {
        auto scopeTerminator = builder.getScopeTerminator();
        scopeTerminator.kernel("TestScopeTerminator").out("out").asGraphOutput("scope");
    }
    builder.scope(currentScope);
    auto clientWorldFork = builder.node("client_world_fork").kernel("WorldKernel");

    fork.in("input1").from(sourceFork.out("output1"));
    fork.out("output2").asGraphOutput("fork_output2");
    clientWorldFork.in("input1").from(fork.out("output1"));
    clientWorldFork.out("output1").asGraphOutput(outputName);
    {
        auto scopeTerminator = builder.getScopeTerminator();
        scopeTerminator.kernel("TestScopeTerminator").out("out").asGraphOutput("scope2");
    }
    return builder.ok();
}

TEST_F(NaviExampleTest, testForkGraphError) {
    ResourceMapPtr rootResourceMap(new ResourceMap());

    std::string configPath = NAVI_TEST_DATA_PATH + "config/cluster/";
    std::string biz1Config = NAVI_TEST_DATA_PATH + "config/cluster/biz1.py";
    NaviTestCluster cluster;
    ASSERT_TRUE(
            cluster.addServer("host_0", _loader, configPath, rootResourceMap));
    ASSERT_TRUE(
            cluster.addServer("host_1", _loader, configPath, rootResourceMap));
    ASSERT_TRUE(
            cluster.addServer("host_2", _loader, configPath, rootResourceMap));
    ASSERT_TRUE(
            cluster.addServer("host_3", _loader, configPath, rootResourceMap));

    std::vector<std::string> hostList = { "host_0", "host_1", "host_2",
                                          "host_3", "host_4", "host_5",
                                          "host_6", "host_7" };
    ASSERT_TRUE(cluster.addBiz("host_0", "client_biz", 1, 0, biz1Config));
    ASSERT_TRUE(cluster.addBiz("host_1", "server_biz_1", 3, 0, biz1Config));
    ASSERT_TRUE(cluster.addBiz("host_2", "server_biz_1", 3, 1, biz1Config));
    ASSERT_TRUE(cluster.addBiz("host_3", "server_biz_1", 3, 2, biz1Config));
    ASSERT_TRUE(cluster.start());

    rootResourceMap.reset();

    auto bizList = cluster.getBizList();
    EXPECT_TRUE(buildGraphForkWithError("o1", _graphDef, { "client_biz" }));

    RunGraphParams params;
    params.setTimeoutMs(1000000000);
    params.setTraceLevel("ERROR");
    params.setCollectMetric(true);

    enum RunMode {
        RM_SINGLE = 1,
        RM_PERF = 2,
    };
    RunMode mode= RM_SINGLE;

    size_t queryCount = 1;
    size_t threadCount = 1;
    bool show = false;
    if (RM_SINGLE == mode) {
        show = true;
    } else if (RM_PERF == mode) {
        string perfParamEnvStr = autil::EnvUtil::getEnv("perf_param");
        if (!perfParamEnvStr.empty()) {
            auto vec = autil::StringUtil::split(perfParamEnvStr, ";");
            ASSERT_EQ(2u, vec.size());
            queryCount = autil::StringUtil::fromString<int32_t>(vec[0]);
            threadCount = autil::StringUtil::fromString<int32_t>(vec[1]);
        } else {
            queryCount = 1000000;
            threadCount = 1;
        }
    }
    bool abort = false;
    atomic64_t counter;
    atomic_set(&counter, 0);
    // std::cout << _graphDef->DebugString() << std::endl;
    std::ofstream ofile("./graph.serialize");
    ofile << _graphDef->SerializeAsString();
    auto func = std::bind([&]() {
        for (size_t i = 0; i < queryCount; i++) {
            // std::cout << "begin round " << i << std::endl;
            if (abort) {
                break;
            }
            auto thisShow = show;
            thisShow = true;
            auto current = atomic_inc_return(&counter);
            if (current == 2000) {
                // thisShow = true;
                std::cout << "show true" << std::endl;
                // params.setTraceLevel("");
            }
            auto def = new GraphDef();
            def->CopyFrom(*_graphDef);
            int64_t expectCount = 20;
            int64_t dataCount = 0;
            auto graphStr = def->SerializeAsString();
            // auto host = randItem(hostList);
            // std::cout << host << std::endl;
            auto navi = cluster.getNavi("host_0");
            auto userResult = navi->runGraph(def, params);

            std::vector<NaviUserData> dataVec;
            std::vector<NaviResultPtr> resultVec;
            auto naviResult = showData(userResult, thisShow, expectCount,
                                       dataCount, dataVec, resultVec);
            EXPECT_EQ(28u, dataVec.size());
            EXPECT_EQ(EC_NONE, naviResult->getErrorCode());
            std::cout << "data count: " << dataVec.size() << std::endl;
            dataVec.clear();
            resultVec.clear();
        }
        std::cout << "thread exited" << std::endl;
    });
    std::vector<autil::ThreadPtr> threads;
    for (size_t i = 0; i < threadCount; i++) {
        threads.push_back(autil::Thread::createThread(
                        func, "test_thread_" + autil::StringUtil::toString(i)));
    }
    threads.clear();
    std::cout << "begin destruct" << std::endl;
}

bool buildGraphForkRecur(const std::string &outputName, GraphDef *graphDef,
                         const std::vector<std::string> &bizList)
{
    GraphBuilder builder(graphDef);
    builder.newSubGraph(randItem(bizList));
    auto sourceFork = builder.node("source_node_fork").kernel("SourceKernel").jsonAttrs(R"json({"times" : 20})json");
    auto currentScope = builder.getCurrentScope();
    builder.scopeErrorHandleStrategy(currentScope, EHS_ERROR_AS_FATAL);
    builder.addScope();
    auto fork = builder.node("fork").kernel("SubGraphRecurKernel");
    builder.scope(currentScope);
    auto clientWorldFork = builder.node("client_world_fork").kernel("WorldKernel2");

    fork.in("input1").from(sourceFork.out("output1"));
    clientWorldFork.in("input1").from(fork.out("output2"));
    clientWorldFork.out("output1").asGraphOutput(outputName);
    fork.out("output1").asGraphOutput(outputName + "_2");

    return builder.ok();
}

TEST_F(NaviExampleTest, testForkRecur) {
    ResourceMapPtr rootResourceMap(new ResourceMap());

    std::string configPath = NAVI_TEST_DATA_PATH + "config/cluster/";
    std::string biz1Config = NAVI_TEST_DATA_PATH + "config/cluster/biz1.py";
    NaviTestCluster cluster;
    ASSERT_TRUE(
            cluster.addServer("host_0", _loader, configPath, rootResourceMap));
    ASSERT_TRUE(
            cluster.addServer("host_1", _loader, configPath, rootResourceMap));
    ASSERT_TRUE(
            cluster.addServer("host_2", _loader, configPath, rootResourceMap));
    ASSERT_TRUE(
            cluster.addServer("host_3", _loader, configPath, rootResourceMap));
    ASSERT_TRUE(
            cluster.addServer("host_4", _loader, configPath, rootResourceMap));

    std::vector<std::string> hostList = { "host_0", "host_1", "host_2",
                                          "host_3", "host_4", "host_5",
                                          "host_6", "host_7" };
    ASSERT_TRUE(cluster.addBiz("host_0", "client_biz", 1, 0, biz1Config));
    ASSERT_TRUE(cluster.addBiz("host_1", "server_biz_1", 2, 0, biz1Config));
    ASSERT_TRUE(cluster.addBiz("host_2", "server_biz_1", 2, 1, biz1Config));
    ASSERT_TRUE(cluster.start());

    rootResourceMap.reset();

    auto bizList = cluster.getBizList();
    EXPECT_TRUE(buildGraphForkRecur("o1", _graphDef, { "client_biz" }));
    EXPECT_TRUE(buildGraphForkRecur("o2", _graphDef, { "client_biz" }));
    EXPECT_TRUE(buildGraphForkRecur("o3", _graphDef, { "client_biz" }));
    EXPECT_TRUE(buildGraphForkRecur("o4", _graphDef, { "server_biz_1" }));
    EXPECT_TRUE(buildGraphForkRecur("o5", _graphDef, { "server_biz_1" }));
    EXPECT_TRUE(buildGraphForkRecur("o6", _graphDef, { "server_biz_1" }));

    RunGraphParams params;
    params.setTimeoutMs(1000000000);
    params.setTraceLevel("debug");
    params.setCollectMetric(true);
    params.setCollectPerf(true);

    usleep(3 * 1000 * 1000);
    enum RunMode {
        RM_SINGLE = 1,
        RM_PERF = 2,
    };
    RunMode mode= RM_SINGLE;

    size_t queryCount = 1;
    size_t threadCount = 1;
    bool show = false;
    if (RM_SINGLE == mode) {
        show = true;
    } else if (RM_PERF == mode) {
        string perfParamEnvStr = autil::EnvUtil::getEnv("perf_param");
        if (!perfParamEnvStr.empty()) {
            auto vec = autil::StringUtil::split(perfParamEnvStr, ";");
            ASSERT_EQ(2u, vec.size());
            queryCount = autil::StringUtil::fromString<int32_t>(vec[0]);
            threadCount = autil::StringUtil::fromString<int32_t>(vec[1]);
        } else {
            queryCount = 1000000;
            threadCount = 1;
        }
    }
    bool abort = false;
    atomic64_t counter;
    atomic_set(&counter, 0);
    auto func = std::bind([&]() {
        for (size_t i = 0; i < queryCount; i++) {
            if (abort) {
                break;
            }
            auto thisShow = show;
            thisShow = true;
            auto def = new GraphDef();
            def->CopyFrom(*_graphDef);
            int64_t expectCount = 20;
            int64_t dataCount = 0;
            auto graphStr = def->SerializeAsString();
            // auto host = randItem(hostList);
            // std::cout << host << std::endl;
            auto navi = cluster.getNavi("host_1");
            auto userResult = navi->runGraph(def, params);

            std::vector<NaviUserData> dataVec;
            std::vector<NaviResultPtr> resultVec;
            auto naviResult = showData(userResult, thisShow, expectCount,
                                       dataCount, dataVec, resultVec);
            EXPECT_EQ(378u, dataVec.size());
            std::cout << "data count: " << dataVec.size() << std::endl;
            dataVec.clear();
            resultVec.clear();
            showResultAndSaveVis(userResult);
            EXPECT_EQ(EC_NONE, naviResult->getErrorCode());
        }
        std::cout << "thread exited" << std::endl;
    });
    std::vector<autil::ThreadPtr> threads;
    for (size_t i = 0; i < threadCount; i++) {
        threads.push_back(autil::Thread::createThread(
                        func, "test_thread_" + autil::StringUtil::toString(i)));
    }
    threads.clear();
    std::cout << "begin destruct" << std::endl;
}

}
