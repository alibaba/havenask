#include "build_service/admin/controlflow/ControlDefine.h"
#include "build_service/admin/controlflow/FlowContainer.h"
#include "build_service/admin/controlflow/LocalLuaScriptReader.h"
#include "build_service/admin/controlflow/TaskBase.h"
#include "build_service/admin/controlflow/TaskFactory.h"
#include "build_service/admin/controlflow/TaskFlow.h"
#include "build_service/admin/controlflow/TaskFlowManager.h"
#include "build_service/common/PathDefine.h"
#include "build_service/config/ConfigReaderAccessor.h"
#include "build_service/config/ResourceReader.h"
#include "build_service/test/unittest.h"
#include "fslib/util/FileUtil.h"

using namespace std;
using namespace autil;
using namespace testing;
using namespace build_service::config;
using namespace build_service::util;

namespace build_service { namespace admin {

class BatchBuildGraphTest : public BUILD_SERVICE_TESTBASE
{
public:
    void setUp();
    void tearDown();

private:
    void stopFlowByTag(const string& tag, TaskFlowManager& mgr)
    {
        vector<TaskFlowPtr> flows;
        mgr.getFlowByTag(tag, flows);
        for (auto flow : flows) {
            flow->_status = TaskFlow::TF_STATUS::tf_finish;
        }
    }
};

void BatchBuildGraphTest::setUp()
{
    string filePath = GET_TEST_DATA_PATH() + "/build_service/admin/controlflow/lua.conf";
    setenv("control_flow_config_path", filePath.c_str(), true);
}

void BatchBuildGraphTest::tearDown() { unsetenv("control_flow_config_path"); }

TEST_F(BatchBuildGraphTest, TestSingleAddBatchGraph)
{
    KeyValueMap kvMap;
    kvMap["batchId"] = "2";
    kvMap["clusterName"] = "cluster1";

    TaskFactoryPtr factory(new TaskFactory);
    string filePath = TEST_ROOT_PATH() + "/build_service/admin/controlflow/lua";
    TaskFlowManager mgr(filePath, factory, TaskResourceManagerPtr());
    TaskFlowPtr processorFlow = mgr.loadSimpleFlow("processor", "processor", kvMap, true);
    kvMap["processorFlowId"] = processorFlow->getFlowId();

    ASSERT_TRUE(mgr.init("BatchBuild/SingleClusterAddBatch.graph", kvMap));
    mgr.stepRun();
    vector<TaskFlowPtr> flows;
    mgr.getFlowByTag("cluster1-BatchBuilder-2", flows);
    ASSERT_EQ(1, flows.size());
    ASSERT_EQ(0, flows[0]->_upstreamFlowInfos.size());
    flows.clear();

    mgr.getFlowByTag("cluster1-BatchMerger-2", flows);
    ASSERT_EQ(1, flows.size());
    ASSERT_EQ(1, flows[0]->_upstreamFlowInfos.size());
    flows.clear();
    string flowId = flows[0]->_upstreamFlowInfos[0].flowId;
    TaskFlowPtr upFlow = mgr.getFlow(flowId);
    ASSERT_TRUE(upFlow->hasTag("cluster1-BatchBuilder-2"));
    flows.clear();

    // test add batch again
    kvMap["batchId"] = "5";
    kvMap["lastBatchId"] = "2";
    ASSERT_TRUE(mgr.loadSubGraph("addBatch", "BatchBuild/SingleClusterAddBatch.graph", kvMap));
    mgr.stepRun();

    mgr.getFlowByTag("cluster1-BatchBuilder-5", flows);
    ASSERT_EQ(1, flows.size());
    ASSERT_EQ(1, flows[0]->_upstreamFlowInfos.size());
    flowId = flows[0]->_upstreamFlowInfos[0].flowId;
    upFlow = mgr.getFlow(flowId);
    ASSERT_TRUE(upFlow->hasTag("cluster1-BatchMerger-2"));
    flows.clear();

    mgr.getFlowByTag("cluster1-BatchMerger-5", flows);
    ASSERT_EQ(1, flows.size());
    ASSERT_EQ(1, flows[0]->_upstreamFlowInfos.size());
    flowId = flows[0]->_upstreamFlowInfos[0].flowId;
    upFlow = mgr.getFlow(flowId);
    ASSERT_TRUE(upFlow->hasTag("cluster1-BatchBuilder-5"));
    flows.clear();
}
/**
TEST_F(BatchBuildGraphTest, TestSingleEndBatchGraph)
{
    KeyValueMap kvMap;
    kvMap["batchId"] = "2";
    kvMap["clusterName"] = "cluster1";

    TaskFactoryPtr factory(new TaskFactory);
    string filePath = TEST_ROOT_PATH() + "/build_service/admin/controlflow/lua";
    TaskFlowManager mgr(filePath, factory, TaskResourceManagerPtr());
    // invalid parameter
    ASSERT_FALSE(mgr.init("BatchBuild/SingleClusterEndBatch.graph", kvMap));

    // start batch 2
    ASSERT_TRUE(mgr.init("BatchBuild/SingleClusterAddBatch.graph", kvMap));
    mgr.stepRun();
    vector<TaskFlowPtr> flows;
    // start batch 3
    kvMap["batchId"] = "3";
    kvMap["lastBatchId"] = "2";
    ASSERT_TRUE(mgr.init("BatchBuild/SingleClusterAddBatch.graph", kvMap));
    mgr.stepRun();
    mgr.getFlowByTag("cluster1-BatchProcessor-3", flows);
    ASSERT_EQ(1, flows.size());
    TaskFlowPtr processor3 = flows[0];
    flows.clear();
    mgr.getFlowByTag("cluster1-BatchBuilder-3", flows);
    ASSERT_EQ(1, flows.size());
    TaskFlowPtr builder3 = flows[0];
    flows.clear();

    // end batch 3 failed, lack of endTime
    ASSERT_FALSE(mgr.loadSubGraph("BatchBuild", "BatchBuild/SingleClusterEndBatch.graph", kvMap));
    mgr.stepRun();
    // end batch 3 success
    kvMap["endTime"] = "1234567";
    ASSERT_TRUE(mgr.loadSubGraph("BatchBuild", "BatchBuild/SingleClusterEndBatch.graph", kvMap));
    mgr.stepRun();
    ASSERT_EQ(kvMap["endTime"], processor3->getProperty("endTime"));

    // mock batch processor 2 to finish, processor 3 begin to run
    stopFlowByTag("cluster1-BatchProcessor-2", mgr);
    mgr.stepRun();
    TaskBasePtr incProcessor = processor3->getTask("incProcessor");
    ASSERT_EQ(kvMap["endTime"], incProcessor->getProperty("endTime"));
    // batch builder3 is not running
    ASSERT_EQ("", builder3->getProperty("endTime"));

    // mock batch 2, builder & merger to finish
    stopFlowByTag("cluster1-BatchBuilder-2", mgr);
    mgr.stepRun();
    stopFlowByTag("cluster1-BatchMerger-2", mgr);
    mgr.stepRun();

    // mock processor 3 to finished
    vector<TaskBasePtr> tasks = processor3->getTasks();
    ASSERT_EQ(1, tasks.size());
    tasks[0]->setTaskStatus(TaskBase::TaskStatus::ts_finish);
    tasks[0]->_endTime = 1234568;
    mgr.stepRun();

    // batch builder3 ready to run, and processor 3 is finish, builder 3 should finish
    TaskBasePtr incBuilder = builder3->getTask("builder");
    ASSERT_EQ("1234573", incBuilder->getProperty("finishTimeInSecond"));
}
**/
TEST_F(BatchBuildGraphTest, TestBatchForMultiCluster)
{
    KeyValueMap kvMap;
    TaskFactoryPtr factory(new TaskFactory);
    string filePath = TEST_ROOT_PATH() + "/build_service/admin/controlflow/lua";
    TaskFlowManager mgr(filePath, factory, TaskResourceManagerPtr());
    // lack batch id
    ASSERT_FALSE(mgr.init("BatchBuild/AddBatch.graph", kvMap));
    kvMap["batchId"] = "2";
    // lack clusterNames
    ASSERT_FALSE(mgr.init("BatchBuild/AddBatch.graph", kvMap));
    kvMap["clusterNames"] = "[\"c1\",\"c2\"]";
    // lack beginTime
    ASSERT_FALSE(mgr.init("BatchBuild/AddBatch.graph", kvMap));
    kvMap["beginTime"] = "12345";
    // lack build id
    ASSERT_FALSE(mgr.init("BatchBuild/AddBatch.graph", kvMap));
    kvMap["buildId"] = "12345";
    // lack schema id map
    ASSERT_FALSE(mgr.init("BatchBuild/AddBatch.graph", kvMap));
    kvMap["schemaIdMap"] = "{\"c1\":\"0\",\"c2\":\"1\"}";
    kvMap["batchMask"] = "2";

    ASSERT_TRUE(mgr.init("BatchBuild/AddBatch.graph", kvMap));
    mgr.stepRun();

    vector<TaskFlowPtr> flows;
    mgr.getFlowByTag("BatchProcessor-2", flows);
    ASSERT_EQ(1, flows.size());
    mgr.getFlowByTag("c1-BatchBuilder-2", flows);
    ASSERT_EQ("", flows[1]->getTask("builder")->getProperty("startTimestamp"));
    mgr.getFlowByTag("c2-BatchBuilder-2", flows);
    ASSERT_EQ("", flows[2]->getTask("builder")->getProperty("startTimestamp"));
    mgr.getFlowByTag("c1-BatchMerger-2", flows);
    mgr.getFlowByTag("c2-BatchMerger-2", flows);
    ASSERT_EQ(5, flows.size());

    // check end batch
    // lack of endTime
    ASSERT_FALSE(mgr.loadSubGraph("endBatch", "BatchBuild/EndBatch.graph", kvMap));
    kvMap["endTime"] = "12356743";
    kvMap["clusterNames"] = "[\"c1\",\"c2\"]";
    ASSERT_TRUE(mgr.loadSubGraph("endBatch", "BatchBuild/EndBatch.graph", kvMap));
    TaskFlowPtr processor = flows[0];
    ASSERT_EQ(kvMap["endTime"], processor->getProperty("endTime"));
}

TEST_F(BatchBuildGraphTest, TestBatchFullBuild)
{
    KeyValueMap kvMap;
    TaskFactoryPtr factory(new TaskFactory);
    string filePath = TEST_ROOT_PATH() + "/build_service/admin/controlflow/lua";
    TaskFlowManager mgr(filePath, factory, TaskResourceManagerPtr());
    // lack clustername
    ASSERT_FALSE(mgr.init("BatchBuild/FullBatchBuild.graph", kvMap));
    kvMap["clusterNames"] = "[\"c1\",\"c2\"]";
    kvMap["schemaIdMap"] = "{\"c1\":\"0\",\"c2\":\"1\"}";
    // lack dataDesc
    ASSERT_FALSE(mgr.init("BatchBuild/FullBatchBuild.graph", kvMap));
    kvMap["dataDescriptions"] = "data1";
    // lack buildId
    ASSERT_FALSE(mgr.init("BatchBuild/FullBatchBuild.graph", kvMap));
    kvMap["buildId"] = "build1";
    // lack buildStep
    ASSERT_FALSE(mgr.init("BatchBuild/FullBatchBuild.graph", kvMap));
    kvMap["buildStep"] = "full";
    ASSERT_TRUE(mgr.init("BatchBuild/FullBatchBuild.graph", kvMap));

    EXPECT_EQ(7, mgr._flowContainer->_flowMap.size());
    mgr.stepRun();
    // mock finish prepare data source
    TaskFlowPtr preDataSource = mgr.getFlow("0");
    preDataSource->_status = TaskFlow::TF_STATUS::tf_finish;
    mgr.stepRun();
    TaskFlowPtr controlFlow = mgr.getFlow("1");
    EXPECT_EQ("4", controlFlow->_upstreamFlowInfos[0].flowId); // merger1
    EXPECT_EQ("6", controlFlow->_upstreamFlowInfos[1].flowId); // merger2
    // check processor parameters and upstream
    TaskFlowPtr processor = mgr.getFlow("2");
    EXPECT_EQ("0", processor->_upstreamFlowInfos[0].flowId);
    ASSERT_TRUE(processor->getTask("fullProcessor"));
    EXPECT_EQ("[\"c1\",\"c2\"]", processor->getTask("fullProcessor")->getProperty("clusterName"));
    EXPECT_EQ("data1", processor->getTask("fullProcessor")->getProperty("dataDescriptions"));
    EXPECT_EQ("build1", processor->getTask("fullProcessor")->getProperty("buildId"));
    EXPECT_EQ("full", processor->getTask("fullProcessor")->getProperty("buildStep"));

    // check builder parameters and upstream
    TaskFlowPtr builder1 = mgr.getFlow("3");
    TaskFlowPtr builder2 = mgr.getFlow("5");
    EXPECT_EQ("0", builder1->_upstreamFlowInfos[0].flowId);
    EXPECT_EQ("0", builder2->_upstreamFlowInfos[0].flowId);
    EXPECT_EQ("c1", builder1->getTask("builder")->getProperty("clusterName"));
    EXPECT_EQ("data1", builder1->getTask("builder")->getProperty("dataDescriptions"));
    EXPECT_EQ("build1", builder1->getTask("builder")->getProperty("buildId"));
    EXPECT_EQ("full", builder1->getTask("builder")->getProperty("buildStep"));
    // check merger parameters and upstream
    TaskFlowPtr merger1 = mgr.getFlow("4");
    TaskFlowPtr merger2 = mgr.getFlow("6");
    EXPECT_EQ("3", merger1->_upstreamFlowInfos[0].flowId);
    EXPECT_EQ("5", merger2->_upstreamFlowInfos[0].flowId);

    // mock finish processor
    int64_t curTs = TimeUtility::currentTimeInSeconds();
    processor->getTask("fullProcessor")->setTaskStatus(TaskBase::TaskStatus::ts_finish);
    mgr.stepRun();
    string builderStopTs = builder1->getTask("builder")->getProperty("finishTimeInSecond");
    int64_t stopTs = -1;
    ASSERT_TRUE(StringUtil::fromString(builderStopTs, stopTs));
    ASSERT_EQ(stopTs, curTs);
    // mock finish builder
    builder1->getTask("builder")->setTaskStatus(TaskBase::TaskStatus::ts_finish);
    builder2->getTask("builder")->setTaskStatus(TaskBase::TaskStatus::ts_finish);
    mgr.stepRun();
    // control flow will not run when merger is not finish
    ASSERT_FALSE(controlFlow->getTask("batchController"));
    // mock finish merger
    merger1->getTask("merger")->setTaskStatus(TaskBase::TaskStatus::ts_finish);
    merger2->getTask("merger")->setTaskStatus(TaskBase::TaskStatus::ts_finish);
    mgr.stepRun();
    mgr.stepRun();
    // control flow begin to run when full merge is finished
    ASSERT_TRUE(controlFlow->getTask("batchController"));
}

}} // namespace build_service::admin
