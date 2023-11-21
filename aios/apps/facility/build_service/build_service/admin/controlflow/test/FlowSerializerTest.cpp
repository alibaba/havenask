#include "build_service/admin/controlflow/FlowSerializer.h"

#include <assert.h>
#include <cstddef>
#include <ext/alloc_traits.h>
#include <map>
#include <memory>
#include <string>
#include <vector>

#include "autil/legacy/any.h"
#include "autil/legacy/exception.h"
#include "autil/legacy/json.h"
#include "autil/legacy/legacy_jsonizable.h"
#include "autil/legacy/legacy_jsonizable_dec.h"
#include "build_service/admin/controlflow/TaskContainer.h"
#include "build_service/admin/controlflow/TaskFactory.h"
#include "build_service/admin/controlflow/TaskFlow.h"
#include "build_service/admin/controlflow/TaskResourceManager.h"
#include "build_service/common_define.h"
#include "build_service/test/unittest.h"
#include "unittest/unittest.h"

using namespace std;
using namespace autil::legacy;
using namespace autil::legacy::json;
using namespace testing;

namespace build_service { namespace admin {

class FlowSerializerTest : public BUILD_SERVICE_TESTBASE
{
public:
    void setUp();
    void tearDown();
};

void FlowSerializerTest::setUp() {}

void FlowSerializerTest::tearDown() {}

TaskFlowPtr prepareTaskFlow(const string& rootPath, const string& fileName, const string& flowId, const string& graphId)
{
    TaskFactoryPtr factory(new TaskFactory);
    TaskFlowPtr flow(new TaskFlow(rootPath, factory));
    if (!flow->init(fileName, flowId)) {
        return TaskFlowPtr();
    }
    flow->setGraphId(graphId);
    flow->setGlobalTaskParam("auto_finish", "true");
    return flow;
}

TaskFlowPtr prepareSimpleTaskFlow(const string& flowId, const string& taskId, const KeyValueMap& kvMap,
                                  const string& graphId)
{
    TaskFactoryPtr factory(new TaskFactory);
    TaskFlowPtr flow(new TaskFlow("", factory));
    if (!flow->initSimpleFlow(taskId, "", kvMap, flowId)) {
        return TaskFlowPtr();
    }

    flow->setGraphId(graphId);
    flow->setGlobalTaskParam("auto_finish", "true");
    return flow;
}

void checkFlow(const TaskFlowPtr& lft, const TaskFlowPtr& rht)
{
    ASSERT_EQ(lft->_flowId, rht->_flowId);
    ASSERT_EQ(lft->_scriptInfo, rht->_scriptInfo);
    ASSERT_EQ(lft->_graphId, rht->_graphId);
    ASSERT_EQ(lft->_status, rht->_status);
    ASSERT_EQ(lft->_taskIdSet, rht->_taskIdSet);
    ASSERT_EQ(lft->_taskKVMap, rht->_taskKVMap);
    ASSERT_EQ(lft->_globalParams, rht->_globalParams);
    ASSERT_EQ(ToJsonString(lft->_upstreamFlowInfos), ToJsonString(rht->_upstreamFlowInfos));
    ASSERT_EQ(lft->_errorMsg, rht->_errorMsg);
}

void checkSerialize(const string& rootPath, vector<TaskFlowPtr>& flows)
{
    assert(!flows.empty());
    FlowSerializer fs(rootPath, flows[0]->_taskFactory, flows[0]->_taskContainer, flows[0]->_taskResMgr);

    Jsonizable::JsonWrapper json;
    FlowSerializer::serialize(json, flows);
    string serializeStr = ToString(ToJson(json.GetMap()));

    vector<TaskFlowPtr> tmp;
    tmp.swap(flows);

    Any any;
    ParseJson(serializeStr, any);
    Jsonizable::JsonWrapper wrapper(any);
    ASSERT_TRUE(fs.deserialize(wrapper, flows));
    ASSERT_EQ(tmp.size(), flows.size());

    Jsonizable::JsonWrapper newJson;
    FlowSerializer::serialize(newJson, flows);
    string newStr = ToString(ToJson(newJson.GetMap()));
    ASSERT_EQ(serializeStr, newStr);
}

TEST_F(FlowSerializerTest, testSimple)
{
    TaskContainerPtr container(new TaskContainer);
    TaskResourceManagerPtr resMgr(new TaskResourceManager);

    string rootPath = TEST_ROOT_PATH() + "/build_service/admin/controlflow/test/lua_scripts";
    TaskFlowPtr fullFlow = prepareTaskFlow(rootPath, "test_flow.flow", "full", "main");
    fullFlow->_taskContainer = container;
    fullFlow->_taskResMgr = resMgr;

    KeyValueMap kvMap;
    kvMap["key"] = "value";
    TaskFlowPtr simpleFlow = prepareSimpleTaskFlow("simple", "task", kvMap, "sub");
    simpleFlow->_taskContainer = container;
    simpleFlow->_taskResMgr = resMgr;
    simpleFlow->addUpstreamFlowId("full", "stop");
    simpleFlow->setErrorMsg("test error!");

    vector<TaskFlowPtr> flows;
    flows.push_back(fullFlow);
    flows.push_back(simpleFlow);
    checkSerialize(rootPath, flows);

    flows[0]->makeActive();
    flows[1]->makeActive();

    for (size_t i = 0; i < 5; i++) {
        flows[0]->stepRun();
        flows[1]->stepRun();
        checkSerialize(rootPath, flows);
    }
}

}} // namespace build_service::admin
