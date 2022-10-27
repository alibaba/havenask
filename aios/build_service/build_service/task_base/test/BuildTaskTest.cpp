#include "build_service/test/unittest.h"
#include "build_service/task_base/BuildTask.h"
#include "build_service/config/ConfigDefine.h"
#include "build_service/config/CLIOptionNames.h"
#include "build_service/config/BuildRuleConfig.h"
#include "build_service/workflow/test/MockBrokerFactory.h"
#include "build_service/task_base/test/MockBuildTask.h"
#include "build_service/builder/test/MockBuilder.h"
#include "build_service/proto/ProtoUtil.h"
#include "build_service/util/IndexPathConstructor.h"
#include "build_service/util/FileUtil.h"
#include <autil/legacy/jsonizable.h>
#include <indexlib/config/index_partition_options.h>
#include <indexlib/index_base/schema_adapter.h>

using namespace std;
using namespace autil::legacy;
using namespace testing;
using namespace build_service::proto;
using namespace build_service::config;
using namespace build_service::workflow;
using namespace build_service::builder;
using namespace build_service::util;
using namespace heavenask::indexlib;
IE_NAMESPACE_USE(index_base);
IE_NAMESPACE_USE(config);

namespace build_service {
namespace task_base {

class BuildTaskTest : public BUILD_SERVICE_TESTBASE {
public:
    void setUp();
    void tearDown();
protected:
    KeyValueMap _kvMap;
};

void BuildTaskTest::setUp() {
    _kvMap[CONFIG_PATH] = TEST_DATA_PATH"/build_task_base_test/no_need_partition_config";
    _kvMap[CLUSTER_NAMES] = "cluster1";
    _kvMap[GENERATION_ID] = "5";
    _kvMap[INDEX_ROOT_PATH] = GET_TEST_DATA_PATH();
    _kvMap[BUILD_MODE] = BUILD_MODE_FULL;
    _kvMap[NEED_PARTITION] = "false";
    _kvMap[SRC_SIGNATURE] = "1234";
}

void BuildTaskTest::tearDown() {
}

TEST_F(BuildTaskTest, testInvalidJobStrFormat) {
    NiceMockBuildTask buildTaskBase;
    // from json from empty string
    StrictMockBrokerFactory brokerFactory;
    int32_t instanceId = 0;
    EXPECT_TRUE(!buildTaskBase.run("", &brokerFactory, instanceId,
                                   TaskBase::BUILD_MAP));
}

TEST_F(BuildTaskTest, testInvalidJobStr) {
    NiceMockBuildTask buildTaskBase;
    KeyValueMap kvMap;
    string jobParam = ToJsonString(kvMap);
    StrictMockBrokerFactory brokerFactory;
    int32_t instanceId = 0;
    EXPECT_TRUE(!buildTaskBase.run(jobParam, &brokerFactory, instanceId,
                                   TaskBase::BUILD_MAP));
}

TEST_F(BuildTaskTest, testMapCreateFlowFailed) {
    NiceMockBuildTask buildTaskBase;
    _kvMap[CONFIG_PATH] = TEST_DATA_PATH"/build_task_base_test/need_partition_config";
    _kvMap[NEED_PARTITION] = "true";
    string jobParam = ToJsonString(_kvMap);
    StrictMockBrokerFactory brokerFactory;
    EXPECT_CALL(brokerFactory, createProcessedDocConsumer(_)).WillOnce(ReturnNull());
    int32_t instanceId = 0;
    EXPECT_TRUE(!buildTaskBase.run(jobParam, &brokerFactory, instanceId,
                                   TaskBase::BUILD_MAP));
}

TEST_F(BuildTaskTest, testReduceCreateFlowFailed) {
    NiceMockBuildTask buildTaskBase;
    _kvMap[CONFIG_PATH] = TEST_DATA_PATH"/build_task_base_test/need_partition_config";
    _kvMap[NEED_PARTITION] = "true";
    string jobParam = ToJsonString(_kvMap);
    StrictMockBrokerFactory brokerFactory;
    EXPECT_CALL(brokerFactory, createProcessedDocProducer(_)).WillOnce(ReturnNull());
    int32_t instanceId = 0;
    EXPECT_TRUE(!buildTaskBase.run(jobParam, &brokerFactory, instanceId,
                                   TaskBase::BUILD_REDUCE));
}

TEST_F(BuildTaskTest, testNeedPartitionMap) {
    NiceMockBuildTask buildTaskBase;
    _kvMap[CONFIG_PATH] = TEST_DATA_PATH"/build_task_base_test/need_partition_config";
    _kvMap[NEED_PARTITION] = "true";
    string jobParam = ToJsonString(_kvMap);
    StrictMockBrokerFactory brokerFactory;
    EXPECT_CALL(brokerFactory, createProcessedDocConsumer(_));
    int32_t instanceId = 0;
    EXPECT_TRUE(buildTaskBase.run(jobParam, &brokerFactory, instanceId,
                                  TaskBase::BUILD_MAP));
}

TEST_F(BuildTaskTest, testNeedPartitionReduce) {
    NiceMockBuildTask buildTaskBase;
    _kvMap[CONFIG_PATH] = TEST_DATA_PATH"/build_task_base_test/need_partition_config";
    _kvMap[NEED_PARTITION] = "true";
    string jobParam = ToJsonString(_kvMap);

    NiceMockBuildFlow *buildFlow = new NiceMockBuildFlow();
    EXPECT_CALL(buildTaskBase, createBuildFlow()).WillOnce(Return(buildFlow));

    StrictMockBrokerFactory brokerFactory;
    EXPECT_CALL(brokerFactory, createProcessedDocProducer(_));
    int32_t instanceId = 0;
    EXPECT_TRUE(buildTaskBase.run(jobParam, &brokerFactory, instanceId,
                                  TaskBase::BUILD_REDUCE));
}

TEST_F(BuildTaskTest, testNoNeedPartitionMap) {
    NiceMockBuildTask buildTaskBase;
    _kvMap[CONFIG_PATH] = TEST_DATA_PATH"/build_task_base_test/no_need_partition_config";
    string jobParam = ToJsonString(_kvMap);
    StrictMockBrokerFactory brokerFactory;
    int32_t instanceId = 0;
    EXPECT_TRUE(buildTaskBase.run(jobParam, &brokerFactory, instanceId,
                                  TaskBase::BUILD_MAP));
}

TEST_F(BuildTaskTest, testGetBuildFlowMode) {
    EXPECT_EQ(BuildFlow::READER_AND_PROCESSOR, BuildTask::getBuildFlowMode(true, TaskBase::BUILD_MAP));
    EXPECT_EQ(BuildFlow::BUILDER, BuildTask::getBuildFlowMode(true, TaskBase::BUILD_REDUCE));
    EXPECT_EQ(BuildFlow::ALL, BuildTask::getBuildFlowMode(false, TaskBase::BUILD_MAP));
    EXPECT_EQ(BuildFlow::NONE, BuildTask::getBuildFlowMode(false, TaskBase::BUILD_REDUCE));
}

TEST_F(BuildTaskTest, testBuildRawFileIndex) {
    NiceMockBuildTask buildTaskBase;
    string configRoot = TEST_DATA_PATH"/build_task_rawfile_test/config/";
    _kvMap[CONFIG_PATH] = configRoot;
    _kvMap[NEED_PARTITION] = "true";

    StrictMockBrokerFactory brokerFactory;
    int32_t instanceId = 0;

    _kvMap[READ_SRC] = "";
    string jobParam = ToJsonString(_kvMap);
    EXPECT_FALSE(buildTaskBase.run(jobParam, &brokerFactory, instanceId,
                                   TaskBase::BUILD_REDUCE));
    _kvMap[READ_SRC] = FILE_READ_SRC;
    _kvMap[DATA_PATH] = configRoot;
    _kvMap[INDEX_ROOT_PATH] = GET_TEST_DATA_PATH();
    _kvMap[CLUSTER_NAMES] = "";

    jobParam = ToJsonString(_kvMap);
    EXPECT_FALSE(buildTaskBase.run(jobParam, &brokerFactory, instanceId,
                                   TaskBase::BUILD_REDUCE));

    _kvMap[CLUSTER_NAMES] = "simple_cluster";
    _kvMap[READ_SRC_TYPE] = FILE_READ_SRC;
    jobParam = ToJsonString(_kvMap);
    EXPECT_TRUE(buildTaskBase.run(jobParam, &brokerFactory, instanceId,
                                  TaskBase::BUILD_REDUCE));
    auto schema = IE_NAMESPACE(index_base)::SchemaAdapter::LoadSchema(
            _kvMap[INDEX_ROOT_PATH] + "/simple_cluster/generation_5/partition_0_65535/", "schema.json");
    ASSERT_TRUE(schema);
    EXPECT_EQ(tt_rawfile, schema->GetTableType());
    string value;
    EXPECT_TRUE(schema->GetValueFromUserDefinedParam(BS_FILE_NAME, value));
    EXPECT_EQ("config", value);

    {
        // restart and recover
        NiceMockBuildTask buildTaskBase2;
        EXPECT_TRUE(buildTaskBase2.run(jobParam, &brokerFactory, instanceId,
                        TaskBase::BUILD_REDUCE));
        auto schema2 = IE_NAMESPACE(index_base)::SchemaAdapter::LoadSchema(
                _kvMap[INDEX_ROOT_PATH] + "/simple_cluster/generation_5/partition_0_65535/", "schema.json");
        ASSERT_TRUE(schema2);
        EXPECT_EQ(tt_rawfile, schema2->GetTableType());
        string value;
        EXPECT_TRUE(schema->GetValueFromUserDefinedParam(BS_FILE_NAME, value));
        EXPECT_EQ("config", value);
    }
}

TEST_F(BuildTaskTest, testIncMerge) {
    _kvMap[BUILD_MODE] = BUILD_MODE_INCREMENTAL;
    string jobParam = ToJsonString(_kvMap);

    StrictMockBrokerFactory brokerFactory;

    {
        NiceMockBuildTask buildTaskBase;
        NiceMockBuildFlow *buildFlow = new NiceMockBuildFlow();
        EXPECT_CALL(buildTaskBase, createBuildFlow()).WillOnce(Return(buildFlow));

        NiceMockBuilder *builder = new NiceMockBuilder();
        EXPECT_CALL(*buildFlow, getBuilder()).WillRepeatedly(Return(builder));
        EXPECT_CALL(*builder, doMerge(_)).Times(1);

        int32_t instanceId = 0;

        EXPECT_TRUE(buildTaskBase.run(jobParam, &brokerFactory,
                        instanceId, TaskBase::BUILD_MAP));
        delete builder;
    }
}

TEST_F(BuildTaskTest, testIncMergeFailed) {
    _kvMap[BUILD_MODE] = BUILD_MODE_INCREMENTAL;
    string jobParam = ToJsonString(_kvMap);

    StrictMockBrokerFactory brokerFactory;

    {
        NiceMockBuildTask buildTaskBase;
        NiceMockBuildFlow *buildFlow = new NiceMockBuildFlow();
        EXPECT_CALL(buildTaskBase, createBuildFlow()).WillOnce(Return(buildFlow));

        NiceMockBuilder *builder = new NiceMockBuilder();
        EXPECT_CALL(*buildFlow, getBuilder()).WillRepeatedly(Return(builder));
        EXPECT_CALL(*builder, doMerge(_)).WillOnce(Throw(IE_NAMESPACE(misc)::FileIOException()));

        int32_t instanceId = 0;

        EXPECT_FALSE(buildTaskBase.run(jobParam, &brokerFactory,
                        instanceId, TaskBase::BUILD_MAP));
        delete builder;
    }
}

}
}
