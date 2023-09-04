#include "indexlib/table/index_task/ComplexIndexTaskPlanCreator.h"

#include "indexlib/config/IndexTaskConfig.h"
#include "indexlib/config/OfflineConfig.h"
#include "indexlib/config/TabletOptions.h"
#include "indexlib/framework/TabletData.h"
#include "indexlib/framework/Version.h"
#include "indexlib/framework/index_task/IIndexOperationCreator.h"
#include "indexlib/framework/index_task/IndexOperation.h"
#include "indexlib/framework/index_task/IndexTaskContext.h"
#include "indexlib/framework/index_task/IndexTaskHistory.h"
#include "indexlib/framework/index_task/IndexTaskPlan.h"
#include "indexlib/table/index_task/SimpleIndexTaskPlanCreator.h"
#include "unittest/unittest.h"

using namespace std;

namespace indexlibv2 { namespace table {

class FakeSimpleTaskPlanCreator : public SimpleIndexTaskPlanCreator
{
public:
    FakeSimpleTaskPlanCreator(const std::string& taskName, const std::map<std::string, std::string>& params)
        : SimpleIndexTaskPlanCreator(taskName, params)
    {
    }

    std::pair<Status, std::unique_ptr<framework::IndexTaskPlan>>
    CreateTaskPlan(const framework::IndexTaskContext* taskContext) override
    {
        auto indexTaskPlan = std::make_unique<framework::IndexTaskPlan>(_taskName, TASK_TYPE);
        indexTaskPlan->AddOperation(framework::IndexOperationDescription());
        return {Status::OK(), std::move(indexTaskPlan)};
    }

    std::string ConstructLogTaskType() const override { return string("fake@") + _taskName; }

    std::string ConstructLogTaskId(const framework::IndexTaskContext* taskContext,
                                   const framework::Version& targetVersion) const override
    {
        return "id";
    }

    static const std::string TASK_TYPE;
};

class FakeIndexOperation : public framework::IndexOperation
{
public:
    FakeIndexOperation() : framework::IndexOperation(-1, true) {}
    Status Execute(const framework::IndexTaskContext& context) override { return Status::OK(); }
};

class FakeIndexOperationCreator : public framework::IIndexOperationCreator
{
    std::unique_ptr<framework::IndexOperation>
    CreateOperation(const framework::IndexOperationDescription& opDesc) override
    {
        return std::make_unique<FakeIndexOperation>();
    }
};

const std::string FakeSimpleTaskPlanCreator::TASK_TYPE = "fake";

class FakeComplexTaskPlanCreator : public ComplexIndexTaskPlanCreator
{
public:
    FakeComplexTaskPlanCreator() { RegisterSimpleCreator<FakeSimpleTaskPlanCreator>(); }
    std::shared_ptr<framework::IIndexOperationCreator>
    CreateIndexOperationCreator(const std::shared_ptr<config::ITabletSchema>& tabletSchema) override
    {
        return std::make_shared<FakeIndexOperationCreator>();
    }
};

class ComplexIndexTaskPlanCreatorTest : public TESTBASE
{
public:
    ComplexIndexTaskPlanCreatorTest();
    ~ComplexIndexTaskPlanCreatorTest();

public:
    void setUp() override {}
    void tearDown() override {}
    void TestSimpleProcess();

private:
    std::shared_ptr<framework::IndexTaskContext> PrepareContext(const std::string& indexTaskConfigStr,
                                                                const std::string& logInfoStr);

private:
    AUTIL_LOG_DECLARE();
};

AUTIL_LOG_SETUP(indexlib.table, ComplexIndexTaskPlanCreatorTest);

ComplexIndexTaskPlanCreatorTest::ComplexIndexTaskPlanCreatorTest() {}

ComplexIndexTaskPlanCreatorTest::~ComplexIndexTaskPlanCreatorTest() {}

// indexTaskConfigStr = merge;inc_merge;period=300|reclaim:index;daytime=12:00
// indexTaskConfigStr = merge;inc_merge;123123456|reclaim;index;12361334263
std::shared_ptr<framework::IndexTaskContext>
ComplexIndexTaskPlanCreatorTest::PrepareContext(const std::string& indexTaskConfigStr, const std::string& logInfoStr)
{
    auto context = std::make_shared<framework::IndexTaskContext>();
    auto options = std::make_shared<config::TabletOptions>();
    string jsonTemplateStr = R"(
    {
        "task_type" : "$TYPE",
        "task_name" : "$NAME",
        "trigger" : "$PERIOD"
    })";

    vector<vector<string>> configStrVec;
    autil::StringUtil::fromString(indexTaskConfigStr, configStrVec, ";", "|");
    for (size_t i = 0; i < configStrVec.size(); i++) {
        assert(configStrVec[i].size() == 3);
        string tmpJson = jsonTemplateStr;
        autil::StringUtil::replaceAll(tmpJson, "$TYPE", configStrVec[i][0]);
        autil::StringUtil::replaceAll(tmpJson, "$NAME", configStrVec[i][1]);
        autil::StringUtil::replaceAll(tmpJson, "$PERIOD", configStrVec[i][2]);

        config::IndexTaskConfig taskConfig;
        FromJsonString(taskConfig, tmpJson);
        options->TEST_GetOfflineConfig().TEST_GetIndexTaskConfigs().emplace_back(taskConfig);
    }
    context->SetTabletOptions(options);

    framework::IndexTaskHistory his;
    vector<vector<string>> logStrVec;
    autil::StringUtil::fromString(logInfoStr, logStrVec, ";", "|");
    for (size_t i = 0; i < logStrVec.size(); i++) {
        assert(logStrVec[i].size() == 3);
        string logType = logStrVec[i][0] + "@" + logStrVec[i][1];
        std::map<std::string, std::string> params;
        auto logItem = std::make_shared<framework::IndexTaskLog>(
            "id", i, i + 1, autil::StringUtil::fromString<int64_t>(logStrVec[i][2]), params);
        his.AddLog(logType, logItem);
    }

    framework::Version version;
    version.SetIndexTaskHistory(his);
    auto tabletData = std::make_shared<framework::TabletData>("no_name");
    tabletData->TEST_SetOnDiskVersion(version);
    context->TEST_SetTabletData(tabletData);
    return context;
}

TEST_F(ComplexIndexTaskPlanCreatorTest, TestSimpleProcess)
{
    FakeComplexTaskPlanCreator creator;
    auto context = PrepareContext("fake;task1;period=300|fake;task2;period=600|fake;task3;manual|fake;task4;auto",
                                  "fake;task1;100|fake;task2;200|fake;task3;50|fake;task4;300");

    std::vector<ComplexIndexTaskPlanCreator::SimpleTaskItem> taskItems;
    ASSERT_TRUE(creator.GetCandidateTasks(context.get(), &taskItems).IsOK());
    ASSERT_EQ(3, taskItems.size());
    std::vector<ComplexIndexTaskPlanCreator::SimpleTaskItem> task;
    ASSERT_TRUE(creator.ScheduleSimpleTask(context.get(), &task).IsOK());
    ASSERT_EQ("task1", task[0].taskName);
}

TEST_F(ComplexIndexTaskPlanCreatorTest, TestUnknownTaskType)
{
    FakeComplexTaskPlanCreator creator;
    auto context = PrepareContext("unknown;task1;period=300|fake;task2;period=600", "fake;task1;100|fake;task2;200");

    std::vector<ComplexIndexTaskPlanCreator::SimpleTaskItem> taskItems;
    ASSERT_TRUE(!creator.GetCandidateTasks(context.get(), &taskItems).IsOK());
}

TEST_F(ComplexIndexTaskPlanCreatorTest, TestSetDesignateTask)
{
    FakeComplexTaskPlanCreator creator;
    auto context = PrepareContext("fake;task1;period=300|fake;task2;period=600|fake;task3;manual",
                                  "fake;task1;100|fake;task2;200");

    context->SetDesignateTask("fake", "task3");
    std::vector<ComplexIndexTaskPlanCreator::SimpleTaskItem> task;
    ASSERT_TRUE(creator.ScheduleSimpleTask(context.get(), &task).IsOK());
    ASSERT_EQ("task3", task[0].taskName);
}

TEST_F(ComplexIndexTaskPlanCreatorTest, TestSetDesignateTask_2)
{
    FakeComplexTaskPlanCreator creator;
    auto context =
        PrepareContext("fake;task1;period=300|fake;task2;manual|fake;task3;manual", "fake;task1;100|fake;task2;200");
    context->SetDesignateTask("fake", "task3");
    std::vector<ComplexIndexTaskPlanCreator::SimpleTaskItem> task;
    ASSERT_TRUE(creator.ScheduleSimpleTask(context.get(), &task).IsOK());
    ASSERT_EQ("task3", task[0].taskName);
}

// test period
TEST_F(ComplexIndexTaskPlanCreatorTest, TestPeriodTrigger)
{
    // currentTs - half an hour
    int64_t taskLogTs = autil::TimeUtility::currentTimeInSeconds() - 1800;
    string logStr = "fake;task1;" + autil::StringUtil::toString(taskLogTs);

    FakeComplexTaskPlanCreator creator;
    auto context = PrepareContext("fake;task1;period=3600", logStr);
    std::vector<ComplexIndexTaskPlanCreator::SimpleTaskItem> taskItems;
    ASSERT_TRUE(creator.GetCandidateTasks(context.get(), &taskItems).IsOK());
    ASSERT_EQ(0, taskItems.size());
    std::vector<ComplexIndexTaskPlanCreator::SimpleTaskItem> task;
    ASSERT_TRUE(creator.ScheduleSimpleTask(context.get(), &task).IsOK());
    ASSERT_EQ("merge", task[0].taskType);
    ASSERT_EQ("", task[0].taskName);

    context = PrepareContext("fake;task1;period=300", logStr);
    ASSERT_TRUE(creator.GetCandidateTasks(context.get(), &taskItems).IsOK());
    ASSERT_EQ(1, taskItems.size());
    ASSERT_TRUE(creator.ScheduleSimpleTask(context.get(), &task).IsOK());
    ASSERT_EQ("task1", task[0].taskName);
}

// test daytime
TEST_F(ComplexIndexTaskPlanCreatorTest, TestDaytimeTrigger)
{
    int64_t currentTime = autil::TimeUtility::currentTimeInSeconds();
    // log time > trigger time
    int64_t taskLogTs = currentTime;
    string logStr = "fake;task1;" + autil::StringUtil::toString(taskLogTs);

    FakeComplexTaskPlanCreator creator;
    auto context = PrepareContext("fake;task1;daytime=00:00", logStr);
    std::vector<ComplexIndexTaskPlanCreator::SimpleTaskItem> taskItems;
    ASSERT_TRUE(creator.GetCandidateTasks(context.get(), &taskItems).IsOK());
    ASSERT_EQ(0, taskItems.size());

    // current time > trigger time > log time
    taskLogTs = currentTime - 3600 * 24;
    logStr = "fake;task1;" + autil::StringUtil::toString(taskLogTs);
    context = PrepareContext("fake;task1;daytime=00:00", logStr);
    ASSERT_TRUE(creator.GetCandidateTasks(context.get(), &taskItems).IsOK());
    ASSERT_EQ(1, taskItems.size());

    // trigger time > current time > log time
    taskLogTs = currentTime - 3600 * 24;
    logStr = "fake;task1;" + autil::StringUtil::toString(taskLogTs);
    context = PrepareContext("fake;task1;daytime=23:59", logStr);
    ASSERT_TRUE(creator.GetCandidateTasks(context.get(), &taskItems).IsOK());
    ASSERT_EQ(1, taskItems.size());
}

TEST_F(ComplexIndexTaskPlanCreatorTest, TestIndexTaskQueueSchedule)
{
    int64_t currentTime = autil::TimeUtility::currentTimeInSeconds();
    string logStr = "fake;task1;" + autil::StringUtil::toString(currentTime - 3600 * 24) + "|" + "fake;task2;" +
                    autil::StringUtil::toString(currentTime - 3600 * 30) + "|" + "fake;task3;" +
                    autil::StringUtil::toString(currentTime - 3600 * 26);

    string taskConfigStr = "fake;task1;daytime=00:00|"
                           "fake;task2;daytime=00:00|"
                           "fake;task3;daytime=00:00";

    FakeComplexTaskPlanCreator creator;
    auto context = PrepareContext(taskConfigStr, logStr);
    auto version = context->GetTabletData()->GetOnDiskVersion();
    std::map<std::string, std::string> params1;
    params1["key1"] = "value1";
    version.AddIndexTask("type1", "name1", params1);
    std::map<std::string, std::string> params2;
    params2["key2"] = "value2";
    version.AddIndexTask("type2", "name2", params2);

    context->GetTabletData()->TEST_SetOnDiskVersion(version);
    std::vector<ComplexIndexTaskPlanCreator::SimpleTaskItem> task;
    ASSERT_TRUE(creator.ScheduleSimpleTask(context.get(), &task).IsOK());
    ASSERT_EQ(task.size(), 1);
    ASSERT_EQ(task[0].taskType, "type1");
    ASSERT_EQ(task[0].taskName, "name1");
    ASSERT_EQ(task[0].params, params1);
    version.UpdateIndexTaskState("type1", "name1", framework::IndexTaskMeta::DONE);
    context->GetTabletData()->TEST_SetOnDiskVersion(version);
    ASSERT_TRUE(creator.ScheduleSimpleTask(context.get(), &task).IsOK());
    ASSERT_EQ(task.size(), 1);
    ASSERT_EQ(task[0].taskType, "type2");
    ASSERT_EQ(task[0].taskName, "name2");
    ASSERT_EQ(task[0].params, params2);
}

TEST_F(ComplexIndexTaskPlanCreatorTest, TestFailedIndexTaskInQueueSchedule)
{
    int64_t currentTime = autil::TimeUtility::currentTimeInSeconds();
    string logStr = "fake;task1;" + autil::StringUtil::toString(currentTime - 3600 * 24) + "|" + "fake;task2;" +
                    autil::StringUtil::toString(currentTime - 3600 * 30) + "|" + "fake;task3;" +
                    autil::StringUtil::toString(currentTime - 3600 * 26);

    string taskConfigStr = "fake;task1;daytime=00:00|"
                           "fake;task2;daytime=00:00|"
                           "fake;task3;daytime=00:00";

    FakeComplexTaskPlanCreator creator;
    auto context = PrepareContext(taskConfigStr, logStr);
    auto version = context->GetTabletData()->GetOnDiskVersion();
    std::map<std::string, std::string> params1;
    params1["key1"] = "value1";
    version.AddIndexTask("type1", "name1", params1);
    std::map<std::string, std::string> params2;
    params2["key2"] = "value2";
    version.AddIndexTask("type2", "name2", params2);

    context->GetTabletData()->TEST_SetOnDiskVersion(version);
    std::vector<ComplexIndexTaskPlanCreator::SimpleTaskItem> task;
    ASSERT_TRUE(creator.ScheduleSimpleTask(context.get(), &task).IsOK());
    ASSERT_EQ(task.size(), 1);
    ASSERT_EQ(task[0].taskType, "type1");
    ASSERT_EQ(task[0].taskName, "name1");
    ASSERT_EQ(task[0].params, params1);
    version.UpdateIndexTaskState("type1", "name1", framework::IndexTaskMeta::SUSPENDED);
    context->GetTabletData()->TEST_SetOnDiskVersion(version);
    ASSERT_TRUE(creator.ScheduleSimpleTask(context.get(), &task).IsOK());
    ASSERT_EQ(task.size(), 4);
    ASSERT_EQ("fake", task[0].taskType);
    ASSERT_EQ("task2", task[0].taskName);

    ASSERT_EQ("fake", task[1].taskType);
    ASSERT_EQ("task3", task[1].taskName);

    ASSERT_EQ("fake", task[2].taskType);
    ASSERT_EQ("task1", task[2].taskName);
}

// test priority
TEST_F(ComplexIndexTaskPlanCreatorTest, TestPriority)
{
    int64_t currentTime = autil::TimeUtility::currentTimeInSeconds();
    string logStr = "fake;task1;" + autil::StringUtil::toString(currentTime - 3600 * 24) + "|" + "fake;task2;" +
                    autil::StringUtil::toString(currentTime - 3600 * 30) + "|" + "fake;task3;" +
                    autil::StringUtil::toString(currentTime - 3600 * 26);

    string taskConfigStr = "fake;task1;daytime=00:00|"
                           "fake;task2;daytime=00:00|"
                           "fake;task3;daytime=00:00";

    FakeComplexTaskPlanCreator creator;
    auto context = PrepareContext(taskConfigStr, logStr);
    std::vector<ComplexIndexTaskPlanCreator::SimpleTaskItem> taskItems;
    ASSERT_TRUE(creator.GetCandidateTasks(context.get(), &taskItems).IsOK());
    ASSERT_EQ(3, taskItems.size());

    std::vector<ComplexIndexTaskPlanCreator::SimpleTaskItem> task;
    ASSERT_TRUE(creator.ScheduleSimpleTask(context.get(), &task).IsOK());
    ASSERT_EQ("fake", task[0].taskType);
    ASSERT_EQ("task2", task[0].taskName);

    ASSERT_EQ("fake", task[1].taskType);
    ASSERT_EQ("task3", task[1].taskName);

    ASSERT_EQ("fake", task[2].taskType);
    ASSERT_EQ("task1", task[2].taskName);
}

}} // namespace indexlibv2::table
