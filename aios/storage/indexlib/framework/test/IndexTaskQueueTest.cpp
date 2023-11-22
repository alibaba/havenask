#include "indexlib/framework/IndexTaskQueue.h"

#include <thread>

#include "indexlib/framework/index_task/Constant.h"
#include "unittest/unittest.h"

namespace indexlibv2::framework {

class IndexTaskQueueTest : public TESTBASE
{
private:
    IndexTaskMetaCreator _creator;
};

TEST_F(IndexTaskQueueTest, testJoin)
{
    int64_t currentTs = autil::TimeUtility::currentTimeInSeconds();

    // task0 : ready task
    auto meta0 = _creator.TaskType(BULKLOAD_TASK_TYPE).TaskTraceId("bulkload_task0").Create();

    // task1 : done task, exceeds ttl, is supposed to be reclaimed
    auto meta1 = _creator.TaskType(BULKLOAD_TASK_TYPE)
                     .TaskTraceId("bulkload_task1")
                     .State(IndexTaskMeta::DONE)
                     .BeginTimeInSecs(0)
                     .EndTimeInSecs(currentTs - indexlib::DEFAULT_DONE_TASK_TTL_IN_SECONDS)
                     .Create();

    // task2 : done task, not exceeds ttl
    auto meta2 = _creator.TaskType(BULKLOAD_TASK_TYPE)
                     .TaskTraceId("bulkload_task2")
                     .State(IndexTaskMeta::DONE)
                     .BeginTimeInSecs(0)
                     .EndTimeInSecs(currentTs - indexlib::DEFAULT_DONE_TASK_TTL_IN_SECONDS + 1)
                     .Create();

    // task3 : aborted task, exceeds ttl, is supposed to be reclaimed
    auto meta3 = _creator.TaskType(BULKLOAD_TASK_TYPE)
                     .TaskTraceId("bulkload_task3")
                     .State(IndexTaskMeta::ABORTED)
                     .BeginTimeInSecs(0)
                     .EndTimeInSecs(currentTs - indexlib::DEFAULT_DONE_TASK_TTL_IN_SECONDS)
                     .Create();

    // task4 : aborted task, not exceeds ttl
    auto meta4 = _creator.TaskType(BULKLOAD_TASK_TYPE)
                     .TaskTraceId("bulkload_task4")
                     .State(IndexTaskMeta::ABORTED)
                     .BeginTimeInSecs(0)
                     .EndTimeInSecs(currentTs - indexlib::DEFAULT_DONE_TASK_TTL_IN_SECONDS + 1)
                     .Create();

    IndexTaskQueue queue;
    queue.Add(meta1);
    queue.Add(meta2);
    queue.Add(meta3);
    queue.Add(meta4);

    // old task1/task2/task3/task4 : ready task
    auto oldMeta1 = _creator.TaskType(BULKLOAD_TASK_TYPE).TaskTraceId("bulkload_task1").Create();
    auto oldMeta2 = _creator.TaskType(BULKLOAD_TASK_TYPE).TaskTraceId("bulkload_task2").Create();
    auto oldMeta3 = _creator.TaskType(BULKLOAD_TASK_TYPE).TaskTraceId("bulkload_task3").Create();
    auto oldMeta4 = _creator.TaskType(BULKLOAD_TASK_TYPE).TaskTraceId("bulkload_task4").Create();

    auto queue2 = std::make_shared<IndexTaskQueue>();
    queue2->Add(meta0);
    queue2->Add(oldMeta1);
    queue2->Add(oldMeta2);
    queue2->Add(oldMeta3);
    queue2->Add(oldMeta4);

    queue.Join(*queue2);

    ASSERT_EQ(queue.Size(), 3);
    auto indexTasks = queue.GetAllTasks();
    ASSERT_EQ(indexTasks[0]->GetTaskTraceId(), "bulkload_task0");
    ASSERT_EQ(indexTasks[0]->GetState(), IndexTaskMeta::PENDING);
    ASSERT_EQ(indexTasks[1]->GetTaskTraceId(), "bulkload_task2");
    ASSERT_EQ(indexTasks[1]->GetState(), IndexTaskMeta::DONE);
    ASSERT_EQ(indexTasks[2]->GetTaskTraceId(), "bulkload_task4");
    ASSERT_EQ(indexTasks[2]->GetState(), IndexTaskMeta::ABORTED);
}

TEST_F(IndexTaskQueueTest, TestIndexTaskStateTransfer)
{
    IndexTaskMeta meta;
    ASSERT_TRUE(meta.ValidateStateTransfer(IndexTaskMeta::DONE, IndexTaskMeta::DONE));
    ASSERT_FALSE(meta.ValidateStateTransfer(IndexTaskMeta::DONE, IndexTaskMeta::PENDING));
    ASSERT_FALSE(meta.ValidateStateTransfer(IndexTaskMeta::DONE, IndexTaskMeta::SUSPENDED));
    ASSERT_FALSE(meta.ValidateStateTransfer(IndexTaskMeta::DONE, IndexTaskMeta::ABORTED));

    ASSERT_TRUE(meta.ValidateStateTransfer(IndexTaskMeta::PENDING, IndexTaskMeta::PENDING));
    ASSERT_TRUE(meta.ValidateStateTransfer(IndexTaskMeta::PENDING, IndexTaskMeta::SUSPENDED));
    ASSERT_TRUE(meta.ValidateStateTransfer(IndexTaskMeta::PENDING, IndexTaskMeta::ABORTED));
    ASSERT_TRUE(meta.ValidateStateTransfer(IndexTaskMeta::PENDING, IndexTaskMeta::DONE));

    ASSERT_TRUE(meta.ValidateStateTransfer(IndexTaskMeta::SUSPENDED, IndexTaskMeta::SUSPENDED));
    ASSERT_TRUE(meta.ValidateStateTransfer(IndexTaskMeta::SUSPENDED, IndexTaskMeta::PENDING));
    ASSERT_TRUE(meta.ValidateStateTransfer(IndexTaskMeta::SUSPENDED, IndexTaskMeta::ABORTED));
    ASSERT_FALSE(meta.ValidateStateTransfer(IndexTaskMeta::SUSPENDED, IndexTaskMeta::DONE));

    ASSERT_TRUE(meta.ValidateStateTransfer(IndexTaskMeta::ABORTED, IndexTaskMeta::ABORTED));
    ASSERT_FALSE(meta.ValidateStateTransfer(IndexTaskMeta::ABORTED, IndexTaskMeta::PENDING));
    ASSERT_FALSE(meta.ValidateStateTransfer(IndexTaskMeta::ABORTED, IndexTaskMeta::SUSPENDED));
    ASSERT_FALSE(meta.ValidateStateTransfer(IndexTaskMeta::ABORTED, IndexTaskMeta::DONE));
}

TEST_F(IndexTaskQueueTest, TestUpdateState)
{
    std::string taskType1 = "type1";
    std::string taskName1 = "name1";
    std::map<std::string, std::string> params1;
    params1["key1"] = "value1";

    IndexTaskQueue queue;

    auto task = queue.Get(taskType1, taskName1);
    ASSERT_FALSE(task);

    auto meta = _creator.TaskType(taskType1).TaskTraceId(taskName1).Params(params1).Create();
    queue.Add(meta);

    task = queue.Get(taskType1, taskName1);
    ASSERT_TRUE(task);
    ASSERT_EQ(task->GetState(), IndexTaskMeta::PENDING);

    task->UpdateState(IndexTaskMeta::DONE);
    ASSERT_EQ(task->GetState(), IndexTaskMeta::DONE);
    ASSERT_TRUE(task->GetParams().empty());
    ASSERT_NE(task->GetEndTimeInSecs(), indexlib::INVALID_TIMESTAMP);
}

TEST_F(IndexTaskQueueTest, TestOverwrite)
{
    std::string taskType1 = "type1";
    std::string taskName1 = "name1";
    std::map<std::string, std::string> params1;
    params1["key1"] = "value1";
    std::map<std::string, std::string> params2;
    params2["key2"] = "value2";
    params2[PARAM_LAST_SEQUENCE_NUMBER] = "123";

    auto meta = _creator.TaskType(taskType1).TaskTraceId(taskName1).Params(params1).Create();

    IndexTaskQueue queue;
    queue.Overwrite(meta);
    auto task = queue.Get(taskType1, taskName1);
    ASSERT_FALSE(task);

    queue.Add(meta);
    task = queue.Get(taskType1, taskName1);
    ASSERT_TRUE(task);
    ASSERT_EQ(task->GetState(), IndexTaskMeta::PENDING);
    auto firstBeginTime = task->GetBeginTimeInSecs();
    ASSERT_NE(firstBeginTime, indexlib::INVALID_TIMESTAMP);

    std::this_thread::sleep_for(std::chrono::milliseconds(2000));

    meta = _creator.TaskType(taskType1).TaskTraceId(taskName1).Params(params2).Create();
    ASSERT_TRUE(queue.Overwrite(meta));
    task = queue.Get(taskType1, taskName1);
    ASSERT_TRUE(task);
    ASSERT_EQ(task->GetState(), IndexTaskMeta::PENDING);
    auto secondBeginTime = task->GetBeginTimeInSecs();
    ASSERT_NE(secondBeginTime, indexlib::INVALID_TIMESTAMP);
    ASSERT_NE(firstBeginTime, secondBeginTime);
    ASSERT_EQ(task->GetParams(), params2);
}

TEST_F(IndexTaskQueueTest, TestAdd)
{
    std::string taskType1 = "type1";
    std::string taskName1 = "name1";
    std::map<std::string, std::string> params1;
    params1["key1"] = "value1";
    std::map<std::string, std::string> params2;
    params2["key2"] = "value2";

    auto meta = _creator.TaskType(taskType1).TaskTraceId(taskName1).Params(params1).Create();

    IndexTaskQueue queue;
    queue.Add(meta);
    auto task = queue.Get(taskType1, taskName1);
    ASSERT_TRUE(task);
    ASSERT_EQ(task->GetState(), IndexTaskMeta::PENDING);
    ASSERT_EQ(task->GetParams(), params1);

    meta = _creator.TaskType(taskType1).TaskTraceId(taskName1).Params(params2).Create();
    queue.Add(meta);
    task = queue.Get(taskType1, taskName1);
    ASSERT_TRUE(task);
    ASSERT_EQ(task->GetState(), IndexTaskMeta::PENDING);
    ASSERT_EQ(task->GetParams(), params1);
}

TEST_F(IndexTaskQueueTest, TestSimpleIndexTasks)
{
    // task to be done
    std::string taskType1 = "type1";
    std::string taskName1 = "name1";
    std::map<std::string, std::string> params1;
    params1["key1"] = "value1";
    // task to be overwritten
    std::string taskType2 = "type2";
    std::string taskName2 = "name2";
    std::map<std::string, std::string> params2;
    params2["key2"] = "wrong value";
    std::string comment2 = "invalid value";
    // task to be abort
    std::string taskType3 = "type3";
    std::string taskName3 = "name3";
    std::map<std::string, std::string> params3;
    params3["key3"] = "value3";

    auto meta1 = _creator.TaskType(taskType1).TaskTraceId(taskName1).Params(params1).Create();

    auto meta2 = _creator.TaskType(taskType2)
                     .TaskTraceId(taskName2)
                     .Params(params2)
                     .State(IndexTaskMeta::SUSPENDED)
                     .Comment(comment2)
                     .Create();
    auto meta3 = _creator.TaskType(taskType3).TaskTraceId(taskName3).Params(params3).Create();
    IndexTaskQueue queue;
    queue.Add(meta1);
    queue.Add(meta2);
    queue.Add(meta3);

    auto tasks = queue.GetAllTasks();
    ASSERT_EQ(tasks.size(), 3u);
    ASSERT_EQ(tasks[0]->GetTaskType(), "type1");
    ASSERT_EQ(tasks[0]->GetTaskTraceId(), "name1");
    ASSERT_EQ(tasks[0]->GetState(), IndexTaskMeta::PENDING);
    ASSERT_EQ(tasks[0]->GetParams(), params1);

    ASSERT_EQ(tasks[1]->GetTaskType(), "type2");
    ASSERT_EQ(tasks[1]->GetTaskTraceId(), "name2");
    ASSERT_EQ(tasks[1]->GetState(), IndexTaskMeta::SUSPENDED);
    ASSERT_EQ(tasks[1]->GetParams(), params2);
    ASSERT_EQ(tasks[1]->GetComment(), comment2);

    ASSERT_EQ(tasks[2]->GetTaskType(), "type3");
    ASSERT_EQ(tasks[2]->GetTaskTraceId(), "name3");
    ASSERT_EQ(tasks[2]->GetState(), IndexTaskMeta::PENDING);
    ASSERT_EQ(tasks[2]->GetParams(), params3);
    // update state of task1
    ASSERT_TRUE(queue.Done("type1", "name1"));
    // overwrite task2
    params2["key2"] = "value2";
    meta2 = _creator.TaskType(taskType2).TaskTraceId(taskName2).Params(params2).Create();
    ASSERT_TRUE(queue.Overwrite(meta2));
    // cancel task 3
    ASSERT_TRUE(queue.Abort("type3", "name3"));
    // check task status
    auto indexTask1 = queue.Get("type1", "name1");
    ASSERT_TRUE(indexTask1);
    ASSERT_EQ(indexTask1->GetState(), IndexTaskMeta::DONE);

    auto indexTask2 = queue.Get("type2", "name2");
    ASSERT_TRUE(indexTask2);
    ASSERT_EQ(indexTask2->GetState(), IndexTaskMeta::PENDING);
    ASSERT_EQ(indexTask2->GetParams(), params2);
    ASSERT_EQ(indexTask2->GetComment(), "");

    auto indexTask3 = queue.Get("type3", "name3");
    ASSERT_TRUE(indexTask3);
    ASSERT_EQ(indexTask3->GetState(), IndexTaskMeta::ABORTED);
}

} // namespace indexlibv2::framework
