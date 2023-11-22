#include "indexlib/framework/index_task/LocalExecuteEngine.h"

#include <string>

#include "future_lite/executors/SimpleExecutor.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/FileSystemCreator.h"
#include "indexlib/file_system/FileSystemOptions.h"
#include "indexlib/file_system/LogicalFileSystem.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/framework/index_task/IIndexOperationCreator.h"
#include "indexlib/framework/index_task/IIndexTaskResourceCreator.h"
#include "indexlib/framework/index_task/IndexOperation.h"
#include "indexlib/framework/index_task/IndexOperationDescription.h"
#include "indexlib/framework/index_task/IndexTaskContext.h"
#include "indexlib/framework/index_task/IndexTaskPlan.h"
#include "indexlib/framework/index_task/IndexTaskResource.h"
#include "indexlib/framework/index_task/IndexTaskResourceManager.h"
#include "indexlib/framework/index_task/test/FakeResource.h"
#include "indexlib/framework/index_task/testlib/FakeTask.h"
#include "indexlib/util/PathUtil.h"
#include "unittest/unittest.h"

namespace indexlibv2::framework {

class LocalExecuteEngineTest : public TESTBASE
{
public:
    LocalExecuteEngineTest() {}
    ~LocalExecuteEngineTest() {}

    void setUp() override {}
    void tearDown() override {}
};

namespace {

class SetOperation : public IndexOperation
{
public:
    SetOperation() : IndexOperation(INVALID_INDEX_OPERATION_ID, true) {}
    Status Execute(const IndexTaskContext& context) override
    {
        const auto& resourceManager = context.GetResourceManager();
        std::shared_ptr<IndexTaskResource> resource;
        auto status = resourceManager->CreateResource(/*name*/ "hello", /*type*/ "vector", resource);
        if (!status.IsOK()) {
            return status;
        }
        auto vectorResource = dynamic_cast<FakeVectorResource*>(resource.get());
        assert(vectorResource);
        vectorResource->SetData({1, 2, 3});
        status = resourceManager->CommitResource("hello");
        if (!status.IsOK()) {
            return status;
        }
        status = resourceManager->ReleaseResource("hello");
        return status;
    }
};

class GetOperation : public IndexOperation
{
public:
    GetOperation() : IndexOperation(INVALID_INDEX_OPERATION_ID, true) {}
    Status Execute(const IndexTaskContext& context) override
    {
        const auto& resourceManager = context.GetResourceManager();
        auto [status, resource] = resourceManager->LoadResource("hello", "vector");
        if (!status.IsOK()) {
            return status;
        }
        auto vectorResource = dynamic_cast<FakeVectorResource*>(resource.get());
        assert(vectorResource);
        auto data = vectorResource->GetData();
        std::vector<int> expectedData = {1, 2, 3};
        if (expectedData != data) {
            return Status::Corruption();
        }
        return Status::OK();
    }
};

class GetSetOperationCreator : public IIndexOperationCreator
{
public:
    GetSetOperationCreator() {}
    std::unique_ptr<IndexOperation> CreateOperation(const IndexOperationDescription& opDesc) override
    {
        if (opDesc.GetType() == "get") {
            return std::make_unique<GetOperation>();
        } else if (opDesc.GetType() == "set") {
            return std::make_unique<SetOperation>();
        } else {
            return nullptr;
        }
    }
};

} // namespace

TEST_F(LocalExecuteEngineTest, testSimpleExecute)
{
    auto testRoot = GET_TEMP_DATA_PATH();
    auto fs = indexlib::file_system::FileSystemCreator::Create("test", testRoot).GetOrThrow();
    auto resourceDir = indexlib::file_system::Directory::Get(fs);
    auto resourceCreator = std::make_unique<FakeResourceCreator>();
    auto manager = std::make_shared<IndexTaskResourceManager>();
    ASSERT_TRUE(manager->Init(resourceDir, "work", std::move(resourceCreator)).IsOK());
    IndexTaskContext context;
    context.TEST_SetFenceRoot(resourceDir);
    context._resourceManager = manager;
    auto opCreator = std::make_unique<GetSetOperationCreator>();
    future_lite::executors::SimpleExecutor executor(2);
    LocalExecuteEngine engine(&executor, std::move(opCreator));
    IndexOperationDescription op0(/*id*/ 0, /*type*/ "set");
    IndexOperationDescription op1(/*id*/ 1, /*type*/ "get");
    op1.AddDepend(0);

    auto task = [&engine, &context, op0, op1]() -> future_lite::coro::Lazy<Status> {
        auto status = co_await engine.Schedule(op0, &context);
        EXPECT_TRUE(status.IsOK());
        status = co_await engine.Schedule(op1, &context);
        EXPECT_TRUE(status.IsOK());
        co_return status;
    };
    auto status = future_lite::coro::syncAwait(task());
    ASSERT_TRUE(status.IsOK());

    // TODO: Check All Ops executed
}

namespace {

auto getIdVector(const std::vector<LocalExecuteEngine::NodeDef*> nodes)
{
    std::vector<int64_t> vec;
    for (auto node : nodes) {
        vec.push_back((node->desc).GetId());
    }
    return vec;
};

} // namespace

TEST_F(LocalExecuteEngineTest, testTopoStages)
{
    auto testRoot = GET_TEMP_DATA_PATH();
    auto fs = indexlib::file_system::FileSystemCreator::Create("test", testRoot).GetOrThrow();
    auto resourceDir = indexlib::file_system::Directory::Get(fs);
    testlib::FakeSharedState st;
    auto opCreator = std::make_unique<testlib::FakeOperationCreator>(&st);
    future_lite::executors::SimpleExecutor executor(2);
    LocalExecuteEngine engine(&executor, std::move(opCreator));
    IndexOperationDescription op0(/*id*/ 0, /*type*/ "");
    IndexOperationDescription op1(/*id*/ 1, /*type*/ "");
    IndexOperationDescription op2(/*id*/ 2, /*type*/ "");
    IndexOperationDescription op3(/*id*/ 3, /*type*/ "");
    op1.AddDepend(0);
    op2.AddDepend(0);
    op3.AddDepend(1);
    op3.AddDepend(2);
    IndexTaskPlan plan("test", "test");
    plan._opDescs = std::vector<IndexOperationDescription> {op3, op2, op1, op0};

    auto nodeMap = engine.InitNodeMap(plan);
    auto stages = LocalExecuteEngine::ComputeTopoStages(nodeMap);

    ASSERT_EQ(3u, stages.size());
    auto iter = stages.begin();

    EXPECT_THAT(getIdVector(*iter), testing::UnorderedElementsAre(0));
    ++iter;
    EXPECT_THAT(getIdVector(*iter), testing::UnorderedElementsAre(1, 2));
    ++iter;
    EXPECT_THAT(getIdVector(*iter), testing::UnorderedElementsAre(3));
}

TEST_F(LocalExecuteEngineTest, testCyclic)
{
    auto testRoot = GET_TEMP_DATA_PATH();
    auto fs = indexlib::file_system::FileSystemCreator::Create("test", testRoot).GetOrThrow();
    auto resourceDir = indexlib::file_system::Directory::Get(fs);
    testlib::FakeSharedState st;
    auto opCreator = std::make_unique<testlib::FakeOperationCreator>(&st);
    future_lite::executors::SimpleExecutor executor(2);
    LocalExecuteEngine engine(&executor, std::move(opCreator));
    IndexOperationDescription op0(/*id*/ 0, /*type*/ "");
    IndexOperationDescription op1(/*id*/ 1, /*type*/ "");
    IndexOperationDescription op2(/*id*/ 2, /*type*/ "");
    IndexOperationDescription op3(/*id*/ 3, /*type*/ "");
    op1.AddDepend(0);
    op1.AddDepend(3);
    op2.AddDepend(0);
    op2.AddDepend(1);
    op3.AddDepend(1);
    op3.AddDepend(2);
    IndexTaskPlan plan("", "");
    plan._opDescs = std::vector<IndexOperationDescription> {op3, op2, op1, op0};

    auto nodeMap = engine.InitNodeMap(plan);
    auto stages = LocalExecuteEngine::ComputeTopoStages(nodeMap);
    ASSERT_EQ(0u, stages.size());
}

TEST_F(LocalExecuteEngineTest, testWholeCyclic)
{
    auto testRoot = GET_TEMP_DATA_PATH();
    auto fs = indexlib::file_system::FileSystemCreator::Create("test", testRoot).GetOrThrow();
    auto resourceDir = indexlib::file_system::Directory::Get(fs);
    testlib::FakeSharedState st;
    auto opCreator = std::make_unique<testlib::FakeOperationCreator>(&st);
    future_lite::executors::SimpleExecutor executor(2);
    LocalExecuteEngine engine(&executor, std::move(opCreator));
    IndexOperationDescription op0(/*id*/ 0, /*type*/ "");
    IndexOperationDescription op1(/*id*/ 1, /*type*/ "");
    IndexOperationDescription op2(/*id*/ 2, /*type*/ "");
    op0.AddDepend(2);
    op1.AddDepend(0);
    op2.AddDepend(1);
    IndexTaskPlan plan("", "");
    plan._opDescs = std::vector<IndexOperationDescription> {op2, op1, op0};
    auto nodeMap = engine.InitNodeMap(plan);
    auto stages = LocalExecuteEngine::ComputeTopoStages(nodeMap);
    ASSERT_EQ(0u, stages.size());
}
TEST_F(LocalExecuteEngineTest, testEmptyTaskPlan)
{
    auto testRoot = GET_TEMP_DATA_PATH();
    auto fs = indexlib::file_system::FileSystemCreator::Create("test", testRoot).GetOrThrow();
    auto resourceDir = indexlib::file_system::Directory::Get(fs);
    testlib::FakeSharedState st;
    auto opCreator = std::make_unique<testlib::FakeOperationCreator>(&st);
    future_lite::executors::SimpleExecutor executor(2);
    LocalExecuteEngine engine(&executor, std::move(opCreator));
    IndexTaskPlan plan("", "");
    plan._opDescs = std::vector<IndexOperationDescription> {};
    auto nodeMap = engine.InitNodeMap(plan);
    ASSERT_EQ(0u, nodeMap.size());
    auto stages = LocalExecuteEngine::ComputeTopoStages(nodeMap);
    ASSERT_EQ(0u, stages.size());
}
TEST_F(LocalExecuteEngineTest, testWrongDepend)
{
    auto testRoot = GET_TEMP_DATA_PATH();
    auto fs = indexlib::file_system::FileSystemCreator::Create("test", testRoot).GetOrThrow();
    auto resourceDir = indexlib::file_system::Directory::Get(fs);
    testlib::FakeSharedState st;
    auto opCreator = std::make_unique<testlib::FakeOperationCreator>(&st);
    future_lite::executors::SimpleExecutor executor(2);
    LocalExecuteEngine engine(&executor, std::move(opCreator));
    IndexOperationDescription op0(/*id*/ 0, /*type*/ "");
    IndexOperationDescription op1(/*id*/ 1, /*type*/ "");
    IndexOperationDescription op2(/*id*/ 2, /*type*/ "");
    IndexOperationDescription op3(/*id*/ 3, /*type*/ "");
    op1.AddDepend(0);
    op1.AddDepend(1);
    op2.AddDepend(0);
    op3.AddDepend(100);
    IndexTaskPlan plan("", "");
    plan._opDescs = std::vector<IndexOperationDescription> {op3, op2, op1, op0};
    auto nodeMap = engine.InitNodeMap(plan);
    auto stages = LocalExecuteEngine::ComputeTopoStages(nodeMap);
    ASSERT_EQ(0u, stages.size());
}
TEST_F(LocalExecuteEngineTest, testSelfDepend)
{
    auto testRoot = GET_TEMP_DATA_PATH();
    auto fs = indexlib::file_system::FileSystemCreator::Create("test", testRoot).GetOrThrow();
    auto resourceDir = indexlib::file_system::Directory::Get(fs);
    testlib::FakeSharedState st;
    auto opCreator = std::make_unique<testlib::FakeOperationCreator>(&st);
    future_lite::executors::SimpleExecutor executor(2);
    LocalExecuteEngine engine(&executor, std::move(opCreator));
    IndexOperationDescription op0(/*id*/ 0, /*type*/ "");
    IndexOperationDescription op1(/*id*/ 1, /*type*/ "");
    IndexOperationDescription op2(/*id*/ 2, /*type*/ "");
    IndexOperationDescription op3(/*id*/ 3, /*type*/ "");
    op1.AddDepend(0);
    op1.AddDepend(1);
    op2.AddDepend(0);
    op3.AddDepend(1);
    op3.AddDepend(2);
    IndexTaskPlan plan("", "");
    plan._opDescs = std::vector<IndexOperationDescription> {op3, op2, op1, op0};
    auto nodeMap = engine.InitNodeMap(plan);
    auto stages = LocalExecuteEngine::ComputeTopoStages(nodeMap);
    ASSERT_EQ(0u, stages.size());
}
TEST_F(LocalExecuteEngineTest, testDisconnectedTask)
{
    auto testRoot = GET_TEMP_DATA_PATH();
    auto fs = indexlib::file_system::FileSystemCreator::Create("test", testRoot).GetOrThrow();
    auto resourceDir = indexlib::file_system::Directory::Get(fs);
    testlib::FakeSharedState st;
    auto opCreator = std::make_unique<testlib::FakeOperationCreator>(&st);
    future_lite::executors::SimpleExecutor executor(2);
    LocalExecuteEngine engine(&executor, std::move(opCreator));
    IndexOperationDescription op0(/*id*/ 0, /*type*/ "");
    IndexOperationDescription op1(/*id*/ 1, /*type*/ "");
    IndexOperationDescription op2(/*id*/ 2, /*type*/ "");
    IndexOperationDescription op3(/*id*/ 3, /*type*/ "");
    IndexOperationDescription op4(/*id*/ 4, /*type*/ "");
    IndexOperationDescription op5(/*id*/ 5, /*type*/ "");
    op1.AddDepend(0);
    op2.AddDepend(0);
    op3.AddDepend(1);
    op3.AddDepend(2);
    op5.AddDepend(4);
    IndexTaskPlan plan("", "");
    plan._opDescs = std::vector<IndexOperationDescription> {op5, op4, op3, op2, op1, op0};
    auto nodeMap = engine.InitNodeMap(plan);
    auto stages = LocalExecuteEngine::ComputeTopoStages(nodeMap);
    ASSERT_EQ(3u, stages.size());
    auto iter = stages.begin();
    EXPECT_THAT(getIdVector(*iter), testing::UnorderedElementsAre(0, 4));
    ++iter;
    EXPECT_THAT(getIdVector(*iter), testing::UnorderedElementsAre(1, 2, 5));
    ++iter;
    EXPECT_THAT(getIdVector(*iter), testing::UnorderedElementsAre(3));
}

TEST_F(LocalExecuteEngineTest, testScheduleTask)
{
    auto testRoot = GET_TEMP_DATA_PATH();
    auto fs = indexlib::file_system::FileSystemCreator::Create("test", testRoot).GetOrThrow();
    auto resourceDir = indexlib::file_system::Directory::Get(fs);
    testlib::FakeSharedState st;
    auto opCreator = std::make_unique<testlib::FakeOperationCreator>(&st);
    future_lite::executors::SimpleExecutor executor(2);
    IndexTaskContext context;
    context.TEST_SetFenceRoot(resourceDir);
    LocalExecuteEngine engine(&executor, std::move(opCreator));
    IndexOperationDescription op0(/*id*/ 0, /*type*/ "");
    IndexOperationDescription op1(/*id*/ 1, /*type*/ "");
    IndexOperationDescription op2(/*id*/ 2, /*type*/ "");
    IndexOperationDescription op3(/*id*/ 3, /*type*/ "");
    op1.AddDepend(0);
    op2.AddDepend(0);
    op3.AddDepend(1);
    op3.AddDepend(2);
    IndexTaskPlan plan("", "");
    plan._opDescs = std::vector<IndexOperationDescription> {op3, op2, op1, op0};
    auto task = [&engine, &context, plan]() -> future_lite::coro::Lazy<Status> {
        co_return co_await engine.ScheduleTask(plan, &context);
    };
    auto status = future_lite::coro::syncAwait(task());
    ASSERT_TRUE(status.IsOK());
    ASSERT_EQ(4u, st.executeIds.size());
    ASSERT_EQ(0, st.executeIds[0]);
    ASSERT_EQ(3, st.executeIds[3]);
    ASSERT_TRUE((st.executeIds[1] == 1 && st.executeIds[2] == 2) || (st.executeIds[1] == 2 && st.executeIds[2] == 1));
}

} // namespace indexlibv2::framework
