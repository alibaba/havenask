#include "suez/table/TodoRunner.h"

#include <atomic>
#include <memory>

#include "TableTestUtil.h"
#include "autil/ThreadPool.h"
#include "suez/common/test/TableMetaUtil.h"
#include "unittest/unittest.h"

using namespace std;
using namespace testing;

namespace suez {

class AsyncRunnerTest : public TESTBASE {
public:
    void setUp() override { startThreadPool(1); }

    void tearDown() override { stopThreadPool(); }

protected:
    void startThreadPool(uint32_t threadCount) {
        _tp = std::make_unique<autil::ThreadPool>(threadCount, threadCount);
        _tp->start("SuezLoad");
    }

    void stopThreadPool() {
        if (_tp) {
            _tp->stop();
            _tp.reset();
        }
    }

protected:
    std::unique_ptr<autil::ThreadPool> _tp;
};

class FakeTodo final : public TodoWithTarget {
public:
    using Task = std::function<void()>;

public:
    FakeTodo(OperationType op, const TablePtr &table, const TargetPartitionMeta &target, Task t)
        : TodoWithTarget(op, table, target), _task(std::move(t)) {}

public:
    void run() override { _task(); }

private:
    Task _task;
};

TEST_F(AsyncRunnerTest, testRun) {
    AsyncRunner runner(_tp.get());
    ASSERT_EQ(0, runner.ongoingOperationsCount());

    std::atomic<bool> flag{false};
    std::atomic<bool> finished{false};
    auto fn = [&flag, &finished]() {
        while (!flag) {
            usleep(1000);
        }
        finished.store(true);
    };

    auto table = TableTestUtil::make(TableMetaUtil::makePid("t1"));
    std::shared_ptr<Todo> todo(new FakeTodo(OP_LOAD, table, TargetPartitionMeta(), std::move(fn)));
    ASSERT_TRUE(runner.run(todo));
    ASSERT_EQ(1, runner.ongoingOperationsCount());

    ASSERT_FALSE(runner.run(todo));
    ASSERT_EQ(1, runner.ongoingOperationsCount());
    flag.store(true);

    while (!finished) {
        usleep(1000);
    }
    ASSERT_EQ(0, runner.ongoingOperationsCount());
}

TEST_F(AsyncRunnerTest, testRunConflictOperations) {
    stopThreadPool();
    startThreadPool(2);
    AsyncRunner runner(_tp.get());
    ASSERT_EQ(0, runner.ongoingOperationsCount());

    auto table = TableTestUtil::make(TableMetaUtil::makePid("t1"));
    std::atomic<bool> flag{false};
    std::atomic<bool> finished{false};
    auto fnLoad = [&flag, &finished]() {
        while (!flag) {
            usleep(1000);
        }
        finished.store(true);
    };
    std::shared_ptr<Todo> load(new FakeTodo(OP_LOAD, table, TargetPartitionMeta(), std::move(fnLoad)));
    ASSERT_TRUE(runner.run(load));
    ASSERT_EQ(1, runner.ongoingOperationsCount());

    std::atomic<bool> flag2{false};
    std::atomic<bool> finished2{false};
    auto fnBecomeLeader = [&flag2, &finished2]() {
        while (!flag2) {
            usleep(1000);
        }
        finished2.store(true);
    };
    std::shared_ptr<Todo> becomeLeader(
        new FakeTodo(OP_BECOME_LEADER, table, TargetPartitionMeta(), std::move(fnBecomeLeader)));
    ASSERT_FALSE(runner.run(becomeLeader));
    ASSERT_EQ(1, runner.ongoingOperationsCount());

    flag.store(true);
    while (!finished) {
        usleep(1000);
    }
    ASSERT_EQ(0, runner.ongoingOperationsCount());

    ASSERT_TRUE(runner.run(becomeLeader));
    ASSERT_EQ(1, runner.ongoingOperationsCount());

    flag2.store(true);
    while (!finished2) {
        usleep(1000);
    }
    ASSERT_EQ(0, runner.ongoingOperationsCount());
}

} // namespace suez
