#include "suez/table/TodoList.h"

#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>
#include <iosfwd>
#include <string>

#include "TableTestUtil.h"
#include "autil/legacy/legacy_jsonizable.h"
#include "suez/common/test/TableMetaUtil.h"
#include "unittest/unittest.h"

using namespace std;
using namespace testing;

namespace suez {

class TodoListTest : public TESTBASE {
public:
    void setUp();
    void tearDown();
};

void TodoListTest::setUp() {}

void TodoListTest::tearDown() {}

TEST_F(TodoListTest, testNeedCleanDisk) {
    TodoList todoList;
    EXPECT_EQ(false, todoList.needCleanDisk());

    auto pid = TableMetaUtil::makePid("table1");
    auto table = TableTestUtil::make(pid);
    todoList.addOperation(table, OP_CLEAN_DISK, TargetPartitionMeta());
    EXPECT_EQ(true, todoList.needCleanDisk());
}

TEST_F(TodoListTest, testNeedStopServiceLoadOperation) {
    TodoList todoList;
    ASSERT_FALSE(todoList.needStopService());

    auto pid = TableMetaUtil::makePid("table1");
    auto table = TableTestUtil::make(pid);
    auto target = TableMetaUtil::make(0);

    todoList.addOperation(table, OP_FORCELOAD, target);
    EXPECT_TRUE(todoList.needStopService());
    todoList._operations.clear();

    todoList.addOperation(table, OP_RELOAD, target);
    EXPECT_TRUE(todoList.needStopService());

    todoList._operations.clear();
    todoList.addOperation(table, OP_BECOME_LEADER, target);
    EXPECT_FALSE(todoList.needStopService());

    todoList._operations.clear();
    todoList.addOperation(table, OP_NO_LONGER_LEADER, target);
    EXPECT_FALSE(todoList.needStopService());
}

TEST_F(TodoListTest, testNeedStopServiceLackDisk) {
    TodoList todoList;

    auto pid = TableMetaUtil::makePid("table1");
    auto table = TableTestUtil::make(pid);
    auto target = TableMetaUtil::make(0);

    todoList.addOperation(table, OP_UNLOAD);
    EXPECT_FALSE(todoList.needStopService());

    todoList.addOperation(table, OP_CLEAN_DISK, target);
    EXPECT_FALSE(todoList.needStopService());
}

TEST_F(TodoListTest, testRemove) {
    TodoList todoList;
    auto pid = TableMetaUtil::makePid("table1", 1);
    auto table = TableTestUtil::make(pid);
    auto pid2 = TableMetaUtil::makePid("table2", 1);
    auto table2 = TableTestUtil::make(pid2);
    auto pid3 = TableMetaUtil::makePid("table3", 1);
    auto table3 = TableTestUtil::make(pid3);

    todoList.addOperation(table, OP_UNLOAD);
    todoList.addOperation(table2, OP_CANCELLOAD);
    todoList.addOperation(table3, OP_CANCELDEPLOY);

    ASSERT_FALSE(todoList.remove(OP_LOAD, pid));
    ASSERT_FALSE(todoList.remove(OP_LOAD, pid2));
    ASSERT_FALSE(todoList.remove(OP_LOAD, pid3));

    ASSERT_TRUE(todoList.remove(OP_UNLOAD, pid));
    ASSERT_TRUE(todoList.remove(OP_CANCELLOAD, pid2));
    ASSERT_TRUE(todoList.remove(OP_CANCELDEPLOY, pid3));

    ASSERT_TRUE(todoList.getOperations(pid).empty());
    ASSERT_TRUE(todoList.getOperations(pid2).empty());
    ASSERT_TRUE(todoList.getOperations(pid3).empty());
}

TEST_F(TodoListTest, testOptimize) {
    TodoList todoList;
    auto pid = TableMetaUtil::makePid("table1", 1);
    auto table = TableTestUtil::make(pid);
    auto target = TableMetaUtil::make(0);
    auto pid2 = TableMetaUtil::makePid("table2", 1);
    auto table2 = TableTestUtil::make(pid2);

    todoList.addOperation(table, OP_UNLOAD);
    todoList.addOperation(table2, OP_FORCELOAD, target);

    ASSERT_TRUE(todoList.hasOpType(OP_FORCELOAD));
    todoList.maybeOptimize();
    EXPECT_FALSE(todoList.hasOpType(OP_FORCELOAD));
}

TEST_F(TodoListTest, testGetRemoveTables) {
    TodoList todoList;
    auto pid = TableMetaUtil::makePid("table1", 1);
    auto table = TableTestUtil::make(pid);
    auto pid2 = TableMetaUtil::makePid("table2", 1);
    auto table2 = TableTestUtil::make(pid2);
    auto pid3 = TableMetaUtil::makePid("table3", 1);
    auto table3 = TableTestUtil::make(pid3);

    todoList.addOperation(table, OP_UNLOAD);
    todoList.addOperation(table2, OP_REMOVE);
    todoList.addOperation(table3, OP_REMOVE);

    auto removePids = todoList.getRemovedTables();
    EXPECT_EQ(2, removePids.size());
    EXPECT_EQ(1, removePids.count(pid2));
    EXPECT_EQ(1, removePids.count(pid3));
}

} // namespace suez
