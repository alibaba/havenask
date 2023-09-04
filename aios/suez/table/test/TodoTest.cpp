#include "suez/table/Todo.h"

#include "TableTestUtil.h"
#include "suez/common/test/TableMetaUtil.h"
#include "unittest/unittest.h"

namespace suez {

class TodoTest : public TESTBASE {
public:
    void setUp() override {
        auto pid = TableMetaUtil::makePid("t1");
        _table = TableTestUtil::make(pid);
    }

protected:
    std::shared_ptr<Table> _table;
};

TEST_F(TodoTest, testCreate) {
    std::unique_ptr<Todo> todo(Todo::create(OP_CANCELDEPLOY, _table));
    ASSERT_TRUE(todo.get() != nullptr);

    TargetPartitionMeta target;
    todo.reset(Todo::createWithTarget(OP_CANCELDEPLOY, _table, target));
    ASSERT_TRUE(todo.get() != nullptr);

    todo.reset(Todo::createWithTarget(OP_DEPLOY, _table, target));
    ASSERT_TRUE(todo.get() != nullptr);

    todo.reset(Todo::createWithTarget(OP_LOAD, _table, target));
    ASSERT_TRUE(todo.get() != nullptr);

    todo.reset(Todo::createWithTarget(OP_COMMIT, _table, target));
    ASSERT_TRUE(todo.get() == nullptr);
}

} // namespace suez
