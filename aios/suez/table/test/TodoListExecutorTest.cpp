#include "suez/table/TodoListExecutor.h"

#include <memory>

#include "unittest/unittest.h"

using namespace std;
using namespace testing;

namespace suez {

class TodoListExecutorTest : public TESTBASE {};

TEST_F(TodoListExecutorTest, testAddAndGetRunner) {
    TodoListExecutor executor;
    ASSERT_TRUE(executor.getDefaultRunner() != nullptr);

    ASSERT_EQ(executor.getDefaultRunner(), executor.getRunner(OP_INIT));
    ASSERT_EQ(executor.getDefaultRunner(), executor.getRunner(OP_LOAD));
    ASSERT_EQ(executor.getDefaultRunner(), executor.getRunner(OP_UPDATERT));

    auto runner = std::make_unique<SimpleRunner>();
    executor.addRunner(std::move(runner), {OP_INIT, OP_DEPLOY, OP_DIST_DEPLOY});
    ASSERT_NE(executor.getDefaultRunner(), executor.getRunner(OP_INIT));
    ASSERT_NE(executor.getDefaultRunner(), executor.getRunner(OP_DEPLOY));
    ASSERT_NE(executor.getDefaultRunner(), executor.getRunner(OP_DIST_DEPLOY));
}

} // namespace suez
