#include "suez/table/RemoveDecisionMaker.h"

#include "suez/common/test/TableMetaUtil.h"
#include "unittest/unittest.h"

namespace suez {

class RemoveDecisionMakerTest : public TESTBASE {};

TEST_F(RemoveDecisionMakerTest, testRemove) {
    RemoveDecisionMaker maker;
    PartitionMeta current;

    // still deploying
    current.setDeployStatus(1, DS_DEPLOYING);
    auto op = maker.makeDecision(current);
    ASSERT_EQ(OP_CANCELDEPLOY, op);

    // mark deploy canceled
    current.setDeployStatus(1, DS_CANCELLED);

    current.setTableStatus(TS_UNKNOWN);
    op = maker.makeDecision(current);
    ASSERT_EQ(OP_REMOVE, op);

    current.setTableStatus(TS_UNLOADED);
    op = maker.makeDecision(current);
    ASSERT_EQ(OP_REMOVE, op);

    current.setTableStatus(TS_LOADING);
    op = maker.makeDecision(current);
    ASSERT_EQ(OP_CANCELLOAD, op);

    current.setTableStatus(TS_FORCELOADING);
    op = maker.makeDecision(current);
    ASSERT_EQ(OP_CANCELLOAD, op);

    current.setTableStatus(TS_PRELOADING);
    op = maker.makeDecision(current);
    ASSERT_EQ(OP_CANCELLOAD, op);

    current.setTableStatus(TS_UNLOADING);
    op = maker.makeDecision(current);
    ASSERT_EQ(OP_HOLD, op);

    current.setTableStatus(TS_COMMITTING);
    op = maker.makeDecision(current);
    ASSERT_EQ(OP_HOLD, op);

    current.setTableStatus(TS_ROLE_SWITCHING);
    op = maker.makeDecision(current);
    ASSERT_EQ(OP_HOLD, op);

    current.setTableStatus(TS_COMMIT_ERROR);
    op = maker.makeDecision(current);
    ASSERT_EQ(OP_UNLOAD, op);
}

} // namespace suez
