#include "suez/table/DecisionMakerChain.h"

#include "suez/common/test/TableMetaUtil.h"
#include "unittest/unittest.h"

namespace suez {

class DecisionMakerChainTest : public TESTBASE {};

TEST_F(DecisionMakerChainTest, testMakeDecision) {
    DecisionMakerChain maker;
    PartitionMeta current;
    auto target = TableMetaUtil::make(1, TS_UNKNOWN, DS_UNKNOWN, "path/to/index");
    DecisionMakerChain::Context ctx;
    ctx.isFinal = false;
    ctx.supportPreload = false;
    ctx.config = ScheduleConfig();

    current.setDeployStatus(1, DS_DEPLOYDONE);
    current.setTableStatus(TS_UNLOADED);
    auto op = maker.makeDecision(current, target, ctx);
    EXPECT_EQ(OP_LOAD, op);

    current.setDeployStatus(1, DS_DEPLOYING);
    op = maker.makeDecision(current, target, ctx);
    EXPECT_EQ(OP_HOLD, op);

    current.setDeployStatus(1, DS_UNKNOWN);
    op = maker.makeDecision(current, target, ctx);
    EXPECT_EQ(OP_DEPLOY, op);

    current.setTableStatus(TS_INITIALIZING);
    op = maker.makeDecision(current, target, ctx);
    EXPECT_EQ(OP_HOLD, op);

    current.setTableStatus(TS_UNKNOWN);
    op = maker.makeDecision(current, target, ctx);
    EXPECT_EQ(OP_INIT, op);
}

} // namespace suez
