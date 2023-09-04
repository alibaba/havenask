#include "suez/table/InitDecisionMaker.h"

#include "suez/common/test/TableMetaUtil.h"
#include "unittest/unittest.h"

using namespace std;
using namespace testing;

namespace suez {

class InitDecisionMakerTest : public TESTBASE {};

TEST_F(InitDecisionMakerTest, testMakeInitDecision) {
    InitDecisionMaker maker;
    PartitionMeta current;

    EXPECT_EQ(OP_INIT, maker.makeDecision(current, TargetPartitionMeta(), StageDecisionMaker::Context()));

    current.setTableStatus(TS_INITIALIZING);
    EXPECT_EQ(OP_HOLD, maker.makeDecision(current, TargetPartitionMeta(), StageDecisionMaker::Context()));

    auto otherTs = {TS_UNLOADED,
                    TS_LOADING,
                    TS_UNLOADING,
                    TS_FORCELOADING,
                    TS_LOADED,
                    TS_FORCE_RELOAD,
                    TS_PRELOADING,
                    TS_PRELOAD_FAILED,
                    TS_PRELOAD_FORCE_RELOAD,
                    TS_ERROR_LACK_MEM,
                    TS_ERROR_CONFIG,
                    TS_ERROR_UNKNOWN};

    for (auto ts : otherTs) {
        current.setTableStatus(ts);
        EXPECT_EQ(OP_NONE, maker.makeDecision(current, TargetPartitionMeta(), StageDecisionMaker::Context()));
    }
}

} // namespace suez
