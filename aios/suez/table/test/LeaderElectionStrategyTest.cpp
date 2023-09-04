#include "suez/table/LeaderElectionStrategy.h"

#include <memory>

#include "autil/EnvUtil.h"
#include "autil/MurmurHash.h"
#include "suez/common/test/TableMetaUtil.h"
#include "unittest/unittest.h"

using namespace testing;

namespace suez {

class LeaderElectionStrategyTest : public TESTBASE {};

TEST_F(LeaderElectionStrategyTest, testSimple) {
    auto pid1 = TableMetaUtil::makePid("t1", 1, 0, 32767);
    auto pid2 = TableMetaUtil::makePid("t1", 2, 0, 65535);
    auto pid3 = TableMetaUtil::makePid("t2", 1, 0, 65535);

    std::unique_ptr<LeaderElectionStrategy> strategy;
    strategy.reset(LeaderElectionStrategy::create("unknown"));
    ASSERT_FALSE(strategy);

    strategy.reset(LeaderElectionStrategy::create(""));
    ASSERT_TRUE(strategy);
    ASSERT_EQ("0_32767", strategy->getPath(pid1));
    ASSERT_EQ("0_65535", strategy->getPath(pid2));
    ASSERT_EQ("0_65535", strategy->getPath(pid3));

    strategy.reset(LeaderElectionStrategy::create("table"));
    ASSERT_TRUE(strategy);
    ASSERT_EQ("t1_1_0_32767", strategy->getPath(pid1));
    ASSERT_EQ("t1_2_0_65535", strategy->getPath(pid2));
    ASSERT_EQ("t2_1_0_65535", strategy->getPath(pid3));

    strategy.reset(LeaderElectionStrategy::create("table_ignore_version"));
    ASSERT_TRUE(strategy);
    ASSERT_EQ("t1_0_32767", strategy->getPath(pid1));
    ASSERT_EQ("t1_0_65535", strategy->getPath(pid2));
    ASSERT_EQ("t2_0_65535", strategy->getPath(pid3));

    strategy.reset(LeaderElectionStrategy::create("range"));
    ASSERT_TRUE(strategy);
    ASSERT_EQ("0_32767", strategy->getPath(pid1));
    ASSERT_EQ("0_65535", strategy->getPath(pid2));
    ASSERT_EQ("0_65535", strategy->getPath(pid3));
    ASSERT_EQ(32767, strategy->getKey(pid1));
    ASSERT_EQ(65535, strategy->getKey(pid2));
    ASSERT_EQ(65535, strategy->getKey(pid3));

    autil::EnvGuard env1("roleName", "role_name"); // 10 * 1024 * 1024

    strategy.reset(LeaderElectionStrategy::create("worker"));
    ASSERT_TRUE(strategy);
    ASSERT_EQ("role_name", strategy->getPath(pid1));
    ASSERT_EQ("role_name", strategy->getPath(pid2));
    ASSERT_EQ("role_name", strategy->getPath(pid3));

    ASSERT_EQ(0, strategy->getKey(pid1));
    ASSERT_EQ(0, strategy->getKey(pid2));
    ASSERT_EQ(0, strategy->getKey(pid3));
}

} // namespace suez
