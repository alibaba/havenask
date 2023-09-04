#include "suez/table/LeaderElectionManager.h"

#include "autil/EnvUtil.h"
#include "suez/table/DefaultLeaderElectionManager.h"
#include "suez/table/ZkLeaderElectionManager.h"
#include "unittest/unittest.h"

using namespace testing;
using namespace autil;

namespace suez {

class LeaderElectionManagerTest : public TESTBASE {};

TEST_F(LeaderElectionManagerTest, testCreate) {
    {
        std::unique_ptr<LeaderElectionManager> mgr(LeaderElectionManager::create(""));
        ASSERT_FALSE(mgr);

        EnvGuard guard("zk_root", "zfs://path/to/some/zk");
        mgr.reset(LeaderElectionManager::create(""));
        ASSERT_TRUE(mgr);
        ZkLeaderElectionManager *zkMgr = dynamic_cast<ZkLeaderElectionManager *>(mgr.get());
        ASSERT_TRUE(zkMgr != nullptr);
        ASSERT_EQ("zfs://path/to/some/zk", zkMgr->_config.zkRoot);
        ASSERT_EQ("zfs://path/to/some/zk/leader_election", zkMgr->_leaderElectionRoot);
        ASSERT_EQ("zfs://path/to/some/zk/leader_info", zkMgr->_leaderInfoRoot);

        mgr.reset(LeaderElectionManager::create("searcher"));
        ASSERT_TRUE(mgr);
        zkMgr = dynamic_cast<ZkLeaderElectionManager *>(mgr.get());
        ASSERT_TRUE(zkMgr != nullptr);
        ASSERT_EQ("zfs://path/to/some/zk/searcher", zkMgr->_config.zkRoot);
        ASSERT_EQ("zfs://path/to/some/zk/searcher/leader_election", zkMgr->_leaderElectionRoot);
        ASSERT_EQ("zfs://path/to/some/zk/searcher/leader_info", zkMgr->_leaderInfoRoot);
    }

    {
        EnvGuard guard("zk_root", "LOCAL");
        std::unique_ptr<LeaderElectionManager> mgr(LeaderElectionManager::create("searcher"));
        ASSERT_TRUE(mgr.get() != nullptr);
        DefaultLeaderElectionManager *defaultMgr = dynamic_cast<DefaultLeaderElectionManager *>(mgr.get());
        ASSERT_TRUE(defaultMgr != nullptr);
    }
}

} // namespace suez
