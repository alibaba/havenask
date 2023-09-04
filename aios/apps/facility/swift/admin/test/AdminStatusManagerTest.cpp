#include "swift/admin/AdminStatusManager.h"

#include <unistd.h>

#include "autil/Lock.h"
#include "autil/TimeUtility.h"
#include "swift/admin/TopicTable.h"
#include "swift/protocol/Common.pb.h"
#include "unittest/unittest.h"

using namespace swift::protocol;
using namespace swift::monitor;

namespace swift {
namespace admin {

class AdminStatusManagerTest : public TESTBASE {};

TEST_F(AdminStatusManagerTest, testSimple) {
    AdminStatusManager adminStatusManager;
    ASSERT_FALSE(adminStatusManager.isMaster());
    ASSERT_FALSE(adminStatusManager.isLeader());

    adminStatusManager.setMaster(true);
    ASSERT_TRUE(adminStatusManager.isMaster());
    ASSERT_FALSE(adminStatusManager.isLeader());

    adminStatusManager.setLeader(true);
    ASSERT_TRUE(adminStatusManager.isMaster());
    ASSERT_TRUE(adminStatusManager.isLeader());
}

TEST_F(AdminStatusManagerTest, testTransition) {
    AdminStatusManager adminStatusManager;
    ASSERT_FALSE(adminStatusManager.isMaster());
    ASSERT_FALSE(adminStatusManager.isLeader());
    ASSERT_TRUE(adminStatusManager.transition());
}

TEST_F(AdminStatusManagerTest, testTransitionHandleError) {
    AdminStatusManager adminStatusManager;
    ASSERT_FALSE(adminStatusManager.isMaster());
    ASSERT_FALSE(adminStatusManager.isLeader());

    adminStatusManager.setLeader(true);
    adminStatusManager.setStatusUpdateHandler([](Status lastStatus, Status curStatus) -> bool { return false; });
    ASSERT_FALSE(adminStatusManager.transition());
    ASSERT_TRUE(adminStatusManager.isLeader());
    ASSERT_TRUE(adminStatusManager._currentStatus != adminStatusManager._lastStatus);
}

TEST_F(AdminStatusManagerTest, testTransitionHandleSucc) {
    AdminStatusManager adminStatusManager;
    ASSERT_FALSE(adminStatusManager.isMaster());
    ASSERT_FALSE(adminStatusManager.isLeader());

    adminStatusManager.setLeader(true);
    adminStatusManager.setStatusUpdateHandler([](Status lastStatus, Status curStatus) -> bool { return true; });
    ASSERT_TRUE(adminStatusManager.transition());
    ASSERT_TRUE(adminStatusManager.isLeader());
    ASSERT_TRUE(adminStatusManager._currentStatus == adminStatusManager._lastStatus);
}

TEST_F(AdminStatusManagerTest, testTransitionWait) {
    AdminStatusManager adminStatusManager;
    ASSERT_FALSE(adminStatusManager.isMaster());
    ASSERT_FALSE(adminStatusManager.isLeader());
    ASSERT_TRUE(adminStatusManager._stopped);
    adminStatusManager.start();
    ASSERT_FALSE(adminStatusManager._stopped);
    autil::Notifier notifier;
    adminStatusManager.setStatusUpdateHandler([&](Status lastStatus, Status curStatus) -> bool {
        notifier.notify();
        return true;
    });
    autil::StageTime stageTime;
    adminStatusManager.setLeader(true);
    notifier.wait();
    stageTime.end_stage();
    ASSERT_TRUE(stageTime.last_ms() * 1000 < AdminStatusManager::TRANSITION_WAIT_TIME);
    ASSERT_TRUE(adminStatusManager.isLeader());
    adminStatusManager.stop();
}

TEST_F(AdminStatusManagerTest, testTransitionMulti) {
    AdminStatusManager adminStatusManager;
    ASSERT_FALSE(adminStatusManager.isMaster());
    ASSERT_FALSE(adminStatusManager.isLeader());
    ASSERT_TRUE(adminStatusManager._stopped);
    adminStatusManager.start();
    ASSERT_FALSE(adminStatusManager._stopped);
    autil::Notifier notifier;
    adminStatusManager.setStatusUpdateHandler([&](Status lastStatus, Status curStatus) -> bool {
        notifier.wait();
        return true;
    });
    adminStatusManager.setLeader(true);
    ASSERT_TRUE(adminStatusManager.isLeader());
    usleep(1000 * 1000);
    adminStatusManager.setLeader(false);
    notifier.notify();
    usleep(1000 * 1000);
    ASSERT_FALSE(adminStatusManager.isLeader());
    ASSERT_EQ(LF_LEADER, adminStatusManager._lastStatus.lfStatus);
    notifier.notify();
    usleep(1000 * 1000);
    ASSERT_EQ(LF_FOLLOWER, adminStatusManager._lastStatus.lfStatus);
    adminStatusManager.stop();
}

} // namespace admin
} // namespace swift
