#include "swift/admin/WorkerManager.h"

#include <cstddef>
#include <deque>
#include <stdint.h>
#include <string>

#include "autil/StringUtil.h"
#include "autil/TimeUtility.h"
#include "swift/admin/AppPlanMaker.h"
#include "swift/protocol/Common.pb.h"
#include "unittest/unittest.h"

using namespace std;
using namespace testing;
using namespace autil;
using namespace swift::protocol;

namespace swift {
namespace admin {

class WorkerManagerTest : public TESTBASE {
public:
    void setUp();
    void tearDown();
};

void WorkerManagerTest::setUp() {}

void WorkerManagerTest::tearDown() {}

TEST_F(WorkerManagerTest, testGetAppPlanMaker) {
    WorkerManager manager;
    for (size_t i = 0; i < 11; i++) {
        AppPlanMaker *appPlanMaker = new AppPlanMaker();
        appPlanMaker->_configPath = StringUtil::toString(i);
        appPlanMaker->_roleVersion = StringUtil::toString(i);
        manager._appPlanMakerQueue.push_back(appPlanMaker);
    }
    ASSERT_EQ(manager._appPlanMakerQueue[5], manager.getAppPlanMaker("5", "5"));
    ASSERT_TRUE(manager.getAppPlanMaker("6", "5") == NULL);
    ASSERT_EQ(size_t(10), manager._appPlanMakerQueue.size());
    ASSERT_TRUE(manager.getAppPlanMaker(GET_TEST_DATA_PATH() + "worker_manager_test/normal_conf", "5"));
    ASSERT_EQ(size_t(11), manager._appPlanMakerQueue.size());
}

TEST_F(WorkerManagerTest, testGetRoleCount) {
    WorkerManager manager;
    ASSERT_EQ(uint32_t(4),
              manager.getRoleCount(
                  GET_TEST_DATA_PATH() + "worker_manager_test/normal_conf", "5", RoleType::ROLE_TYPE_BROKER));
    ASSERT_EQ(uint32_t(0), manager.getRoleCount("invalid_config", "5", RoleType::ROLE_TYPE_BROKER));
}

}; // namespace admin
}; // namespace swift
