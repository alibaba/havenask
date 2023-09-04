#include "swift/admin/modules/BaseModule.h"

#include <atomic>

#include "autil/Log.h"
#include "swift/admin/AdminStatusManager.h"
#include "swift/admin/modules/test/MockBaseModule.h"
#include "swift/common/Common.h"
#include "unittest/unittest.h"

namespace swift {
namespace config {
class AdminConfig;
} // namespace config

namespace admin {
class ModuleManager;
class SysController;

using namespace swift::config;
using namespace swift::common;

class BaseModuleTest : public TESTBASE {
public:
protected:
    AUTIL_LOG_DECLARE();
};

AUTIL_LOG_SETUP(swift, BaseModuleTest);

TEST_F(BaseModuleTest, testInit) {
    ModuleManager *moduleManager = (ModuleManager *)1;
    SysController *sysController = (SysController *)2;
    AdminConfig *adminConfig = (AdminConfig *)3;
    MockBaseModule baseModule;
    ASSERT_FALSE(baseModule.init(nullptr, nullptr, nullptr));

    EXPECT_CALL(baseModule, doInit()).WillOnce(Return(false));
    ASSERT_FALSE(baseModule.init(moduleManager, sysController, adminConfig));

    EXPECT_CALL(baseModule, doInit()).WillOnce(Return(false));
    ASSERT_FALSE(baseModule.init(moduleManager, sysController, adminConfig));

    EXPECT_CALL(baseModule, doInit()).WillOnce(Return(true));
    ASSERT_TRUE(baseModule.init(moduleManager, sysController, adminConfig));
    ASSERT_EQ(moduleManager, baseModule._moduleManager);
    ASSERT_EQ(sysController, baseModule._sysController);
    ASSERT_EQ(adminConfig, baseModule._adminConfig);
}

TEST_F(BaseModuleTest, testLoad) {
    MockBaseModule baseModule;
    baseModule._load = true;
    ASSERT_TRUE(baseModule.load());

    baseModule._load = false;
    EXPECT_CALL(baseModule, doLoad()).WillOnce(Return(false));
    ASSERT_FALSE(baseModule.load());

    baseModule._load = false;
    EXPECT_CALL(baseModule, doLoad()).WillOnce(Return(true));
    ASSERT_TRUE(baseModule.load());
    ASSERT_TRUE(baseModule._load);
}

TEST_F(BaseModuleTest, testNeedLoad) {
    {
        MockBaseModule baseModule;
        baseModule.setLoadStatus(BaseModule::calcLoadStatus("M|S|U", "L|F|U"));
        ASSERT_TRUE(baseModule.needLoad(Status()));
        ASSERT_TRUE(baseModule.needLoad(Status(MS_MASTER)));
        ASSERT_TRUE(baseModule.needLoad(Status(MS_SLAVE)));
        ASSERT_TRUE(baseModule.needLoad(Status(LF_LEADER)));
        ASSERT_TRUE(baseModule.needLoad(Status(LF_FOLLOWER)));
    }
    {
        MockBaseModule baseModule;
        baseModule.setLoadStatus(BaseModule::calcLoadStatus("M|S", "L|F"));
        ASSERT_FALSE(baseModule.needLoad(Status()));
        ASSERT_FALSE(baseModule.needLoad(Status(MS_MASTER)));
        ASSERT_FALSE(baseModule.needLoad(Status(MS_SLAVE)));
        ASSERT_FALSE(baseModule.needLoad(Status(LF_LEADER)));
        ASSERT_FALSE(baseModule.needLoad(Status(LF_FOLLOWER)));
        ASSERT_TRUE(baseModule.needLoad(Status(MS_MASTER, LF_LEADER)));
        ASSERT_TRUE(baseModule.needLoad(Status(MS_MASTER, LF_FOLLOWER)));
        ASSERT_TRUE(baseModule.needLoad(Status(MS_SLAVE, LF_LEADER)));
        ASSERT_TRUE(baseModule.needLoad(Status(MS_SLAVE, LF_FOLLOWER)));
    }
    {
        MockBaseModule baseModule;
        baseModule.setLoadStatus(BaseModule::calcLoadStatus("M", "L"));
        ASSERT_FALSE(baseModule.needLoad(Status()));
        ASSERT_FALSE(baseModule.needLoad(Status(MS_MASTER)));
        ASSERT_FALSE(baseModule.needLoad(Status(MS_SLAVE)));
        ASSERT_FALSE(baseModule.needLoad(Status(LF_LEADER)));
        ASSERT_FALSE(baseModule.needLoad(Status(LF_FOLLOWER)));
        ASSERT_TRUE(baseModule.needLoad(Status(MS_MASTER, LF_LEADER)));
        ASSERT_FALSE(baseModule.needLoad(Status(MS_MASTER, LF_FOLLOWER)));
        ASSERT_FALSE(baseModule.needLoad(Status(MS_SLAVE, LF_LEADER)));
        ASSERT_FALSE(baseModule.needLoad(Status(MS_SLAVE, LF_FOLLOWER)));
    }
    {
        MockBaseModule baseModule;
        baseModule.setLoadStatus(BaseModule::calcLoadStatus("M", "F"));
        ASSERT_FALSE(baseModule.needLoad(Status()));
        ASSERT_FALSE(baseModule.needLoad(Status(MS_MASTER)));
        ASSERT_FALSE(baseModule.needLoad(Status(MS_SLAVE)));
        ASSERT_FALSE(baseModule.needLoad(Status(LF_LEADER)));
        ASSERT_FALSE(baseModule.needLoad(Status(LF_FOLLOWER)));
        ASSERT_FALSE(baseModule.needLoad(Status(MS_MASTER, LF_LEADER)));
        ASSERT_TRUE(baseModule.needLoad(Status(MS_MASTER, LF_FOLLOWER)));
        ASSERT_FALSE(baseModule.needLoad(Status(MS_SLAVE, LF_LEADER)));
        ASSERT_FALSE(baseModule.needLoad(Status(MS_SLAVE, LF_FOLLOWER)));
    }
    {
        MockBaseModule baseModule;
        baseModule.setLoadStatus(BaseModule::calcLoadStatus("S", "F"));
        ASSERT_FALSE(baseModule.needLoad(Status()));
        ASSERT_FALSE(baseModule.needLoad(Status(MS_MASTER)));
        ASSERT_FALSE(baseModule.needLoad(Status(MS_SLAVE)));
        ASSERT_FALSE(baseModule.needLoad(Status(LF_LEADER)));
        ASSERT_FALSE(baseModule.needLoad(Status(LF_FOLLOWER)));
        ASSERT_FALSE(baseModule.needLoad(Status(MS_MASTER, LF_LEADER)));
        ASSERT_FALSE(baseModule.needLoad(Status(MS_MASTER, LF_FOLLOWER)));
        ASSERT_FALSE(baseModule.needLoad(Status(MS_SLAVE, LF_LEADER)));
        ASSERT_TRUE(baseModule.needLoad(Status(MS_SLAVE, LF_FOLLOWER)));
    }
    {
        MockBaseModule baseModule;
        baseModule.setLoadStatus(BaseModule::calcLoadStatus("S", "L"));
        ASSERT_FALSE(baseModule.needLoad(Status()));
        ASSERT_FALSE(baseModule.needLoad(Status(MS_MASTER)));
        ASSERT_FALSE(baseModule.needLoad(Status(MS_SLAVE)));
        ASSERT_FALSE(baseModule.needLoad(Status(LF_LEADER)));
        ASSERT_FALSE(baseModule.needLoad(Status(LF_FOLLOWER)));
        ASSERT_FALSE(baseModule.needLoad(Status(MS_MASTER, LF_LEADER)));
        ASSERT_FALSE(baseModule.needLoad(Status(MS_MASTER, LF_FOLLOWER)));
        ASSERT_TRUE(baseModule.needLoad(Status(MS_SLAVE, LF_LEADER)));
        ASSERT_FALSE(baseModule.needLoad(Status(MS_SLAVE, LF_FOLLOWER)));
    }
}

TEST_F(BaseModuleTest, testGetLoadTag) {
    ASSERT_EQ(common::LoadStatus("111111"), BaseModule::calcLoadStatus("M|S|U", "L|F|U"));
    ASSERT_EQ(common::LoadStatus("110110"), BaseModule::calcLoadStatus("M|S", "L|F"));
    ASSERT_EQ(common::LoadStatus("010110"), BaseModule::calcLoadStatus("S|M", "L"));
    ASSERT_EQ(common::LoadStatus("100110"), BaseModule::calcLoadStatus("S|M", "F"));
    ASSERT_EQ(common::LoadStatus("100100"), BaseModule::calcLoadStatus("S", "F"));
    ASSERT_EQ(common::LoadStatus("100010"), BaseModule::calcLoadStatus("M", "F"));
    ASSERT_EQ(common::LoadStatus("010010"), BaseModule::calcLoadStatus("M", "L"));
    ASSERT_EQ(common::LoadStatus("100010"), BaseModule::calcLoadStatus("M", "F"));
}

} // namespace admin
} // namespace swift
