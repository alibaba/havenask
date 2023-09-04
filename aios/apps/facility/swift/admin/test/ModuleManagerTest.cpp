#include "swift/admin/ModuleManager.h"

#include <gmock/gmock-matchers.h>
#include <gtest/gtest-matchers.h>
#include <memory>
#include <string>
#include <type_traits>
#include <unordered_map>

#include "swift/admin/AdminStatusManager.h"
#include "swift/admin/modules/BaseModule.h"
#include "swift/admin/modules/test/MockBaseModule.h"
#include "swift/common/Common.h"
#include "unittest/unittest.h"

namespace swift {
namespace config {
class AdminConfig;
} // namespace config

namespace admin {
class SysController;

using namespace swift::common;
class FailInitModule : public BaseModule {
public:
    FailInitModule() {}

protected:
    bool doInit() { return false; };
    bool doLoad() { return true; };
    bool doUnload() { return true; };
};

class SuccInitModule : public BaseModule {
public:
    SuccInitModule() {}

protected:
    bool doInit() { return true; };
    bool doLoad() { return true; };
    bool doUnload() { return true; };
};

class ModuleManagerTest : public TESTBASE {};

TEST_F(ModuleManagerTest, testInitSucc) {
    SysController *sysController = (SysController *)1;
    config::AdminConfig *adminConfig = (config::AdminConfig *)2;
    AdminStatusManager *adminStatusManager = (AdminStatusManager *)3;
    ModuleManager moduleManager;
    auto mockModule = std::make_shared<MockBaseModule>();
    EXPECT_CALL(*mockModule, doInit()).WillOnce(Return(true));
    moduleManager._moduleCreatorFunc.clear();
    moduleManager._moduleCreatorFunc["a"] = [&]() {
        mockModule->setLoadStatus(BaseModule::calcLoadStatus("M", "L"));
        return mockModule;
    };
    ASSERT_TRUE(moduleManager.init(adminStatusManager, sysController, adminConfig));
    ASSERT_EQ(mockModule, moduleManager.getModule("a"));
    ASSERT_EQ(1, moduleManager._modules.size());
}

TEST_F(ModuleManagerTest, testInitFailedFuncNullptr) {
    SysController *sysController = (SysController *)1;
    config::AdminConfig *adminConfig = (config::AdminConfig *)2;
    AdminStatusManager *adminStatusManager = (AdminStatusManager *)3;
    ModuleManager moduleManager;
    moduleManager._moduleCreatorFunc.clear();
    moduleManager._moduleCreatorFunc["a"] = nullptr;
    ASSERT_FALSE(moduleManager.init(adminStatusManager, sysController, adminConfig));
}

TEST_F(ModuleManagerTest, testInitFailedModuleNullptr) {
    SysController *sysController = (SysController *)1;
    config::AdminConfig *adminConfig = (config::AdminConfig *)2;
    AdminStatusManager *adminStatusManager = (AdminStatusManager *)3;
    ModuleManager moduleManager;
    moduleManager._moduleCreatorFunc.clear();
    moduleManager._moduleCreatorFunc["a"] = [&]() -> std::shared_ptr<BaseModule> { return nullptr; };
    ASSERT_FALSE(moduleManager.init(adminStatusManager, sysController, adminConfig));
}

TEST_F(ModuleManagerTest, testInitFailedInitFailed) {
    SysController *sysController = (SysController *)1;
    config::AdminConfig *adminConfig = (config::AdminConfig *)2;
    AdminStatusManager *adminStatusManager = (AdminStatusManager *)3;
    ModuleManager moduleManager;
    auto mockModule = std::make_shared<MockBaseModule>();
    EXPECT_CALL(*mockModule, doInit()).WillOnce(Return(false));
    moduleManager._moduleCreatorFunc.clear();
    moduleManager._moduleCreatorFunc["a"] = [&]() {
        mockModule->setLoadStatus(BaseModule::calcLoadStatus("M", "L"));
        return mockModule;
    };
    ASSERT_FALSE(moduleManager.init(adminStatusManager, sysController, adminConfig));
}

TEST_F(ModuleManagerTest, testInitBindDependModuleFailed) {
    SysController *sysController = (SysController *)1;
    config::AdminConfig *adminConfig = (config::AdminConfig *)2;
    AdminStatusManager *adminStatusManager = (AdminStatusManager *)3;
    ModuleManager moduleManager;
    auto mockModule = std::make_shared<MockBaseModule>();
    // failed because depend module not regist
    auto func = mockModule->_bindFuncMap["b"];
    mockModule->_dependMap["b"] = nullptr;
    EXPECT_CALL(*mockModule, doInit()).WillOnce(Return(true));
    moduleManager._moduleCreatorFunc.clear();
    moduleManager._moduleCreatorFunc["a"] = [&]() {
        mockModule->setLoadStatus(BaseModule::calcLoadStatus("M", "L"));
        return mockModule;
    };
    ASSERT_FALSE(moduleManager.init(adminStatusManager, sysController, adminConfig));
}

TEST_F(ModuleManagerTest, testBindDependModule) {
    SysController *sysController = (SysController *)1;
    config::AdminConfig *adminConfig = (config::AdminConfig *)2;
    AdminStatusManager *adminStatusManager = (AdminStatusManager *)3;
    ModuleManager moduleManager;
    auto moduleA = std::make_shared<MockBaseModule>();
    auto moduleB = std::make_shared<MockBaseModule>();
    auto moduleC = std::make_shared<MockBaseModule>();
    EXPECT_CALL(*moduleA, doInit()).WillOnce(Return(true));
    EXPECT_CALL(*moduleB, doInit()).WillOnce(Return(true));
    EXPECT_CALL(*moduleC, doInit()).WillOnce(Return(true));
    moduleB->_bindFuncMap["a"];
    moduleB->_dependMap["a"];
    moduleC->_bindFuncMap["a"];
    moduleC->_dependMap["a"];
    moduleC->_bindFuncMap["b"];
    moduleC->_dependMap["b"];
    moduleManager._moduleCreatorFunc.clear();
    moduleManager._moduleCreatorFunc["a"] = [&]() { return moduleA; };
    moduleManager._moduleCreatorFunc["b"] = [&]() { return moduleB; };
    moduleManager._moduleCreatorFunc["c"] = [&]() { return moduleC; };
    ASSERT_TRUE(moduleManager.init(adminStatusManager, sysController, adminConfig));
    ASSERT_EQ(2, moduleA->_beDependedMap.size());
    ASSERT_TRUE(moduleA->_beDependedMap.end() != moduleA->_beDependedMap.find("b"));
    ASSERT_TRUE(moduleA->_beDependedMap.end() != moduleA->_beDependedMap.find("c"));
    ASSERT_EQ(1, moduleB->_beDependedMap.size());
    ASSERT_TRUE(moduleB->_beDependedMap.end() != moduleB->_beDependedMap.find("c"));
}

TEST_F(ModuleManagerTest, testLoad) {
    SysController *sysController = (SysController *)1;
    config::AdminConfig *adminConfig = (config::AdminConfig *)2;
    AdminStatusManager *adminStatusManager = (AdminStatusManager *)3;
    ModuleManager moduleManager;
    auto moduleA = std::make_shared<MockBaseModule>();
    moduleA->setSysModule();
    auto moduleB = std::make_shared<MockBaseModule>();
    moduleB->setSysModule();
    auto moduleC = std::make_shared<MockBaseModule>();
    moduleC->setSysModule();
    EXPECT_CALL(*moduleA, doInit()).WillOnce(Return(true));
    EXPECT_CALL(*moduleB, doInit()).WillOnce(Return(true));
    EXPECT_CALL(*moduleC, doInit()).WillOnce(Return(true));
    std::string expectStr;
    EXPECT_CALL(*moduleA, doLoad()).WillOnce(Invoke([&]() -> bool {
        expectStr.push_back('a');
        return true;
    }));
    EXPECT_CALL(*moduleB, doLoad()).WillOnce(Invoke([&]() -> bool {
        expectStr.push_back('b');
        return true;
    }));
    EXPECT_CALL(*moduleC, doLoad()).WillOnce(Invoke([&]() -> bool {
        expectStr.push_back('c');
        return true;
    }));
    EXPECT_CALL(*moduleA, doUpdate(_)).WillOnce(Invoke([&]() -> bool { return true; }));
    EXPECT_CALL(*moduleB, doUpdate(_)).WillOnce(Invoke([&]() -> bool { return true; }));
    EXPECT_CALL(*moduleC, doUpdate(_)).WillOnce(Invoke([&]() -> bool { return true; }));
    EXPECT_CALL(*moduleA, doUnload()).WillOnce(Invoke([&]() -> bool {
        expectStr.push_back('a');
        return true;
    }));
    EXPECT_CALL(*moduleB, doUnload()).WillOnce(Invoke([&]() -> bool {
        expectStr.push_back('b');
        return true;
    }));
    EXPECT_CALL(*moduleC, doUnload()).WillOnce(Invoke([&]() -> bool {
        expectStr.push_back('c');
        return true;
    }));
    moduleB->_bindFuncMap["a"];
    moduleB->_dependMap["a"] = moduleA;
    moduleC->_bindFuncMap["a"];
    moduleC->_dependMap["a"] = moduleA;
    moduleC->_bindFuncMap["b"];
    moduleC->_dependMap["b"] = moduleB;
    moduleManager._moduleCreatorFunc.clear();
    moduleManager._moduleCreatorFunc["a"] = [&]() { return moduleA; };
    moduleManager._moduleCreatorFunc["b"] = [&]() { return moduleB; };
    moduleManager._moduleCreatorFunc["c"] = [&]() { return moduleC; };
    ASSERT_TRUE(moduleManager.init(adminStatusManager, sysController, adminConfig));
    ASSERT_EQ(2, moduleA->_beDependedMap.size());
    ASSERT_TRUE(moduleA->_beDependedMap.end() != moduleA->_beDependedMap.find("b"));
    ASSERT_TRUE(moduleA->_beDependedMap.end() != moduleA->_beDependedMap.find("c"));
    ASSERT_EQ(1, moduleB->_beDependedMap.size());
    ASSERT_TRUE(moduleB->_beDependedMap.end() != moduleB->_beDependedMap.find("c"));
    ASSERT_TRUE(moduleManager.start());
    moduleManager.stop();
    ASSERT_EQ("abccba", expectStr);
}

} // namespace admin
} // namespace swift
