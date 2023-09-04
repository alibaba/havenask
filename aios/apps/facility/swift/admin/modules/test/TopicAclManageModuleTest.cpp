#include "swift/admin/modules/TopicAclManageModule.h"

#include <iostream>
#include <map>
#include <memory>
#include <string>

#include "fslib/fs/zookeeper/test/ZkServer.h"
#include "swift/admin/AdminStatusManager.h"
#include "swift/admin/modules/BaseModule.h"
#include "swift/auth/TopicAclDataManager.h"
#include "swift/config/AdminConfig.h"
#include "unittest/unittest.h"

using namespace std;
using namespace testing;
using namespace swift::config;

namespace swift {
namespace admin {

class TopicAclManageModuleTest : public TESTBASE {
public:
    void setUp();
    void tearDown();

protected:
    zookeeper::ZkServer *_zkServer = nullptr;
    string _zkRoot;
};

void TopicAclManageModuleTest::setUp() {
    _zkServer = zookeeper::ZkServer::getZkServer();
    std::ostringstream oss;
    oss << "127.0.0.1:" << _zkServer->port();
    _zkRoot = "zfs://" + oss.str() + "/" + GET_CLASS_NAME();
    cout << "zkRoot: " << _zkRoot << endl;
}

void TopicAclManageModuleTest::tearDown() { _zkServer->stop(); }

TEST_F(TopicAclManageModuleTest, testDoUpdate) {
    {
        TopicAclManageModule module;
        AdminConfig config;
        module._adminConfig = &config;
        EXPECT_TRUE(module.doInit());
        Status status(LF_FOLLOWER);
        ASSERT_FALSE(module.doUpdate(status));
        EXPECT_EQ(module._aclDataManager, nullptr);
    }
    {
        TopicAclManageModule module;
        AdminConfig config;
        config.zkRoot = _zkRoot;
        module._adminConfig = &config;
        EXPECT_TRUE(module.doInit());
        Status status(LF_FOLLOWER);
        ASSERT_TRUE(module.doUpdate(status));
        EXPECT_EQ(true, module._aclDataManager->_readOnly);
        EXPECT_EQ(0, module._aclDataManager->_topicAclDataMap.size());
    }
}
}; // namespace admin
}; // namespace swift
