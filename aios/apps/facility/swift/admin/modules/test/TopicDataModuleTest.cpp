#include <iostream>
#include <string>

#include "aios/apps/facility/cm2/cm_basic/util/zk_wrapper.h"
#include "autil/CommonMacros.h"
#include "autil/TimeUtility.h"
#include "fslib/common/common_type.h"
#include "fslib/fs/File.h"
#include "fslib/fs/FileSystem.h"
#include "fslib/fs/zookeeper/test/ZkServer.h"
#include "swift/common/Common.h"
#include "unittest/unittest.h"

using namespace std;
using namespace testing;
using namespace fslib::fs;
using namespace fslib::util;
using namespace fslib;
using namespace autil;
using namespace swift::common;

namespace swift {
namespace admin {

class TopicDataModuleTest : public TESTBASE {
public:
    void setUp();
    void tearDown();

private:
    zookeeper::ZkServer *_zkServer = nullptr;
    std::string _zkRoot;
    cm_basic::ZkWrapper *_zkWrapper = nullptr;
};

void TopicDataModuleTest::setUp() {
    _zkServer = zookeeper::ZkServer::getZkServer();
    std::ostringstream oss;
    oss << "127.0.0.1:" << _zkServer->port();
    string zkPath = "zfs://" + oss.str() + "/";
    _zkWrapper = new cm_basic::ZkWrapper(oss.str(), 1000);
    ASSERT_TRUE(_zkWrapper->open());
    _zkRoot = zkPath + GET_CLASS_NAME();
    std::cout << "zkRoot: " << _zkRoot << std::endl;
    fs::FileSystem::mkDir(_zkRoot);
}

void TopicDataModuleTest::tearDown() { DELETE_AND_SET_NULL(_zkWrapper); }

// TEST_F(TopicDataModuleTest, testSimple) {
//     common::LoadStatus loadStatus;
//     TopicDataModule topicDataModule(loadStatus);
//     config::AdminConfig adminConfig;
//     adminConfig.zkRoot = _zkRootSelf;
//     metaInfoReplicator._adminConfig = &adminConfig;
//     ASSERT_TRUE(metaInfoReplicator.doInit());
//     ASSERT_TRUE(metaInfoReplicator.doLoad());
// }

}; // namespace admin
}; // namespace swift
