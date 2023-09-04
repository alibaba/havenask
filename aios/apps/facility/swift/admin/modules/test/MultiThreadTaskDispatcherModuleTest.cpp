#include "swift/admin/modules/MultiThreadTaskDispatcherModule.h"

#include <algorithm>
#include <ext/alloc_traits.h>
#include <iostream>
#include <memory>
#include <set>
#include <stdint.h>
#include <string>
#include <utility>
#include <vector>

#include "aios/apps/facility/cm2/cm_basic/util/zk_wrapper.h"
#include "autil/StringUtil.h"
#include "autil/TimeUtility.h"
#include "fslib/common/common_type.h"
#include "fslib/fs/File.h"
#include "fslib/fs/FileSystem.h"
#include "fslib/fs/zookeeper/test/ZkServer.h"
#include "swift/admin/AdminZkDataAccessor.h"
#include "swift/admin/AdminZkDataAccessorPool.h"
#include "swift/admin/WorkerInfo.h"
#include "swift/common/Common.h"
#include "swift/common/PathDefine.h"
#include "swift/config/AdminConfig.h"
#include "swift/protocol/Common.pb.h"
#include "unittest/unittest.h"

using namespace std;
using namespace testing;
using namespace fslib::fs;
using namespace fslib;
using namespace autil;
using namespace swift::protocol;
using namespace swift::common;

namespace swift {
namespace admin {

class MultiThreadTaskDispatcherModuleTest : public TESTBASE {
public:
    void setUp();
    void tearDown();
    void makeDispatchInfo(int32_t taskCount, uint32_t start, vector<pair<WorkerInfoPtr, DispatchInfo>> &outTasks) {
        outTasks.clear();
        for (int32_t i = 0; i < taskCount; i++) {
            DispatchInfo info;
            info.set_brokeraddress(StringUtil::toString(start + i));
            info.set_rolename(StringUtil::toString(start + i));
            outTasks.push_back(make_pair(WorkerInfoPtr(), info));
        }
    }
    void checkTasks(const vector<pair<WorkerInfoPtr, DispatchInfo>> &tasks, const set<string> &failedTask) {
        for (const auto &task : tasks) {
            string path = PathDefine::getTaskInfoPath(_zkRoot, task.second.rolename());
            if (failedTask.count(task.second.rolename()) > 0) {
                EXPECT_EQ(EC_FALSE, fs::FileSystem::isExist(path)) << path;
            } else {
                EXPECT_EQ(EC_TRUE, fs::FileSystem::isExist(path)) << path;
            }
        }
    }

private:
    zookeeper::ZkServer *_zkServer = nullptr;
    string _zkRoot;
};

void MultiThreadTaskDispatcherModuleTest::setUp() {
    _zkServer = zookeeper::ZkServer::getZkServer();
    std::ostringstream oss;
    oss << "127.0.0.1:" << _zkServer->port();
    string zkPath = "zfs://" + oss.str() + "/";
    _zkRoot = zkPath + GET_CLASS_NAME();
    cout << "zkRoot: " << _zkRoot << endl;
    fs::FileSystem::mkDir(_zkRoot);
}

void MultiThreadTaskDispatcherModuleTest::tearDown() {}

TEST_F(MultiThreadTaskDispatcherModuleTest, testSimple) {
    MultiThreadTaskDispatcherModule dispatcher;
    config::AdminConfig adminConfig;
    adminConfig.multiThreadDispatchTask = true;
    adminConfig.dispatchTaskThreadNum = 3;
    adminConfig.zkRoot = _zkRoot;
    dispatcher._adminConfig = &adminConfig;
    ASSERT_TRUE(dispatcher.load());
    ASSERT_TRUE(dispatcher._accessorPool);
    vector<pair<WorkerInfoPtr, DispatchInfo>> tasks;
    makeDispatchInfo(4, 0, tasks);
    dispatcher.dispatchTasks(tasks);
    checkTasks(tasks, {});
}

TEST_F(MultiThreadTaskDispatcherModuleTest, testZkIsDisconnect) {
    MultiThreadTaskDispatcherModule dispatcher;
    config::AdminConfig adminConfig;
    adminConfig.multiThreadDispatchTask = true;
    adminConfig.dispatchTaskThreadNum = 1;
    adminConfig.zkRoot = _zkRoot;
    dispatcher._adminConfig = &adminConfig;
    ASSERT_TRUE(dispatcher.load());
    dispatcher._accessorPool->_accessorPool[0]->getZkWrapper()->close();
    vector<pair<WorkerInfoPtr, DispatchInfo>> tasks;
    makeDispatchInfo(10, 5, tasks);
    dispatcher.dispatchTasks(tasks);
    checkTasks(tasks, {"5"});
}

}; // namespace admin
}; // namespace swift
