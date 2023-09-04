#include "swift/admin/WorkerManagerLocal.h"

#include <cstddef>
#include <stdint.h>
#include <string>

#include "autil/TimeUtility.h"
#include "swift/protocol/Common.pb.h"
#include "unittest/unittest.h"

using namespace std;
using namespace testing;
using namespace autil;

namespace swift {
namespace admin {

class WorkerManagerLocalTest : public TESTBASE {
public:
    void setUp(){};
    void tearDown(){};
};

TEST_F(WorkerManagerLocalTest, testInit) {
    {
        WorkerManagerLocal workerManager("/a/b", "b/usr/local/etc/swift/swift_alog.conf");
        ASSERT_EQ("/a/b", workerManager._workDir);
        ASSERT_EQ("b/usr/local/etc/swift/swift_alog.conf", workerManager._logConfFile);
        ASSERT_FALSE(workerManager.init("hippoRoot", "", NULL));
    }
    {
        WorkerManagerLocal workerManager("/a/b", "b/usr/local/etc/swift/swift_alog.conf");
        ASSERT_EQ("/a/b", workerManager._workDir);
        ASSERT_EQ("b/usr/local/etc/swift/swift_alog.conf", workerManager._logConfFile);
        ASSERT_TRUE(workerManager.init("hippoRoot", "appid", NULL));
        ASSERT_TRUE(workerManager._scheduler != NULL);
    }
}

TEST_F(WorkerManagerLocalTest, testGetReadyRoleCount) {
    WorkerManagerLocal workerManager("aa", "bb");
    uint32_t total = 0, ready = 0;
    workerManager.getReadyRoleCount(
        protocol::ROLE_TYPE_ALL, GET_TEST_DATA_PATH() + "worker_manager_test/normal_conf", "5", total, ready);
    ASSERT_EQ(4, total);
    ASSERT_EQ(4, ready);
}

}; // namespace admin
}; // namespace swift
