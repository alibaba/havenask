#include "swift/admin/AdminServiceImpl.h"

#include <cstddef>
#include <google/protobuf/stubs/callback.h>
#include <memory>
#include <stdint.h>
#include <string>

#include "arpc/Tracer.h"
#include "arpc/anet/ANetRPCServerClosure.h"
#include "autil/TimeUtility.h"
#include "swift/admin/SwiftAdminServer.h"
#include "swift/admin/SysController.h"
#include "swift/config/AdminConfig.h"
#include "swift/protocol/AdminRequestResponse.pb.h"
#include "swift/protocol/ErrCode.pb.h"
#include "unittest/unittest.h"

namespace google {
namespace protobuf {
class RpcController;
} // namespace protobuf
} // namespace google

using namespace std;
using namespace testing;
using namespace swift::protocol;
using namespace swift::config;

namespace swift {
namespace admin {

class AdminServiceImplTest : public TESTBASE {
public:
    void setUp();
    void tearDown();
};

void AdminServiceImplTest::setUp() {}

void AdminServiceImplTest::tearDown() {}

TEST_F(AdminServiceImplTest, testAdminNotLeader) {
    {
        ::google::protobuf::RpcController *controller = NULL;
        TopicInfoRequest request;
        TopicInfoResponse response;
        ::google::protobuf::Closure *done = NULL;
        AdminServiceImpl adminServiceImpl(nullptr, nullptr, nullptr);
        adminServiceImpl.getTopicInfo(controller, &request, &response, done);
        ASSERT_EQ(ERROR_ADMIN_NOT_LEADER, response.errorinfo().errcode());
    }
}

TEST_F(AdminServiceImplTest, testInQueueTimeout) {
    string configPath = GET_TEST_DATA_PATH() + "config/admin_config_test";
    shared_ptr<AdminConfig> adminConfig(AdminConfig::loadConfig(configPath));
    ASSERT_TRUE(adminConfig != nullptr);
    SysController sysController(adminConfig.get(), nullptr);
    ASSERT_TRUE(sysController.init());
    AdminServiceImpl adminServiceImpl(adminConfig.get(), &sysController, nullptr);
    ASSERT_TRUE(adminServiceImpl.init());
    ::google::protobuf::RpcController *controller = NULL;
    TopicInfoRequest request; // default 2000ms timeout
    request.set_topicname("test");
    TopicInfoResponse response;
    {
        ::google::protobuf::Closure *done = new arpc::ANetRPCServerClosure(0, NULL, NULL, NULL, NULL, 1);
        arpc::Tracer *tracer = new arpc::Tracer();
        int64_t currentTime = autil::TimeUtility::currentTime();
        tracer->_beginHandleRequest = currentTime - 799 * 1000;
        arpc::ANetRPCServerClosure *rpcClosure = dynamic_cast<arpc::ANetRPCServerClosure *>(done);
        rpcClosure->SetTracer(tracer);
        adminServiceImpl.getTopicInfo(controller, &request, &response, done);
        ASSERT_EQ(ERROR_ADMIN_TOPIC_NOT_EXISTED, response.errorinfo().errcode());
    }
    {
        ::google::protobuf::Closure *done = new arpc::ANetRPCServerClosure(0, NULL, NULL, NULL, NULL, 1);
        arpc::Tracer *tracer = new arpc::Tracer();
        int64_t currentTime = autil::TimeUtility::currentTime();
        tracer->_beginHandleRequest = currentTime - 801 * 1000;
        arpc::ANetRPCServerClosure *rpcClosure = dynamic_cast<arpc::ANetRPCServerClosure *>(done);
        rpcClosure->SetTracer(tracer);
        adminServiceImpl.getTopicInfo(controller, &request, &response, done);
        ASSERT_EQ(ERROR_ADMIN_WAIT_IN_QUEUE_TIMEOUT, response.errorinfo().errcode());
    }
}

}; // namespace admin
}; // namespace swift
