#include "swift/admin/AdminWorker.h"

#include <google/protobuf/stubs/callback.h>
#include <stddef.h>
#include <stdint.h>
#include <string>

#include "arpc/Tracer.h"
#include "arpc/anet/ANetRPCServerClosure.h"
#include "autil/EnvUtil.h"
#include "autil/OptionParser.h"
#include "autil/TimeUtility.h"
#include "swift/admin/AdminServiceImpl.h"
#include "swift/heartbeat/ZkHeartbeatWrapper.h"
#include "swift/monitor/AdminMetricsReporter.h"
#include "swift/protocol/AdminRequestResponse.pb.h"
#include "swift/protocol/ErrCode.pb.h"
#include "unittest/unittest.h"

namespace google {
namespace protobuf {
class RpcController;
} // namespace protobuf
} // namespace google

using namespace std;
using namespace swift::protocol;
using namespace swift::monitor;

namespace swift {
namespace admin {

class AdminWorkerTest : public TESTBASE {};

TEST_F(AdminWorkerTest, testInvalidParam) {
    AdminWorker admin;
    admin._adminMetricsReporter = new AdminMetricsReporter();
    admin._configPath = GET_TEST_DATA_PATH() + "config/admin_config_test";
    ASSERT_FALSE(admin.doInit()); // register server fail
    ::google::protobuf::RpcController *controller = NULL;
    TopicInfoRequest request;
    TopicInfoResponse response;
    ::google::protobuf::Closure *done = new arpc::ANetRPCServerClosure(0, NULL, NULL, NULL, NULL, 1);
    arpc::Tracer *tracer = new arpc::Tracer();
    int64_t currentTime = autil::TimeUtility::currentTime();
    tracer->_beginHandleRequest = currentTime;
    arpc::ANetRPCServerClosure *rpcClosure = dynamic_cast<arpc::ANetRPCServerClosure *>(done);
    rpcClosure->SetTracer(tracer);
    admin._adminServiceImpl->getTopicInfo(controller, &request, &response, done);
    ASSERT_EQ(ERROR_ADMIN_INVALID_PARAMETER, response.errorinfo().errcode());
}

TEST_F(AdminWorkerTest, testInitEnvArgs) {
    AdminWorker admin;
    admin.doInitOptions();
    {
        string command = "--env key=abc123";
        admin.getOptionParser().parseArgs(command);
        admin.initEnvArgs();
        string value = autil::EnvUtil::getEnv("key", "");
        ASSERT_EQ("abc123", value);
    }
    {
        string command = "--env key=";
        admin.getOptionParser().parseArgs(command);
        admin.initEnvArgs();
        string value = autil::EnvUtil::getEnv("key", "");
        ASSERT_EQ("", value);
    }
    {
        string command = "--env";
        admin.getOptionParser().parseArgs(command);
        admin.initEnvArgs();
        string value = autil::EnvUtil::getEnv("key", "");
        ASSERT_EQ("", value);
    }
}

} // namespace admin
} // namespace swift
