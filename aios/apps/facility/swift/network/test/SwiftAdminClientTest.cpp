#include "swift/network/SwiftAdminClient.h"

#include <iosfwd>
#include <memory>
#include <stdint.h>
#include <stdlib.h>
#include <string>

#include "arpc/ANetRPCServer.h"
#include "autil/CommonMacros.h"
#include "autil/StringUtil.h"
#include "autil/TimeUtility.h"
#include "swift/client/fake_client/FakeChannelManager.h"
#include "swift/client/fake_client/FakeRpcChannel.h"
#include "swift/network/SwiftRpcChannelManager.h"
#include "swift/protocol/AdminRequestResponse.pb.h"
#include "unittest/unittest.h"

using namespace std;
using namespace arpc;
using namespace autil;

using namespace swift::protocol;
using namespace swift::client;
namespace swift {
namespace network {

class SwiftAdminClientTest : public TESTBASE {
    void setUp() {
        _rpcServer = new ANetRPCServer(NULL, 1, 100);
        ASSERT_TRUE(_rpcServer->StartPrivateTransport());
        while (true) {
            string spec("tcp:127.0.0.1:");
            uint16_t port = 30000 + rand() % 10000;
            spec += StringUtil::toString(port);
            if (_rpcServer->Listen(spec)) {
                _spec = spec;
                break;
            }
        }
    }

    void tearDown() {
        _rpcServer->Close();
        _rpcServer->StopPrivateTransport();
        DELETE_AND_SET_NULL(_rpcServer);
    }

protected:
    arpc::ANetRPCServer *_rpcServer;
    std::string _spec;
};

TEST_F(SwiftAdminClientTest, testSyncCall) {
    FakeChannelManager fakeChannelMgr;
    SwiftRpcChannelManagerPtr channelManager(new SwiftRpcChannelManager());
    channelManager->init(&fakeChannelMgr);
    SwiftAdminClient client(_spec, channelManager);
    TopicInfoRequest request;
    TopicInfoResponse response;
    EXPECT_TRUE(client.getTopicInfo(&request, &response));
    fakeChannelMgr.stop();
    fakeChannelMgr.StopPrivateTransport();
}

TEST_F(SwiftAdminClientTest, testIsChannelWorks) {
    SwiftRpcChannelManagerPtr channelManager(new SwiftRpcChannelManager());
    channelManager->init(NULL);
    SwiftAdminClient client(_spec, channelManager);
    TopicInfoRequest request;
    TopicInfoResponse response;
    EXPECT_TRUE(client.createNewRpcChannel());
    EXPECT_TRUE(client.isChannelWorks());
    EXPECT_TRUE(client.createNewRpcChannel());
    EXPECT_TRUE(client.isChannelWorks());
}

} // namespace network
} // namespace swift
