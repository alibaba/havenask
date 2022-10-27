#include "build_service/test/unittest.h"
#include "build_service/util/PooledChannelManager.h"
#include "build_service/util/test/MockConnection.h"
#include "build_service/util/test/MockANetRPCChannelManager.h"
#include <arpc/ANetRPCChannel.h>
using namespace std;
using namespace testing;
using namespace anet;
using namespace arpc;

namespace build_service {
namespace util {

class PooledChannelManagerTest : public BUILD_SERVICE_TESTBASE {
public:
    void setUp();
    void tearDown();
protected:
    PooledChannelManager *_pooledChannelManager;
    MockANetRPCChannelManager *_anetRPCChannelManager;
    NiceMockPacketStreamer _streamer;
};

void PooledChannelManagerTest::setUp() {
    _pooledChannelManager = new PooledChannelManager();
    _anetRPCChannelManager = new StrictMockANetRPCChannelManager();
    delete _pooledChannelManager->_rpcChannelManager;
    _pooledChannelManager->_rpcChannelManager = _anetRPCChannelManager;
}

void PooledChannelManagerTest::tearDown() {
    DELETE_AND_SET_NULL(_pooledChannelManager);
}

TEST_F(PooledChannelManagerTest, testCleanBrokenChannelLoop) {
    MockConnection *connection = new StrictMockConnection(&_streamer);
    RPCChannelPtr rpcChannel(new ANetRPCChannel(connection, NULL, true));
    _pooledChannelManager->_rpcChannelPool["channel 0"] = rpcChannel;
    EXPECT_CALL(*connection, isClosed()).WillOnce(Return(true));
    _pooledChannelManager->cleanBrokenChannelLoop();
    EXPECT_EQ((size_t)0, _pooledChannelManager->_rpcChannelPool.size());
    rpcChannel.reset();
    delete connection;
}

TEST_F(PooledChannelManagerTest, testConvertToSpec) {
    EXPECT_EQ("tcp:10.101.101.101", PooledChannelManager::convertToSpec("tcp:10.101.101.101"));
    EXPECT_EQ("udp:10.101.101.101", PooledChannelManager::convertToSpec("udp:10.101.101.101"));
    EXPECT_EQ("http:10.101.101.101", PooledChannelManager::convertToSpec("http:10.101.101.101"));
    EXPECT_EQ("tcp:10.101.101.101", PooledChannelManager::convertToSpec("10.101.101.101"));
}

TEST_F(PooledChannelManagerTest, testGetRpcChannel) {
    string addr = "tcp:10.101.101.101";
    string newAddr = "tcp:10.101.101.102";
    EXPECT_CALL(*_anetRPCChannelManager, OpenChannel(addr, false, 50ul,
                    5000, false, _)).WillOnce(ReturnNull());
    EXPECT_FALSE(_pooledChannelManager->getRpcChannel(addr));
    EXPECT_TRUE(_pooledChannelManager->_loopThreadPtr.get() != NULL);
    EXPECT_EQ((size_t)0, _pooledChannelManager->_rpcChannelPool.size());

    MockConnection *connection = new NiceMockConnection(&_streamer);
    RPCChannel *rpcChannel = new ANetRPCChannel(connection, NULL, true);
    EXPECT_CALL(*_anetRPCChannelManager, OpenChannel(addr, false, 50ul,
                    5000, false, _)).WillOnce(Return(rpcChannel));
    EXPECT_EQ(rpcChannel, _pooledChannelManager->getRpcChannel(addr).get());
    EXPECT_EQ((size_t)1, _pooledChannelManager->_rpcChannelPool.size());

    MockConnection *connection1 = new NiceMockConnection(&_streamer);
    RPCChannel *rpcChannel1 = new ANetRPCChannel(connection1, NULL, true);
    EXPECT_CALL(*_anetRPCChannelManager, OpenChannel(newAddr, false, 50ul,
                    5000, false, _)).WillOnce(Return(rpcChannel1));
    EXPECT_EQ(rpcChannel1, _pooledChannelManager->getRpcChannel(newAddr).get());
    EXPECT_EQ((size_t)2, _pooledChannelManager->_rpcChannelPool.size());

    EXPECT_EQ(rpcChannel1, _pooledChannelManager->getRpcChannel(newAddr).get());
    EXPECT_EQ((size_t)2, _pooledChannelManager->_rpcChannelPool.size());
    
    EXPECT_CALL(*connection1, isClosed()).WillOnce(Return(true));
    EXPECT_FALSE(_pooledChannelManager->getRpcChannel(newAddr));
    EXPECT_EQ((size_t)1, _pooledChannelManager->_rpcChannelPool.size());

    _pooledChannelManager->_loopThreadPtr.reset();
    DELETE_AND_SET_NULL(_pooledChannelManager);
    delete connection;
    delete connection1;
}

TEST_F(PooledChannelManagerTest, testCreateRpcChannel) {
    string addr = "tcp:x.x.x.x";
    EXPECT_CALL(*_anetRPCChannelManager, OpenChannel(addr, false, 50ul, 5000, false, _)).WillOnce(ReturnNull());
    EXPECT_FALSE(_pooledChannelManager->createRpcChannel(addr));
    MockConnection *connection1 = new NiceMockConnection(&_streamer);
    RPCChannel *channel = new ANetRPCChannel(connection1, NULL, true); // any pointer
    EXPECT_CALL(*_anetRPCChannelManager, OpenChannel(addr, false, 50ul, 5000, false, _)).WillOnce(Return(channel));
    EXPECT_EQ(channel, _pooledChannelManager->createRpcChannel(addr).get());
    delete connection1;
}

}
}
