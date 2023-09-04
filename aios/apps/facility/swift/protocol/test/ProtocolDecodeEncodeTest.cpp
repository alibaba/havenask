#include <iostream>
#include <stdint.h>
#include <string>

#include "autil/TimeUtility.h"
#include "swift/protocol/BrokerRequestResponse.pb.h"
#include "swift/protocol/SwiftMessage.pb.h"
#include "unittest/unittest.h"

using namespace autil;
using namespace std;
class ProtocolDecodeEncodeTest : public TESTBASE {};
static int msgCount = 1000;
static int times = 100;
static int msgLen = 20000;

TEST_F(ProtocolDecodeEncodeTest, testEncodeMessage) {
    string data(msgLen, int8_t(1));
    int64_t begTime = TimeUtility::currentTime();
    int64_t len;
    for (int k = 0; k < times; k++) {
        swift::protocol::Messages msgs;
        for (int i = 0; i < msgCount; ++i) {
            swift::protocol::Message *msg = msgs.add_msgs();
            msg->set_data(data.c_str());
        }
        string msgData;
        msgs.SerializeToString(&msgData);
        len = msgData.size();
    }
    int64_t endTime = TimeUtility::currentTime();
    cout << "used time: " << (endTime - begTime) / 1000.0 << "ms, len:" << len << endl;
}

TEST_F(ProtocolDecodeEncodeTest, testDecodeMessage) {
    string data(msgLen, int8_t(1));
    swift::protocol::Messages msgs;
    for (int i = 0; i < msgCount; ++i) {
        swift::protocol::Message *msg = msgs.add_msgs();
        msg->set_data(data.c_str());
    }
    string msgData;
    msgs.SerializeToString(&msgData);

    int64_t begTime = TimeUtility::currentTime();
    for (int k = 0; k < times; k++) {
        swift::protocol::Messages msgs2;
        msgs2.ParseFromString(msgData);
        ASSERT_EQ(msgCount, msgs2.msgs_size());
        //    ASSERT_EQ(data, msgs2.msgs(k).data());
    }
    int64_t endTime = TimeUtility::currentTime();
    cout << "used time: " << (endTime - begTime) / 1000.0 << "ms" << endl;
}

TEST_F(ProtocolDecodeEncodeTest, testEncodeRequest) {
    string data(msgLen, int8_t(1));
    int64_t len;
    int64_t begTime = TimeUtility::currentTime();
    for (int k = 0; k < times; k++) {
        swift::protocol::ProductionRequest request;
        for (int i = 0; i < msgCount; ++i) {
            swift::protocol::Message *msg = request.add_msgs();
            msg->set_data(data.c_str());
        }
        request.set_topicname("aaaaa_12345");
        request.set_partitionid(1);
        request.set_sessionid(1111111111);
        string msgData;
        request.SerializeToString(&msgData);
        len = msgData.size();
    }
    int64_t endTime = TimeUtility::currentTime();
    cout << "used time: " << (endTime - begTime) / 1000.0 << "ms, size: " << len << endl;
}

TEST_F(ProtocolDecodeEncodeTest, testDecodeRequest) {
    string data(msgLen, int8_t(1));
    swift::protocol::ProductionRequest request;
    for (int i = 0; i < msgCount; ++i) {
        swift::protocol::Message *msg = request.add_msgs();
        msg->set_data(data.c_str());
    }
    request.set_topicname("aaaaa_12345");
    request.set_partitionid(1);
    request.set_sessionid(1111111111);
    string msgData;
    request.SerializeToString(&msgData);
    int64_t begTime = TimeUtility::currentTime();
    for (int k = 0; k < times; k++) {
        swift::protocol::ProductionRequest request2;
        request2.ParseFromString(msgData);
        ASSERT_EQ(msgCount, request2.msgs_size());
    }
    int64_t endTime = TimeUtility::currentTime();
    cout << "used time: " << (endTime - begTime) / 1000.0 << "ms" << endl;
}
