#include <iostream>
#include <stdint.h>
#include <string>
#include <vector>

#include "autil/DataBuffer.h"
#include "autil/Span.h"
#include "autil/TimeUtility.h"
#include "unittest/unittest.h"

using namespace autil;
using namespace std;

struct Message {
    Message() : compress(false), msgId(0), timestamp(0), uint16Payload(0), uint8MaskPayload(0) {}
    void serialize(autil::DataBuffer &dataBuffer) const {
        dataBuffer.write(data);
        dataBuffer.write(compress);
        dataBuffer.write(msgId);
        dataBuffer.write(timestamp);
        dataBuffer.write(uint16Payload);
        dataBuffer.write(uint8MaskPayload);
    }

    void deserialize(autil::DataBuffer &dataBuffer) {
        dataBuffer.read(data);
        dataBuffer.read(compress);
        dataBuffer.read(msgId);
        dataBuffer.read(timestamp);
        dataBuffer.read(uint16Payload);
        dataBuffer.read(uint8MaskPayload);
    }

public:
    string data;
    bool compress;
    int64_t msgId;
    int64_t timestamp;
    int16_t uint16Payload;
    int8_t uint8MaskPayload;
};

struct ProductionRequest {
    ProductionRequest() : partitionId(0), sessionId(-1), compressMsgInBroker(false) {}

    void serialize(autil::DataBuffer &dataBuffer) const {
        dataBuffer.write(topicName);
        dataBuffer.write(partitionId);
        dataBuffer.write(msgs);
        dataBuffer.write(sessionId);
        dataBuffer.write(compressedMsg);
        dataBuffer.write(compressMsgInBroker);
    }

    void deserialize(autil::DataBuffer &dataBuffer) {
        dataBuffer.read(topicName);
        dataBuffer.read(partitionId);
        dataBuffer.read(msgs);
        dataBuffer.read(sessionId);
        dataBuffer.read(compressedMsg);
        dataBuffer.read(compressMsgInBroker);
    }

public:
    string topicName;
    uint16_t partitionId;
    vector<Message> msgs;
    int64_t sessionId;
    string compressedMsg;
    bool compressMsgInBroker;
};

class DataBufferDecodeEncodeTest : public TESTBASE {};
static int msgCount = 1000;
static int times = 100;
static int msgLen = 20000;

TEST_F(DataBufferDecodeEncodeTest, testEncodeMessage) {
    string data(msgLen, int8_t(1));
    int64_t begTime = TimeUtility::currentTime();
    int64_t len;
    autil::DataBuffer dataBuffer(3000000);
    for (int k = 0; k < times; k++) {
        dataBuffer.clear();
        vector<Message> msgs;
        msgs.reserve(msgCount);
        for (int i = 0; i < msgCount; ++i) {
            Message msg;
            msg.data = data;
            msgs.push_back(msg);
        }
        dataBuffer.write(msgs);
        len = dataBuffer.getDataLen();
    }
    int64_t endTime = TimeUtility::currentTime();
    cout << "used time: " << (endTime - begTime) / 1000.0 << "ms, len:" << len << endl;
}

TEST_F(DataBufferDecodeEncodeTest, testDecodeMessage) {
    string data(msgLen, int8_t(1));
    vector<Message> msgs;
    msgs.reserve(msgCount);
    autil::DataBuffer dataBuffer;
    for (int i = 0; i < msgCount; ++i) {
        Message msg;
        msg.data = data;
        msgs.push_back(msg);
    }
    dataBuffer.write(msgs);

    int64_t begTime = TimeUtility::currentTime();
    for (int k = 0; k < times; k++) {
        vector<Message> msgs2;
        autil::DataBuffer dataBuffer2(dataBuffer.getData(), dataBuffer.getDataLen());
        dataBuffer2.read(msgs2);
        ASSERT_EQ(msgCount, msgs2.size());
        //    ASSERT_EQ(data, msgs2.msgs(k).data());
    }
    int64_t endTime = TimeUtility::currentTime();
    cout << "used time: " << (endTime - begTime) / 1000.0 << "ms" << endl;
}

// TEST_F(DataBufferDecodeEncodeTest, testEncodeRequest) {
//     string data(msgLen, int8_t(1));
//     int64_t len;
//     int64_t begTime = TimeUtility::currentTime();
//     for (int k = 0; k < times; k++) {
//         swift::protocol::ProductionRequest request;
//         for (int i = 0; i < msgCount; ++i) {
//             swift::protocol::Message *msg = request.add_msgs();
//             msg->set_data(data.c_str());
//         }
//         request.set_topicname("aaaaa_12345");
//         request.set_partitionid(1);
//         request.set_sessionid(1111111111);
//         string msgData;
//         request.SerializeToString(&msgData);
//         len = msgData.size();
//     }
//     int64_t endTime = TimeUtility::currentTime();
//     cout << "used time: " << (endTime - begTime) /1000.0 << "ms, size: " << len  << endl;
// }

// TEST_F(DataBufferDecodeEncodeTest, testDecodeRequest) {
//     string data(msgLen , int8_t(1));
//     swift::protocol::ProductionRequest request;
//     for (int i = 0; i < msgCount; ++i) {
//         swift::protocol::Message *msg = request.add_msgs();
//         msg->set_data(data.c_str());
//     }
//     request.set_topicname("aaaaa_12345");
//     request.set_partitionid(1);
//     request.set_sessionid(1111111111);
//     string msgData;
//     request.SerializeToString(&msgData);
//     int64_t begTime = TimeUtility::currentTime();
//     for (int k = 0; k < times; k++) {
//         swift::protocol::ProductionRequest request2;
//         request2.ParseFromString(msgData);
//         ASSERT_EQ(msgCount, request2.msgs_size());
//     }
//     int64_t endTime = TimeUtility::currentTime();
//     cout << "used time: " << (endTime - begTime) /1000.0 << "ms"  << endl;
// }
