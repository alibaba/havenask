#include <algorithm>
#include <iostream>
#include <stdint.h>
#include <string>
#include <vector>

#include "autil/TimeUtility.h"
#include "flatbuffers/flatbuffers.h"
#include "swift/protocol/FBMessageReader.h"
#include "swift/protocol/FBMessageWriter.h"
#include "swift/protocol/test/swift_test_generated.h"
#include "unittest/unittest.h"

using namespace autil;
using namespace std;
using namespace swift::protocol::flat::test;
class FlatDecodeEncodeTest : public TESTBASE {};
static int msgCount = 1000;
static int times = 100;
static int msgLen = 20000;

TEST_F(FlatDecodeEncodeTest, testEncodeMessage) {
    string data(msgLen, 'a');
    int64_t len;
    int64_t begTime = TimeUtility::currentTime();
    swift::protocol::FBMessageWriter writer;
    for (int k = 0; k < times; k++) {
        writer.reset();
        for (int i = 0; i < msgCount; ++i) {
            writer.addMessage(data);
        }
        writer.finish();
        len = writer.size();
    }
    int64_t endTime = TimeUtility::currentTime();
    cout << "used time: " << (endTime - begTime) / 1000.0 << "ms, size:" << len << endl;
}

TEST_F(FlatDecodeEncodeTest, testDecodeMessage) {
    string data(msgLen, int8_t(1));
    swift::protocol::FBMessageWriter writer;
    for (int i = 0; i < msgCount; ++i) {
        writer.addMessage(data);
    }
    writer.finish();
    int64_t begTime = TimeUtility::currentTime();
    for (int k = 0; k < times; k++) {
        swift::protocol::FBMessageReader reader;
        ASSERT_TRUE(reader.init(writer.data(), writer.size()));
        ASSERT_EQ(msgCount, reader.size());
    }
    int64_t endTime = TimeUtility::currentTime();
    cout << "used time: " << (endTime - begTime) / 1000.0 << "ms" << endl;
}

TEST_F(FlatDecodeEncodeTest, testDecodeErrorMessage) {
    string data = "ERROR MESSAGE";
    {
        swift::protocol::FBMessageReader reader;
        ASSERT_FALSE(reader.init(data));
    }
}

TEST_F(FlatDecodeEncodeTest, testEncodeRequest) {
    string data(msgLen, int8_t(1));
    int64_t len;
    int64_t begTime = TimeUtility::currentTime();
    flatbuffers::FlatBufferBuilder builder(3000000);
    for (int k = 0; k < times; k++) {
        builder.Clear();
        vector<flatbuffers::Offset<Message>> offsetVec;
        offsetVec.reserve(msgCount);
        for (int i = 0; i < msgCount; ++i) {
            // auto msgData = builder.CreateVector<int8_t>((int8_t*)data.c_str(), data.size());
            auto msgData = builder.CreateString(data);
            auto msg = CreateMessage(builder, msgData);
            offsetVec.push_back(msg);
        }
        auto msgsOffset = builder.CreateVector(offsetVec);
        auto topicName = builder.CreateString("aaaa_123456");
        auto request = CreateProductionRequest(builder, topicName, 1, msgsOffset, 111111111);
        builder.Finish(request);
        len = builder.GetSize();
    }
    int64_t endTime = TimeUtility::currentTime();
    cout << "used time: " << (endTime - begTime) / 1000.0 << "ms, size:" << len << endl;
}

TEST_F(FlatDecodeEncodeTest, testDecodeRequest) {
    string data(msgLen, int8_t(1));
    flatbuffers::FlatBufferBuilder builder;
    vector<flatbuffers::Offset<Message>> offsetVec;
    offsetVec.reserve(msgCount);
    for (int i = 0; i < msgCount; ++i) {
        // auto msgData = builder.CreateVector<int8_t>((int8_t*)data.c_str(), data.size());
        auto msgData = builder.CreateString(data);
        auto msg = CreateMessage(builder, msgData);
        offsetVec.push_back(msg);
    }
    auto topicName = builder.CreateString("aaaa_123456");
    auto msgsOffset = builder.CreateVector(offsetVec);
    auto request = CreateProductionRequest(builder, topicName, 1, msgsOffset, 111111111);
    builder.Finish(request);
    const uint8_t *buf = builder.GetBufferPointer();
    int64_t begTime = TimeUtility::currentTime();
    for (int k = 0; k < times; k++) {
        const ProductionRequest *request2 = flatbuffers::GetRoot<ProductionRequest>(buf);
        ASSERT_EQ(msgCount, request2->msgs()->size());
        // ASSERT_EQ(data, request2->msgs()->Get(k)->data()->str());
        if (k < msgCount) {
            auto msgData = request2->msgs()->Get(k)->data();
            string t((const char *)msgData->Data(), msgData->Length());
            ASSERT_EQ(data, t);
        }
    }
    int64_t endTime = TimeUtility::currentTime();
    cout << "used time: " << (endTime - begTime) / 1000.0 << "ms" << endl;
}
