#include "swift/util/MessageUtil.h"

#include <algorithm>
#include <iostream>
#include <stddef.h>
#include <stdint.h>
#include <string>
#include <vector>

#include "autil/HashAlgorithm.h"
#include "autil/TimeUtility.h"
#include "autil/ZlibCompressor.h"
#include "swift/common/Common.h"
#include "swift/common/FieldGroupWriter.h"
#include "swift/common/FieldSchema.h"
#include "swift/common/MessageInfo.h"
#include "swift/filter/FieldFilter.h"
#include "swift/filter/MessageFilter.h"
#include "swift/protocol/ErrCode.pb.h"
#include "swift/protocol/SwiftMessage.pb.h"
#include "swift/util/MessageConverter.h"
#include "swift/util/MessageInfoUtil.h"
#include "unittest/unittest.h"

using namespace std;
using namespace testing;
using namespace autil;
using namespace swift::protocol;
using namespace swift::common;
namespace swift {
namespace util {

class MessageUtilTest : public TESTBASE {
public:
    void setUp();
    void tearDown();

public:
    protocol::Message prepareMergeMsg(int64_t threshold = -1, bool fieldFilter = false, bool compressOne = false);
};

void MessageUtilTest::setUp() {}

void MessageUtilTest::tearDown() {}

protocol::Message MessageUtilTest::prepareMergeMsg(int64_t threshold, bool fieldFilter, bool compressOne) {
    vector<MessageInfo *> msgVec;
    string msg1Data(100, '1');
    string msg2Data(100, '2');
    ZlibCompressor compressor;
    {
        if (fieldFilter) {
            FieldGroupWriter writer;
            writer.addProductionField("field_a", "1", true);
            msg1Data = writer.toString();
            writer.reset();
            writer.addProductionField("field_b", "2", true);
            msg2Data = writer.toString();
        }
        MessageInfo *msg1 = new MessageInfo(msg1Data, 2, 3, 4);
        MessageInfo *msg2 = new MessageInfo(msg2Data, 3, 4, 5);
        if (compressOne) {
            MessageInfoUtil::compressMessage(&compressor, threshold, *msg1);
        }
        msgVec.push_back(msg1);
        msgVec.push_back(msg2);
    }
    MessageInfo mergeMsg;
    MessageInfoUtil::mergeMessage(123, msgVec, 0, mergeMsg);
    if (threshold >= 0) {
        MessageInfoUtil::compressMessage(&compressor, threshold, mergeMsg);
    }
    for (size_t i = 0; i < msgVec.size(); i++) {
        delete msgVec[i];
    }
    protocol::Message msg;
    msg.set_data(mergeMsg.data);
    msg.set_uint16payload(mergeMsg.uint16Payload);
    msg.set_uint8maskpayload(mergeMsg.uint8Payload);
    msg.set_compress(mergeMsg.compress);
    msg.set_merged(mergeMsg.isMerged);
    msg.set_msgid(1234);
    msg.set_timestamp(12345);
    return msg;
}

TEST_F(MessageUtilTest, testUnpackMessage) {
    vector<protocol::Message> msgVec;
    int32_t count;
    protocol::Message msg = prepareMergeMsg();
    ASSERT_TRUE(MessageUtil::unpackMessage(
        NULL, msg.data().c_str(), msg.data().size(), msg.msgid(), msg.timestamp(), NULL, NULL, msgVec, count));
    ASSERT_EQ(2, count);
    ASSERT_EQ(2, msgVec.size());
    protocol::Message msg1 = msgVec[0];
    protocol::Message msg2 = msgVec[1];
    ASSERT_EQ(1234, msg1.msgid());
    ASSERT_EQ(12345, msg1.timestamp());
    ASSERT_EQ(3, msg1.uint16payload());
    ASSERT_EQ(2, msg1.uint8maskpayload());
    ASSERT_TRUE(!msg1.merged());
    ASSERT_TRUE(!msg1.compress());
    ASSERT_EQ(string(100, '1'), msg1.data());

    ASSERT_EQ(1234, msg2.msgid());
    ASSERT_EQ(12345, msg2.timestamp());
    ASSERT_EQ(4, msg2.uint16payload());
    ASSERT_EQ(3, msg2.uint8maskpayload());
    ASSERT_TRUE(!msg2.merged());
    ASSERT_TRUE(!msg2.compress());
    ASSERT_EQ(string(100, '2'), msg2.data());
}

TEST_F(MessageUtilTest, testUnpackMessage_2) {
    vector<protocol::Message> msgVec;
    protocol::Message msg = prepareMergeMsg();
    MessageInfo msgInfo;
    MessageConverter::decode(msg, msgInfo);
    std::vector<MessageInfo> outMsgs;

    ASSERT_TRUE(MessageUtil::unpackMessage(msgInfo, outMsgs));
    ASSERT_EQ(2, outMsgs.size());

    MessageInfo msg1 = outMsgs[0];
    MessageInfo msg2 = outMsgs[1];

    ASSERT_EQ(3, msg1.uint16Payload);
    ASSERT_EQ(2, msg1.uint8Payload);
    ASSERT_TRUE(!msg1.isMerged);
    ASSERT_TRUE(!msg1.compress);
    ASSERT_EQ(string(100, '1'), msg1.data);

    ASSERT_EQ(4, msg2.uint16Payload);
    ASSERT_EQ(3, msg2.uint8Payload);
    ASSERT_TRUE(!msg2.isMerged);
    ASSERT_TRUE(!msg2.compress);
    ASSERT_EQ(string(100, '2'), msg2.data);

    // not merge msg
    msg.set_data("3333333333");
    MessageConverter::decode(msg, msgInfo);
    ASSERT_FALSE(MessageUtil::unpackMessage(msgInfo, outMsgs));
}

TEST_F(MessageUtilTest, testUnpackMessageWithCompress) {
    vector<protocol::Message> msgVec;
    int32_t count;
    protocol::Message msg = prepareMergeMsg();
    ASSERT_TRUE(!msg.compress());
    ZlibCompressor compressor;
    ASSERT_FALSE(MessageUtil::unpackMessage(
        &compressor, msg.data().c_str(), msg.data().size(), msg.msgid(), msg.timestamp(), NULL, NULL, msgVec, count));
    msg = prepareMergeMsg(1);
    ASSERT_TRUE(msg.compress());
    ASSERT_TRUE(MessageUtil::unpackMessage(
        &compressor, msg.data().c_str(), msg.data().size(), msg.msgid(), msg.timestamp(), NULL, NULL, msgVec, count));
    ASSERT_EQ(2, count);
    ASSERT_EQ(2, msgVec.size());
    protocol::Message msg1 = msgVec[0];
    protocol::Message msg2 = msgVec[1];
    ASSERT_EQ(1234, msg1.msgid());
    ASSERT_EQ(12345, msg1.timestamp());
    ASSERT_EQ(3, msg1.uint16payload());
    ASSERT_EQ(2, msg1.uint8maskpayload());
    ASSERT_TRUE(!msg1.merged());
    ASSERT_TRUE(!msg1.compress());
    ASSERT_EQ(string(100, '1'), msg1.data());

    ASSERT_EQ(1234, msg2.msgid());
    ASSERT_EQ(12345, msg2.timestamp());
    ASSERT_EQ(4, msg2.uint16payload());
    ASSERT_EQ(3, msg2.uint8maskpayload());
    ASSERT_TRUE(!msg2.merged());
    ASSERT_TRUE(!msg2.compress());
    ASSERT_EQ(string(100, '2'), msg2.data());
}

TEST_F(MessageUtilTest, testUnpackMessageInvalidData) {
    vector<protocol::Message> msgVec;
    int32_t count = 0;
    protocol::Message msg = prepareMergeMsg();
    ASSERT_TRUE(!msg.compress());
    ASSERT_FALSE(MessageUtil::unpackMessage(
        NULL, msg.data().c_str(), 1, msg.msgid(), msg.timestamp(), NULL, NULL, msgVec, count));
    ASSERT_EQ(0, count);
    ASSERT_TRUE(msgVec.empty());
    ASSERT_FALSE(MessageUtil::unpackMessage(
        NULL, msg.data().c_str(), 3, msg.msgid(), msg.timestamp(), NULL, NULL, msgVec, count));
    ASSERT_EQ(0, count);
    ASSERT_TRUE(msgVec.empty());
    ASSERT_FALSE(MessageUtil::unpackMessage(
        NULL, msg.data().c_str(), msg.data().size() - 1, msg.msgid(), msg.timestamp(), NULL, NULL, msgVec, count));
    ASSERT_EQ(0, count);
    ASSERT_TRUE(msgVec.empty());
    ASSERT_TRUE(MessageUtil::unpackMessage(
        NULL, msg.data().c_str(), msg.data().size() + 1, msg.msgid(), msg.timestamp(), NULL, NULL, msgVec, count));
    ASSERT_EQ(2, count);
    ASSERT_EQ(2, msgVec.size());
}

TEST_F(MessageUtilTest, testUnpackMessageWithMetaFilter) {
    int32_t count;
    protocol::Message msg = prepareMergeMsg();
    {
        vector<protocol::Message> msgVec;
        Filter filter;
        filter.set_from(0);
        filter.set_to(2);
        filter::MessageFilter metaFilter(filter);
        ASSERT_TRUE(MessageUtil::unpackMessage(NULL,
                                               msg.data().c_str(),
                                               msg.data().size(),
                                               msg.msgid(),
                                               msg.timestamp(),
                                               &metaFilter,
                                               NULL,
                                               msgVec,
                                               count));

        ASSERT_EQ(2, count);
        ASSERT_EQ(0, msgVec.size());
    }
    {
        vector<protocol::Message> msgVec;
        Filter filter;
        filter.set_from(0);
        filter.set_to(3);
        filter::MessageFilter metaFilter(filter);
        ASSERT_TRUE(MessageUtil::unpackMessage(NULL,
                                               msg.data().c_str(),
                                               msg.data().size(),
                                               msg.msgid(),
                                               msg.timestamp(),
                                               &metaFilter,
                                               NULL,
                                               msgVec,
                                               count));

        ASSERT_EQ(2, count);
        ASSERT_EQ(1, msgVec.size());
        protocol::Message msg1 = msgVec[0];
        ASSERT_EQ(1234, msg1.msgid());
        ASSERT_EQ(12345, msg1.timestamp());
        ASSERT_EQ(3, msg1.uint16payload());
        ASSERT_EQ(2, msg1.uint8maskpayload());
        EXPECT_EQ(OFFSET_IN_MERGE_MSG_BASE, msg1.offsetinrawmsg());
        ASSERT_TRUE(!msg1.merged());
        ASSERT_TRUE(!msg1.compress());
        ASSERT_EQ(string(100, '1'), msg1.data());
    }
    {
        vector<protocol::Message> msgVec;
        Filter filter;
        filter.set_from(4);
        filter.set_to(6);
        filter::MessageFilter metaFilter(filter);
        ASSERT_TRUE(MessageUtil::unpackMessage(NULL,
                                               msg.data().c_str(),
                                               msg.data().size(),
                                               msg.msgid(),
                                               msg.timestamp(),
                                               &metaFilter,
                                               NULL,
                                               msgVec,
                                               count));

        ASSERT_EQ(2, count);
        ASSERT_EQ(1, msgVec.size());
        protocol::Message msg2 = msgVec[0];
        ASSERT_EQ(1234, msg2.msgid());
        ASSERT_EQ(12345, msg2.timestamp());
        ASSERT_EQ(4, msg2.uint16payload());
        ASSERT_EQ(3, msg2.uint8maskpayload());
        EXPECT_EQ(OFFSET_IN_MERGE_MSG_BASE + 1, msg2.offsetinrawmsg());
        ASSERT_TRUE(!msg2.merged());
        ASSERT_TRUE(!msg2.compress());
        ASSERT_EQ(string(100, '2'), msg2.data());
    }
}

TEST_F(MessageUtilTest, testUnpackMessageWithFieldFilter) {
    int32_t count;
    protocol::Message msg = prepareMergeMsg(-1, true);
    {
        vector<protocol::Message> msgVec;
        filter::FieldFilter fieldFilter;
        ASSERT_EQ(ERROR_NONE, fieldFilter.init(vector<string>(), "field_a IN 10"));
        ASSERT_TRUE(MessageUtil::unpackMessage(NULL,
                                               msg.data().c_str(),
                                               msg.data().size(),
                                               msg.msgid(),
                                               msg.timestamp(),
                                               NULL,
                                               &fieldFilter,
                                               msgVec,
                                               count));
        ASSERT_EQ(2, count);
        ASSERT_EQ(0, msgVec.size());
    }
    {
        vector<protocol::Message> msgVec;
        filter::FieldFilter fieldFilter;
        ASSERT_EQ(ERROR_NONE, fieldFilter.init(vector<string>(), "field_a IN 1"));
        ASSERT_TRUE(MessageUtil::unpackMessage(NULL,
                                               msg.data().c_str(),
                                               msg.data().size(),
                                               msg.msgid(),
                                               msg.timestamp(),
                                               NULL,
                                               &fieldFilter,
                                               msgVec,
                                               count));
        ASSERT_EQ(2, count);
        ASSERT_EQ(1, msgVec.size());
        protocol::Message msg1 = msgVec[0];
        ASSERT_EQ(1234, msg1.msgid());
        ASSERT_EQ(12345, msg1.timestamp());
        ASSERT_EQ(3, msg1.uint16payload());
        ASSERT_EQ(2, msg1.uint8maskpayload());
        EXPECT_EQ(OFFSET_IN_MERGE_MSG_BASE, msg1.offsetinrawmsg());

        ASSERT_TRUE(!msg1.merged());
        ASSERT_TRUE(!msg1.compress());
        ASSERT_TRUE(msg1.data().find("field_a") != string::npos);
    }
    {
        vector<protocol::Message> msgVec;
        filter::FieldFilter fieldFilter;
        ASSERT_EQ(ERROR_NONE, fieldFilter.init(vector<string>(), "field_b IN 2"));
        ASSERT_TRUE(MessageUtil::unpackMessage(NULL,
                                               msg.data().c_str(),
                                               msg.data().size(),
                                               msg.msgid(),
                                               msg.timestamp(),
                                               NULL,
                                               &fieldFilter,
                                               msgVec,
                                               count));
        ASSERT_EQ(2, count);
        ASSERT_EQ(1, msgVec.size());
        protocol::Message msg2 = msgVec[0];
        ASSERT_EQ(1234, msg2.msgid());
        ASSERT_EQ(12345, msg2.timestamp());
        ASSERT_EQ(4, msg2.uint16payload());
        ASSERT_EQ(3, msg2.uint8maskpayload());
        ASSERT_TRUE(!msg2.merged());
        ASSERT_TRUE(!msg2.compress());
        ASSERT_TRUE(msg2.data().find("field_b") != string::npos);
    }
}

TEST_F(MessageUtilTest, testRWDataHead) {
    { // invalid, msg too short
        protocol::Message msg;
        msg.set_data("aa");
        EXPECT_EQ(protocol::DATA_TYPE_UNKNOWN, msg.datatype());
        EXPECT_FALSE(MessageUtil::readDataHead(msg));
    }
    { // invalid, check not equal
        protocol::Message msg;
        msg.set_data("aaaaa");
        EXPECT_EQ(protocol::DATA_TYPE_UNKNOWN, msg.datatype());
        EXPECT_FALSE(MessageUtil::readDataHead(msg));
        EXPECT_EQ(protocol::DATA_TYPE_UNKNOWN, msg.datatype());
    }
    { // empty msg
        MessageInfo mfo;
        MessageUtil::writeDataHead(mfo);
        const char *data = mfo.data.c_str();
        SchemaHeader header;
        header.fromBufHead(data);
        string dd(sizeof(HeadMeta), 0);
        int32_t hash = static_cast<int32_t>(std::hash<string>{}(dd));
        uint32_t check = SchemaHeader::getCheckval(hash);
        EXPECT_EQ(check, header.meta.checkval);
        EXPECT_EQ(mfo.dataType, header.meta.dataType);
        EXPECT_EQ(0, header.meta.reserve);
        EXPECT_EQ(4, mfo.data.size());

        protocol::Message msg;
        msg.set_data(mfo.data);
        EXPECT_TRUE(MessageUtil::readDataHead(msg));
        EXPECT_EQ(protocol::DATA_TYPE_UNKNOWN, msg.datatype());
        EXPECT_EQ(0, msg.data().size());
    }
    { // field filter
        MessageInfo mfo;
        mfo.dataType = protocol::DATA_TYPE_FIELDFILTER;
        mfo.data = "aa";
        string dd = mfo.data;
        dd.insert(0, sizeof(HeadMeta), 0);
        int32_t hash = static_cast<int32_t>(std::hash<string>{}(dd));
        cout << hash << endl;
        MessageUtil::writeDataHead(mfo);
        EXPECT_EQ(6, mfo.data.size());
        const char *data = mfo.data.c_str();
        SchemaHeader header;
        header.fromBufHead(data);
        uint32_t check = SchemaHeader::getCheckval(hash);
        EXPECT_EQ(check, header.meta.checkval);
        EXPECT_EQ(mfo.dataType, header.meta.dataType);
        EXPECT_EQ(0, header.meta.reserve);

        protocol::Message msg;
        msg.set_data(mfo.data);
        EXPECT_TRUE(MessageUtil::readDataHead(msg));
        EXPECT_EQ(protocol::DATA_TYPE_FIELDFILTER, msg.datatype());
        EXPECT_EQ(2, msg.data().size());
        EXPECT_EQ(string("aa"), msg.data());
    }
    { // schema
        MessageInfo mfo;
        mfo.dataType = protocol::DATA_TYPE_SCHEMA;
        mfo.data = "bb";
        string dd = mfo.data;
        dd.insert(0, sizeof(HeadMeta), 0);
        int32_t hash = static_cast<int32_t>(std::hash<string>{}(dd));
        cout << hash << endl;
        MessageUtil::writeDataHead(mfo);
        EXPECT_EQ(6, mfo.data.size());
        const char *data = mfo.data.c_str();
        SchemaHeader header;
        header.fromBufHead(data);
        uint32_t check = SchemaHeader::getCheckval(hash);
        EXPECT_EQ(check, header.meta.checkval);
        EXPECT_EQ(mfo.dataType, header.meta.dataType);
        EXPECT_EQ(0, header.meta.reserve);

        protocol::Message msg;
        msg.set_data(mfo.data);
        EXPECT_TRUE(MessageUtil::readDataHead(msg));
        EXPECT_EQ(protocol::DATA_TYPE_SCHEMA, msg.datatype());
        EXPECT_EQ(6, msg.data().size());
        EXPECT_EQ(mfo.data, msg.data());
    }
}

size_t hash_c_string(const char *p, size_t s) {
    size_t result = 0;
    for (size_t i = 0; i < s; ++i) {
        result = p[i] + (result * 31);
    }
    return result;
}

TEST_F(MessageUtilTest, testHashSpeed) {
    string data(10240, 'a');
    int repeatTimes = 1000;
    uint64_t begin = TimeUtility::currentTime();
    for (int count = 0; count < repeatTimes; ++count) {
        HashAlgorithm::hashString(data.c_str(), data.size(), 0);
    }
    uint64_t pos1 = TimeUtility::currentTime();
    for (int count = 0; count < repeatTimes; ++count) {
        std::hash<string>{}(data);
    }
    uint64_t pos2 = TimeUtility::currentTime();
    for (int count = 0; count < repeatTimes; ++count) {
        const char *cdata = data.c_str();
        *(int32_t *)(cdata) = 0;
        int32_t hash = static_cast<int32_t>(std::hash<string>{}(data));
        *(int32_t *)cdata = hash;
    }
    uint64_t pos3 = TimeUtility::currentTime();
    for (int count = 0; count < repeatTimes; ++count) {
        hash_c_string(data.c_str(), data.size());
    }
    uint64_t end = TimeUtility::currentTime();
    cout << "autil hash: " << pos1 - begin << endl
         << "std hash: " << pos2 - pos1 << endl
         << "std hash set: " << pos3 - pos2 << endl
         << "self impl hash: " << end - pos3 << endl;
}

TEST_F(MessageUtilTest, testUnpackMessageOneCompressInMerge) {
    vector<protocol::Message> msgVec;
    int32_t count;
    protocol::Message msg = prepareMergeMsg(1, false, true);
    ASSERT_TRUE(msg.compress());
    ZlibCompressor compressor;
    ASSERT_TRUE(MessageUtil::unpackMessage(
        &compressor, msg.data().c_str(), msg.data().size(), msg.msgid(), msg.timestamp(), NULL, NULL, msgVec, count));
    ASSERT_EQ(2, count);
    ASSERT_EQ(2, msgVec.size());
    protocol::Message msg1 = msgVec[0];
    protocol::Message msg2 = msgVec[1];
    ASSERT_EQ(1234, msg1.msgid());
    EXPECT_EQ(12345, msg1.timestamp());
    EXPECT_EQ(3, msg1.uint16payload());
    EXPECT_EQ(2, msg1.uint8maskpayload());
    EXPECT_EQ(OFFSET_IN_MERGE_MSG_BASE, msg1.offsetinrawmsg());
    EXPECT_TRUE(!msg1.merged());
    EXPECT_TRUE(!msg1.compress());
    EXPECT_EQ(string(100, '1'), msg1.data());

    EXPECT_EQ(1234, msg2.msgid());
    EXPECT_EQ(12345, msg2.timestamp());
    EXPECT_EQ(4, msg2.uint16payload());
    EXPECT_EQ(3, msg2.uint8maskpayload());
    EXPECT_EQ(OFFSET_IN_MERGE_MSG_BASE + 1, msg2.offsetinrawmsg());
    EXPECT_TRUE(!msg2.merged());
    EXPECT_TRUE(!msg2.compress());
    EXPECT_EQ(string(100, '2'), msg2.data());
}

}; // namespace util
}; // namespace swift
