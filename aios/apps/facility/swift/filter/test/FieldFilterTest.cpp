#include "swift/filter/FieldFilter.h"

#include <cstddef>
#include <stdint.h>
#include <string>

#include "autil/Span.h"
#include "flatbuffers/flatbuffers.h"
#include "swift/common/Common.h"
#include "swift/common/FieldGroupReader.h"
#include "swift/common/FieldGroupWriter.h"
#include "swift/protocol/BrokerRequestResponse.pb.h"
#include "swift/protocol/Common.pb.h"
#include "swift/protocol/ErrCode.pb.h"
#include "swift/protocol/FBMessageReader.h"
#include "swift/protocol/FBMessageWriter.h"
#include "swift/protocol/SwiftMessage.pb.h"
#include "swift/protocol/SwiftMessage_generated.h"
#include "unittest/unittest.h"

using namespace std;
using namespace swift::common;
using namespace swift::protocol;

namespace swift {
namespace filter {

class FieldFilterTest : public TESTBASE {};

TEST_F(FieldFilterTest, testInit) {
    {
        FieldFilter filter;
        ConsumptionRequest request;
        EXPECT_EQ(ERROR_NONE, filter.init(&request));
    }
    {
        FieldFilter filter;
        ConsumptionRequest request;
        request.set_fieldfilterdesc("xxxx");
        EXPECT_EQ(ERROR_BROKER_INIT_FIELD_FILTER_FAILD, filter.init(&request));
    }
    {
        FieldFilter filter;
        ConsumptionRequest request;
        request.set_fieldfilterdesc("A IN B AND C IN D");
        request.add_requiredfieldnames("field1");
        request.add_requiredfieldnames("field2");
        EXPECT_EQ(ERROR_NONE, filter.init(&request));
        EXPECT_TRUE(filter._descFilter);
        EXPECT_TRUE(filter._elimFilter);
    }
}

TEST_F(FieldFilterTest, testFilter) {
    FieldGroupWriter fieldGroupWriter;
    fieldGroupWriter.addProductionField("body", "body1", true);
    fieldGroupWriter.addProductionField("title", "title1", true);
    string rawData = fieldGroupWriter.toString();

    {
        FieldFilter filter;
        ConsumptionRequest request;
        request.set_fieldfilterdesc("body IN body1 AND title IN title1");
        request.add_requiredfieldnames("body");
        EXPECT_EQ(ERROR_NONE, filter.init(&request));
        string output;
        EXPECT_TRUE(filter.filter(rawData, output));

        FieldGroupReader fieldGroupReader;
        fieldGroupReader.fromConsumptionString(output);
        EXPECT_EQ((size_t)1, fieldGroupReader.getFieldSize());
        const Field *field = fieldGroupReader.getField(0);
        EXPECT_EQ(string("body1"), field->value.to_string());
        EXPECT_TRUE(field->name.empty());
        EXPECT_TRUE(field->isUpdated);
    }
    {
        FieldFilter filter;
        ConsumptionRequest request;
        request.set_fieldfilterdesc("body IN body1 AND title IN title1");
        EXPECT_EQ(ERROR_NONE, filter.init(&request));
        string output;
        EXPECT_TRUE(filter.filter(rawData, output));

        FieldGroupReader fieldGroupReader;
        fieldGroupReader.fromProductionString(output);
        EXPECT_EQ((size_t)2, fieldGroupReader.getFieldSize());
        const Field *field = fieldGroupReader.getField(0);
        EXPECT_FALSE(field->name.empty());
        EXPECT_EQ(string("body1"), field->value.to_string());
        EXPECT_TRUE(field->isUpdated);

        field = fieldGroupReader.getField(1);
        EXPECT_FALSE(field->name.empty());
        EXPECT_EQ(string("title1"), field->value.to_string());
        EXPECT_TRUE(field->isUpdated);
    }
    {
        FieldFilter filter;
        ConsumptionRequest request;
        request.add_requiredfieldnames("body");
        request.add_requiredfieldnames("not_exist");
        EXPECT_EQ(ERROR_NONE, filter.init(&request));
        string output;
        EXPECT_TRUE(filter.filter(rawData, output));

        FieldGroupReader fieldGroupReader;
        fieldGroupReader.fromConsumptionString(output);
        EXPECT_EQ((size_t)2, fieldGroupReader.getFieldSize());
        const Field *field = fieldGroupReader.getField(0);
        EXPECT_TRUE(field->name.empty());
        EXPECT_EQ(string("body1"), field->value.to_string());
        EXPECT_TRUE(field->isUpdated);
        field = fieldGroupReader.getField(1);
        EXPECT_TRUE(field->name.empty());
        EXPECT_EQ(string(""), field->value.to_string());
        EXPECT_FALSE(field->isUpdated);
    }
    {
        FieldFilter filter;
        ConsumptionRequest request;
        int fieldNum = 102400;
        for (int i = 0; i < fieldNum / 2; i++) {
            request.add_requiredfieldnames("body");
            request.add_requiredfieldnames("not_exist");
        }
        EXPECT_EQ(ERROR_NONE, filter.init(&request));
        string output;
        EXPECT_TRUE(filter.filter(rawData, output));
        FieldGroupReader fieldGroupReader;
        fieldGroupReader.fromConsumptionString(output);
        EXPECT_EQ((size_t)fieldNum, fieldGroupReader.getFieldSize());
        for (int i = 0; i < fieldNum - 2; i++) {
            const Field *field = fieldGroupReader.getField(i);
            EXPECT_TRUE(field->name.empty());
            EXPECT_EQ(string(""), field->value.to_string());
            EXPECT_FALSE(field->isUpdated);
        }
        const Field *field = fieldGroupReader.getField(fieldNum - 2);
        EXPECT_TRUE(field->name.empty());
        EXPECT_EQ(string("body1"), field->value.to_string());
        EXPECT_TRUE(field->isUpdated);
        field = fieldGroupReader.getField(fieldNum - 1);
        EXPECT_TRUE(field->name.empty());
        EXPECT_EQ(string(""), field->value.to_string());
        EXPECT_FALSE(field->isUpdated);
    }
    {
        FieldFilter filter;
        ConsumptionRequest request;
        EXPECT_EQ(ERROR_NONE, filter.init(&request));
        string output;
        EXPECT_TRUE(filter.filter(rawData, output));

        FieldGroupReader fieldGroupReader;
        fieldGroupReader.fromConsumptionString(output);
        EXPECT_EQ((size_t)0, fieldGroupReader.getFieldSize());
    }
}

TEST_F(FieldFilterTest, testFilterResponse_PB) {
    FieldGroupWriter fieldGroupWriter;
    fieldGroupWriter.addProductionField("body", "body1", true);
    string rawData1 = fieldGroupWriter.toString();
    fieldGroupWriter.reset();
    fieldGroupWriter.addProductionField("body", "body2", true);
    string rawData2 = fieldGroupWriter.toString();
    string mergeData = "aaaa";
    MessageResponse response;
    for (int i = 0; i < 9; i++) {
        Message *msg = response.add_msgs();
        if (i % 3 == 0) {
            msg->set_data(rawData1);
        } else if (i % 3 == 1) {
            msg->set_data(rawData2);
        } else {
            msg->set_data(mergeData);
            msg->set_merged(true);
        }
    }
    {
        FieldFilter filter;
        ConsumptionRequest request;
        request.set_fieldfilterdesc("body IN body1");
        EXPECT_EQ(ERROR_NONE, filter.init(&request));
        string output;
        int64_t totalSize = 0;
        int64_t rawTotalSize = 0;
        int64_t readMsgCount = 0;
        int64_t updateMsgCount = 0;
        filter.filter(&response, totalSize, rawTotalSize, readMsgCount, updateMsgCount);
        ASSERT_EQ(6, response.msgs_size());
        for (int32_t i = 0; i < response.msgs_size(); i++) {
            if (i % 2 == 0) {
                ASSERT_EQ(rawData1, response.msgs(i).data());
            } else {
                ASSERT_EQ(mergeData, response.msgs(i).data());
            }
        }
        EXPECT_EQ(48, totalSize);
        EXPECT_EQ(84, rawTotalSize);
        EXPECT_EQ(9, readMsgCount);
        EXPECT_EQ(6, updateMsgCount);
    }
}

TEST_F(FieldFilterTest, testFilterResponse_FB) {
    FieldGroupWriter fieldGroupWriter;
    fieldGroupWriter.addProductionField("body", "body1", true);
    string rawData1 = fieldGroupWriter.toString();
    fieldGroupWriter.reset();
    fieldGroupWriter.addProductionField("body", "body2", true);
    string rawData2 = fieldGroupWriter.toString();
    string mergeData = "aaaa";
    MessageResponse response;
    FBMessageWriter writer;
    for (int i = 0; i < 9; i++) {
        if (i % 3 == 0) {
            writer.addMessage(rawData1);
        } else if (i % 3 == 1) {
            writer.addMessage(rawData2);
        } else {
            writer.addMessage(mergeData, 0, 0, 0, 0, false, true);
        }
    }
    response.set_fbmsgs(writer.data(), writer.size());
    response.set_messageformat(MF_FB);
    {
        FieldFilter filter;
        ConsumptionRequest request;
        request.set_fieldfilterdesc("body IN body1");
        request.mutable_versioninfo()->set_supportfb(true);
        EXPECT_EQ(ERROR_NONE, filter.init(&request));
        string output;
        int64_t totalSize = 0;
        int64_t rawTotalSize = 0;
        int64_t readMsgCount = 0;
        int64_t updateMsgCount = 0;
        filter.filter(&response, totalSize, rawTotalSize, readMsgCount, updateMsgCount);
        FBMessageReader reader;
        reader.init(response.fbmsgs());
        ASSERT_EQ(6, reader.size());
        for (uint32_t i = 0; i < reader.size(); i++) {
            if (i % 2 == 0) {
                ASSERT_EQ(rawData1, reader.read(i)->data()->str());
            } else {
                ASSERT_EQ(mergeData, reader.read(i)->data()->str());
            }
        }
        EXPECT_EQ(48, totalSize);
        EXPECT_EQ(84, rawTotalSize);
        EXPECT_EQ(9, readMsgCount);
        EXPECT_EQ(6, updateMsgCount);
    }
}

} // namespace filter
} // namespace swift
