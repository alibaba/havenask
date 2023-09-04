#pragma once

#include <cstdint>
#include <gmock/gmock-function-mocker.h>
#include <gmock/gmock-nice-strict.h>
#include <stddef.h>
#include <string>
#include <utility>
#include <vector>

#include "autil/Log.h"
#include "swift/client/SwiftPartitionStatus.h"
#include "swift/client/SwiftReader.h"
#include "swift/common/Common.h"
#include "swift/protocol/ErrCode.pb.h"
#include "swift/protocol/SwiftMessage.pb.h"
#include "unittest/unittest.h"

namespace swift {
namespace client {
class SwiftReaderConfig;
} // namespace client
namespace common {
class SchemaReader;
} // namespace common

namespace testlib {

class MockSwiftReader : public client::SwiftReader {
public:
    typedef protocol::ErrorCode ErrorCode;
    typedef protocol::Message Message;
    typedef protocol::Messages Messages;
    typedef protocol::DataType DataType;

public:
    MockSwiftReader();
    ~MockSwiftReader();

private:
    MockSwiftReader(const MockSwiftReader &);
    MockSwiftReader &operator=(const MockSwiftReader &);

public:
    MOCK_METHOD1(init, ErrorCode(const client::SwiftReaderConfig &));
    MOCK_METHOD2(read, ErrorCode(Message &, int64_t)); // 3s
    MOCK_METHOD1(seekByMessageId, ErrorCode(int64_t));
    MOCK_METHOD3(read, ErrorCode(int64_t &, Message &, int64_t));

    MOCK_METHOD3(readMulti, ErrorCode(int64_t &, Messages &, int64_t));
    ErrorCode read(int64_t &timestamp, Messages &msgs, int64_t timeout) override {
        return readMulti(timestamp, msgs, timeout);
    }

    MOCK_METHOD2(seekByTimestamp, ErrorCode(int64_t, bool));
    MOCK_CONST_METHOD2(checkCurrentError, void(ErrorCode &, std::string &));
    MOCK_METHOD0(getSwiftPartitionStatus, client::SwiftPartitionStatus());
    MOCK_METHOD1(setRequiredFieldNames, void(const std::vector<std::string> &));
    MOCK_METHOD0(getRequiredFieldNames, std::vector<std::string>());
    MOCK_METHOD1(setFieldFilterDesc, void(const std::string &));
    MOCK_METHOD2(setTimestampLimit, void(int64_t, int64_t &));
    MOCK_METHOD1(updateCommittedCheckpoint, bool(int64_t));
    MOCK_METHOD2(getSchemaReader, common::SchemaReader *(const char *, ErrorCode &ec));
    MOCK_CONST_METHOD1(getReaderProgress, protocol::ErrorCode(protocol::ReaderProgress &progress));
    MOCK_METHOD2(seekByProgress, protocol::ErrorCode(const protocol::ReaderProgress &progress, bool force));
    MOCK_CONST_METHOD0(getCheckpointTimestamp, int64_t());

public:
    swift::protocol::ReaderProgress createProgress(int64_t timestamp);
    void init(const std::string topicName, uint16_t from, uint16_t to, int8_t uint8FilterMask, int8_t uint8MaskResult);
    void addMessage(const std::string &messageData,
                    int64_t timestamp,
                    int64_t retTimestamp = -1,
                    DataType type = protocol::DATA_TYPE_UNKNOWN,
                    uint16_t msgU16payload = 0,
                    uint32_t offsetInRawMsg = 0);

    void setNoMoreMessageAtTimestamp(int64_t timestamp);

    void setSchemaReader(common::SchemaReader *reader, int32_t maxSchemaDocCount) {
        _schemaReader = reader;
        _maxSchemaDocCount = maxSchemaDocCount;
    }

private:
    ErrorCode readForTest(int64_t &minTimestamp, Message &message, int64_t timeout);
    ErrorCode readMultiForTest(int64_t &timestamp, Messages &messages, int64_t timeout);
    ErrorCode seekByTimestampForTest(int64_t timestamp, bool force);
    void setTimestampLimitForTest(int64_t timestamp, int64_t &actualTimestamp);
    common::SchemaReader *getSchemaReaderForTest(const char *docStr, ErrorCode &ec);
    client::SwiftPartitionStatus getSwiftPartitionStatusForTest() const;
    ErrorCode seekByProgressForTest(const protocol::ReaderProgress &progress, bool force);
    ErrorCode getReaderProgressForTest(protocol::ReaderProgress &progress);
    bool isEqual(const protocol::ReaderProgress &progress1, const protocol::ReaderProgress &progress2);
    ErrorCode initForTest(const client::SwiftReaderConfig &config);

private:
    typedef std::pair<Message, int64_t> MessageInfo;
    size_t _cursor;
    std::vector<MessageInfo> _messages;
    std::vector<protocol::ReaderProgress> _progress;
    int64_t _timestampNow;
    int64_t _timestampLimit;
    int64_t _noMoreMessageTimestamp;
    std::string _topicName;
    uint16_t _from = 0;
    uint16_t _to = 65535;
    uint8_t _uint8FilterMask = 0;
    uint8_t _uint8MaskResult = 0;

    common::SchemaReader *_schemaReader;
    size_t _maxSchemaDocCount;

private:
    AUTIL_LOG_DECLARE();
};

SWIFT_TYPEDEF_PTR(MockSwiftReader);
typedef ::testing::NiceMock<MockSwiftReader> NiceMockSwiftReader;
typedef ::testing::StrictMock<MockSwiftReader> StrictMockSwiftReader;

} // namespace testlib
} // namespace swift
