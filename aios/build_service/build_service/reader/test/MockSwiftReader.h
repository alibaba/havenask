#ifndef ISEARCH_BS_MOCKSWIFTREADER_H
#define ISEARCH_BS_MOCKSWIFTREADER_H

#include "build_service/common_define.h"
#include "build_service/util/Log.h"
#include "build_service/test/unittest.h"
#include <swift/client/SwiftReader.h>
#include <deque>

namespace build_service {
namespace reader {

class MockSwiftReader : public SWIFT_NS(client)::SwiftReader
{
public:
    typedef SWIFT_NS(protocol)::ErrorCode ErrorCode;
    typedef SWIFT_NS(protocol)::Message Message;
    typedef SWIFT_NS(protocol)::Messages Messages;    
public:
    MockSwiftReader();
    ~MockSwiftReader();
private:
    MockSwiftReader(const MockSwiftReader &);
    MockSwiftReader& operator=(const MockSwiftReader &);
public:
    MOCK_METHOD1(init, ErrorCode(const SWIFT_NS(client)::SwiftReaderConfig&));
    MOCK_METHOD2(read, ErrorCode(Message &, int64_t)); // 3s
    MOCK_METHOD1(seekByMessageId, ErrorCode(int64_t));
    MOCK_METHOD3(read, ErrorCode(int64_t &, Message &, int64_t));

    MOCK_METHOD3(readMulti, ErrorCode(int64_t &, Messages &, int64_t));
    ErrorCode read(int64_t &timestamp, Messages &msgs, int64_t timeout) override { return readMulti(timestamp, msgs, timeout); }

    MOCK_METHOD2(seekByTimestamp, ErrorCode(int64_t, bool));
    MOCK_CONST_METHOD2(checkCurrentError, void(ErrorCode &, std::string &));
    MOCK_METHOD0(getSwiftPartitionStatus, SWIFT_NS(client)::SwiftPartitionStatus());
    MOCK_METHOD1(setRequiredFieldNames, void(const std::vector<std::string>&));
    MOCK_METHOD0(getRequiredFieldNames, std::vector<std::string>());
    MOCK_METHOD1(setFieldFilterDesc, void(const std::string &));
    MOCK_METHOD2(setTimestampLimit, void(int64_t, int64_t&));
    MOCK_METHOD1(updateCommittedCheckpoint, bool(int64_t));
    
public:
    void addMessage(const std::string &messageData,
                    int64_t timestamp,
                    int64_t retTimestamp = -1);
    void setNoMoreMessageAtTimestamp(int64_t timestamp);
private:
    ErrorCode readForTest(int64_t &minTimestamp, Message &message, int64_t timeout);
    ErrorCode readMultiForTest(int64_t &timestamp, Messages &messages, int64_t timeout);
    ErrorCode seekByTimestampForTest(int64_t timestamp, bool force);
    void setTimestampLimitForTest(int64_t timestamp, int64_t &actualTimestamp);
    SWIFT_NS(client)::SwiftPartitionStatus getSwiftPartitionStatusForTest() const;

private:
    typedef std::pair<Message, int64_t> MessageInfo;
    size_t _cursor;
    std::vector<MessageInfo> _messages;
    int64_t _timestampNow;
    int64_t _timestampLimit;
    int64_t _noMoreMessageTimestamp;
private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(MockSwiftReader);
typedef ::testing::NiceMock<MockSwiftReader> NiceMockSwiftReader;
typedef ::testing::StrictMock<MockSwiftReader> StrictMockSwiftReader;


}
}

#endif //ISEARCH_BS_MOCKSWIFTREADER_H
