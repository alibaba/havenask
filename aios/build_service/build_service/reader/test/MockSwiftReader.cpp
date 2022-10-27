#include "build_service/reader/test/MockSwiftReader.h"
#include <functional>

using namespace std;
SWIFT_USE_NAMESPACE(protocol);

namespace build_service {
namespace reader {

BS_LOG_SETUP(reader, MockSwiftReader);

MockSwiftReader::MockSwiftReader()
    : _cursor(0)
    , _timestampLimit(-1)
    , _noMoreMessageTimestamp(-1)
{
    ON_CALL(*this, read(_, _, _)).WillByDefault(
            Invoke(std::bind(&MockSwiftReader::readForTest, this, std::placeholders::_1, std::placeholders::_2, std::placeholders::_3)));
    ON_CALL(*this, readMulti(_, _, _)).WillByDefault(
            Invoke(std::bind(&MockSwiftReader::readMultiForTest, this, std::placeholders::_1, std::placeholders::_2, std::placeholders::_3)));    
    ON_CALL(*this, seekByTimestamp(_, _)).WillByDefault(
            Invoke(std::bind(&MockSwiftReader::seekByTimestampForTest, this, std::placeholders::_1, std::placeholders::_2)));
    ON_CALL(*this, setTimestampLimit(_, _)).WillByDefault(
            Invoke(std::bind(&MockSwiftReader::setTimestampLimitForTest, this, std::placeholders::_1, std::placeholders::_2)));
    ON_CALL(*this, getSwiftPartitionStatus()).WillByDefault(
            Invoke(std::bind(&MockSwiftReader::getSwiftPartitionStatusForTest, this)));
}

MockSwiftReader::~MockSwiftReader() {
}

ErrorCode MockSwiftReader::readForTest(
        int64_t &timestamp, Message &message, int64_t timeout)
{
    usleep(200 * 1000);
    if (_cursor >= _messages.size()) {
        return ERROR_CLIENT_NO_MORE_MESSAGE;
    }
    if (_timestampLimit != -1 && _messages[_cursor].second > _timestampLimit) {
        return ERROR_CLIENT_EXCEED_TIME_STAMP_LIMIT;
    }
    MessageInfo &messageInfo = _messages[_cursor++];
    message = messageInfo.first;
    timestamp = messageInfo.second;
    return ERROR_NONE;
}
ErrorCode MockSwiftReader::readMultiForTest(
        int64_t &timestamp, Messages &messages, int64_t timeout)
{
    usleep(200 * 1000);
    messages.clear_msgs();
    if (_cursor >= _messages.size()) {
        return ERROR_CLIENT_NO_MORE_MESSAGE;
    }
    for (; _cursor < _messages.size(); ++_cursor) {
        if (_timestampLimit != -1 && _messages[_cursor].second > _timestampLimit) {
            break;
        }
        messages.add_msgs()->Swap(&_messages[_cursor].first);
        timestamp = _messages[_cursor].second;
    }
    if (messages.msgs_size() == 0) {
        return ERROR_CLIENT_NO_MORE_MESSAGE;
    }
    return ERROR_NONE;
}    
    

ErrorCode MockSwiftReader::seekByTimestampForTest(int64_t timestamp, bool force) {
    if ( _cursor >= _messages.size()) {
        return ERROR_CLIENT_NO_MORE_MESSAGE;
    }
    size_t i = force ? 0 : _cursor;
    for (; i < _messages.size(); i++) {
        if (timestamp <= _messages[i].first.timestamp()) {
            break;
        }
    }
    _cursor = i;
    return ERROR_NONE;
}

void MockSwiftReader::setTimestampLimitForTest(int64_t timestamp, int64_t &actualTimestamp) {
    _timestampLimit = timestamp;
    // if (!_messages.empty() &_messages.back().second > timestamp) {
    //     _timestampLimit = _messages.back().second;
    // }
    actualTimestamp = _timestampLimit;
}

void MockSwiftReader::addMessage(
        const string &messageData, int64_t timestamp, int64_t retTimestamp)
{
    if (retTimestamp == -1) {
        retTimestamp = timestamp;
    }
    Message message;
    message.set_data(messageData);
    message.set_timestamp(timestamp);
    _messages.push_back(make_pair(message, retTimestamp));
}

void MockSwiftReader::setNoMoreMessageAtTimestamp(int64_t timestamp) {
    _noMoreMessageTimestamp = timestamp;
}

SWIFT_NS(client)::SwiftPartitionStatus MockSwiftReader::getSwiftPartitionStatusForTest() const {
    SWIFT_NS(client)::SwiftPartitionStatus status;
    status.refreshTime = 0;
    status.maxMessageId = 0;
    status.maxMessageTimestamp = 0;
    return status;
}

}
}
