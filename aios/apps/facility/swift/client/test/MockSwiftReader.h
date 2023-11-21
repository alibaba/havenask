#include <cstdint>
#include <gmock/gmock-function-mocker.h>
#include <gmock/gmock-nice-strict.h>
#include <string>
#include <vector>

#include "autil/ThreadPool.h"
#include "swift/client/SwiftPartitionStatus.h"
#include "swift/client/SwiftReader.h"
#include "swift/protocol/ErrCode.pb.h"
#include "unittest/unittest.h"

namespace swift {
namespace common {
class SchemaReader;
} // namespace common
namespace protocol {
class Message;
class Messages;
class ReaderProgress;
} // namespace protocol

namespace client {
class SwiftReaderConfig;

class MockSwiftReader : public SwiftReader {
public:
    MOCK_METHOD1(init, protocol::ErrorCode(const SwiftReaderConfig &));
    MOCK_METHOD2(read, protocol::ErrorCode(protocol::Message &, int64_t));
    MOCK_METHOD2(read, protocol::ErrorCode(protocol::Messages &, int64_t));
    MOCK_METHOD3(read, protocol::ErrorCode(int64_t &, protocol::Message &, int64_t));
    MOCK_METHOD3(read, protocol::ErrorCode(int64_t &, protocol::Messages &, int64_t));
    MOCK_METHOD1(seekByMessageId, protocol::ErrorCode(int64_t));
    MOCK_METHOD2(seekByTimestamp, protocol::ErrorCode(int64_t, bool));
    MOCK_METHOD2(seekByProgress, protocol::ErrorCode(const protocol::ReaderProgress &, bool));
    MOCK_CONST_METHOD2(checkCurrentError, void(protocol::ErrorCode &, std::string &));
    MOCK_METHOD0(getSwiftPartitionStatus, SwiftPartitionStatus());
    MOCK_METHOD1(setRequiredFieldNames, void(const std::vector<std::string> &));
    MOCK_METHOD1(setFieldFilterDesc, void(const std::string &));
    MOCK_METHOD2(setTimestampLimit, void(int64_t, int64_t &));
    MOCK_METHOD0(getRequiredFieldNames, std::vector<std::string>());
    MOCK_METHOD1(updateCommittedCheckpoint, bool(int64_t));
    MOCK_METHOD1(setDecompressThreadPool, void(const autil::ThreadPoolPtr &));
    MOCK_METHOD2(getSchemaReader, common::SchemaReader *(const char *, protocol::ErrorCode &));
    MOCK_CONST_METHOD1(getReaderProgress, protocol::ErrorCode(protocol::ReaderProgress &));
    MOCK_CONST_METHOD0(getCheckpointTimestamp, int64_t());
};
using StringMockSwiftReader = ::testing::StrictMock<MockSwiftReader>;
using NiceMockSwiftReader = ::testing::NiceMock<MockSwiftReader>;

} // namespace client
} // namespace swift
