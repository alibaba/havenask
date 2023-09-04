#pragma once

#include <cstdint>
#include <functional>
#include <gmock/gmock-function-mocker.h>
#include <string>
#include <utility>
#include <vector>

#include "swift/client/MessageInfo.h"
#include "swift/client/SwiftWriter.h"
#include "swift/protocol/ErrCode.pb.h"
#include "unittest/unittest.h"

namespace swift {
namespace common {
class SchemaWriter;
} // namespace common

namespace testlib {

class MockSwiftWriter : public swift::client::SwiftWriter {
public:
    MOCK_METHOD1(write, swift::protocol::ErrorCode(swift::client::MessageInfo &));
    MOCK_METHOD3(getPartitionIdByHashStr,
                 std::pair<int32_t, uint16_t>(const std::string &, const std::string &, const std::string &));
    MOCK_METHOD1(getPartitionIdByHashStr, std::pair<int32_t, uint16_t>(const std::string &));
    MOCK_METHOD3(getWriterMetrics,
                 void(const std::string &zkPath,
                      const std::string &topicName,
                      swift::client::WriterMetrics &writerMetrics));
    MOCK_METHOD2(clearBuffer, bool(int64_t &cpBeg, int64_t &cpEnd));
    MOCK_METHOD0(isFinished, bool());
    MOCK_METHOD0(isAllSent, bool());
    MOCK_METHOD0(getTopicName, std::string());
    MOCK_METHOD0(getSchemaWriter, swift::common::SchemaWriter *());
    MOCK_METHOD1(waitFinished, swift::protocol::ErrorCode(int64_t));
    MOCK_METHOD1(setCommittedCallBack,
                 void(const std::function<void(const std::vector<std::pair<int64_t, int64_t>> &)> &));
    MOCK_METHOD1(setErrorCallBack, void(const std::function<void(const protocol::ErrorCode &)> &));
    MOCK_METHOD4(updateWriteVersion, swift::protocol::ErrorCode(uint16_t, uint16_t, int64_t, int64_t));
    MOCK_CONST_METHOD0(getCommittedCheckpointId, int64_t());
    MOCK_CONST_METHOD1(getLastRefreshTime, int64_t(uint32_t));
    MOCK_CONST_METHOD0(isSyncWriter, bool());
};

} // namespace testlib
} // namespace swift
