#pragma once

#include "swift/network/SwiftAdminAdapter.h"

namespace swift {
namespace testlib {

class MockSwiftAdminAdapter : public swift::network::SwiftAdminAdapter {
public:
    MockSwiftAdminAdapter() : swift::network::SwiftAdminAdapter("", swift::network::SwiftRpcChannelManagerPtr()) {}
    ~MockSwiftAdminAdapter() {}

public:
    MOCK_METHOD1(deleteTopic, swift::protocol::ErrorCode(const std::string &));
    MOCK_METHOD1(createTopic, swift::protocol::ErrorCode(swift::protocol::TopicCreationRequest &));
    MOCK_METHOD1(updateWriterVersion, swift::protocol::ErrorCode(const swift::protocol::TopicWriterVersionInfo &));
    MOCK_METHOD2(modifyTopic,
                 swift::protocol::ErrorCode(protocol::TopicCreationRequest &request,
                                            protocol::TopicCreationResponse &response));
};

typedef ::testing::StrictMock<MockSwiftAdminAdapter> StrictMockSwiftAdminAdapter;
typedef ::testing::NiceMock<MockSwiftAdminAdapter> NiceMockSwiftAdminAdapter;

} // namespace testlib
} // namespace swift
