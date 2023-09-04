#pragma once

#include "swift/client/SwiftClient.h"

namespace swift {
namespace testlib {

class MockSwiftClient : public swift::client::SwiftClient {
public:
    MOCK_METHOD1(init, swift::protocol::ErrorCode(const swift::client::SwiftClientConfig &config));
    MOCK_METHOD2(createWriter,
                 swift::client::SwiftWriter *(const std::string &writerConfigStr,
                                              swift::protocol::ErrorInfo *errorCode));
    MOCK_METHOD2(createReader,
                 swift::client::SwiftReader *(const std::string &readerConfigStr,
                                              swift::protocol::ErrorInfo *errorCode));
    MOCK_METHOD3(init,
                 swift::protocol::ErrorCode(const std::string &,
                                            arpc::ANetRPCChannelManager *,
                                            const swift::client::SwiftClientConfig &));
};

} // namespace testlib
} // namespace swift
