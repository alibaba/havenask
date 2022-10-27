#ifndef ISEARCH_BS_MOCKSWIFTCLIENT_H
#define ISEARCH_BS_MOCKSWIFTCLIENT_H

#include "build_service/common_define.h"
#include "build_service/util/Log.h"
#include <swift/client/SwiftClient.h>
#include <swift/client/SwiftReaderConfig.h>

namespace build_service {
namespace reader {

class MockSwiftClient : public SWIFT_NS(client)::SwiftClient
{
public:
    MockSwiftClient() {}
    MockSwiftClient(const std::string& configStr) {
        _configStr = configStr;
    }
    ~MockSwiftClient() {}
private:
    MockSwiftClient(const MockSwiftClient &);
    MockSwiftClient& operator=(const MockSwiftClient &);
public:
    MOCK_METHOD3(init, SWIFT_NS(protocol)::ErrorCode(const std::string &,
                    arpc::ANetRPCChannelManager *,
                    const SWIFT_NS(client)::SwiftClientConfig &));
    std::string getConfigStr() {
        return _configStr;
    }
private:
    std::string _configStr;
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(MockSwiftClient);
typedef ::testing::NiceMock<MockSwiftClient> NiceMockSwiftClient;
typedef ::testing::StrictMock<MockSwiftClient> StrictMockSwiftClient;

}
}

#endif //ISEARCH_BS_MOCKSWIFTCLIENT_H
