#ifndef ISEARCH_BS_MOCKSWIFTCLIENT_H
#define ISEARCH_BS_MOCKSWIFTCLIENT_H

#include "build_service/common_define.h"
#include "build_service/test/unittest.h"
#include "build_service/util/Log.h"
#include "swift/client/SwiftReaderConfig.h"
#include "swift/testlib/MockSwiftClient.h"

namespace build_service { namespace reader {

class MockSwiftClient : public swift::testlib::MockSwiftClient
{
public:
    MockSwiftClient() {}
    MockSwiftClient(const std::string& configStr) { _configStr = configStr; }
    ~MockSwiftClient() {}

private:
    MockSwiftClient(const MockSwiftClient&);
    MockSwiftClient& operator=(const MockSwiftClient&);

public:
    std::string getConfigStr() { return _configStr; }

private:
    std::string _configStr;
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(MockSwiftClient);
typedef ::testing::NiceMock<MockSwiftClient> NiceMockSwiftClient;
typedef ::testing::StrictMock<MockSwiftClient> StrictMockSwiftClient;

}} // namespace build_service::reader

#endif // ISEARCH_BS_MOCKSWIFTCLIENT_H
