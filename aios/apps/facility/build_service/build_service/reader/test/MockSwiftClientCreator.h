#pragma once

#include <functional>

#include "build_service/common_define.h"
#include "build_service/reader/test/MockSwiftClient.h"
#include "build_service/util/Log.h"
#include "build_service/util/SwiftClientCreator.h"
#include "swift/client/SwiftClient.h"
#include "swift/client/SwiftReaderConfig.h"

namespace build_service { namespace reader {
class MockSwiftClientCreator : public util::SwiftClientCreator
{
public:
    MockSwiftClientCreator()
    {
        ON_CALL(*this, doCreateSwiftClient(_, _))
            .WillByDefault(Invoke(std::bind(MockSwiftClientCreator::createMockClient)));
    }
    ~MockSwiftClientCreator() {}

private:
    MOCK_METHOD2(doCreateSwiftClient, swift::client::SwiftClient*(const std::string&, const std::string&));

public:
    static swift::client::SwiftClient* createMockClient() { return new NiceMockSwiftClient; }
    static swift::client::SwiftClient* createMockClientWithConfig(const std::string& zkPath,
                                                                  const std::string& clientConfig)
    {
        std::string config = "zkPath=" + zkPath;
        if (!clientConfig.empty()) {
            config += ";" + clientConfig;
        }
        return new NiceMockSwiftClient(config);
    }
};

BS_TYPEDEF_PTR(MockSwiftClientCreator);
typedef ::testing::NiceMock<MockSwiftClientCreator> NiceMockSwiftClientCreator;
typedef ::testing::StrictMock<MockSwiftClientCreator> StrictMockSwiftClientCreator;

}} // namespace build_service::reader
