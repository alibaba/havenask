#ifndef ISEARCH_BS_MOCKSWIFTADMINADAPTER_H
#define ISEARCH_BS_MOCKSWIFTADMINADAPTER_H

#include "build_service/test/unittest.h"
#include "build_service/test/test.h"
#include "build_service/util/Log.h"
#include <swift/client/SwiftAdminAdapter.h>

namespace build_service {
namespace util {

class MockSwiftAdminAdapter : public SWIFT_NS(client)::SwiftAdminAdapter
{
public:
    MockSwiftAdminAdapter()
        : SWIFT_NS(client)::SwiftAdminAdapter("", SWIFT_NS(client)::SwiftRpcChannelManagerPtr())
    {}
    ~MockSwiftAdminAdapter() {}
public:
    MOCK_METHOD1(deleteTopic, swift::protocol::ErrorCode(const std::string &));
    MOCK_METHOD1(createTopic, swift::protocol::ErrorCode(const swift::protocol::TopicCreationRequest &));

private:
    BS_LOG_DECLARE();
};

typedef ::testing::StrictMock<MockSwiftAdminAdapter> StrictMockSwiftAdminAdapter;
typedef ::testing::NiceMock<MockSwiftAdminAdapter> NiceMockSwiftAdminAdapter;
}
}

#endif //ISEARCH_BS_MOCKSWIFTADMINADAPTER_H
