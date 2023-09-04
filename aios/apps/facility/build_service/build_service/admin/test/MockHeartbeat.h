#ifndef ISEARCH_BS_MOCKHEARTBEAT_H
#define ISEARCH_BS_MOCKHEARTBEAT_H

#include "build_service/admin/Heartbeat.h"
#include "build_service/test/test.h"
#include "build_service/test/unittest.h"
#include "build_service/util/Log.h"

namespace build_service { namespace admin {

class MockHeartbeat : public Heartbeat
{
public:
    MockHeartbeat() {}
    ~MockHeartbeat() {}

protected:
    MOCK_METHOD0(syncStatus, void());
};

typedef ::testing::StrictMock<MockHeartbeat> StrictMockHeartbeat;
typedef ::testing::NiceMock<MockHeartbeat> NiceMockHeartbeat;

}} // namespace build_service::admin

#endif // ISEARCH_BS_MOCKHEARTBEAT_H
