#pragma once

#include "build_service/test/test.h"
#include "build_service/test/unittest.h"
#include "build_service/util/Log.h"
#include "build_service/worker/ServiceWorker.h"

namespace build_service { namespace worker {

class MockServiceWorker : public ServiceWorker
{
public:
    MockServiceWorker() {}
    ~MockServiceWorker() {}

private:
    MOCK_METHOD1(initLeaderElector, bool(const std::string&));
    MOCK_METHOD4(initLeaderElector, bool(const std::string&, int64_t, int64_t, const std::string&));
    MOCK_CONST_METHOD0(isLeader, bool());
    MOCK_METHOD5(createServiceImpl,
                 WorkerHeartbeatExecutor*(const proto::PartitionId&, const std::string&, const std::string&,
                                          const std::string&, const std::string&));
    MOCK_CONST_METHOD2(waitToBeLeader, bool(uint32_t, uint32_t));

    LeaderElector::LeaderState getLeaderState() const override { return {true, 0}; }
};

typedef ::testing::StrictMock<MockServiceWorker> StrictMockServiceWorker;
typedef ::testing::NiceMock<MockServiceWorker> NiceMockServiceWorker;

}} // namespace build_service::worker
