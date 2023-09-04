#ifndef ISEARCH_BS_MOCKWORKERHEARTBEAT_H
#define ISEARCH_BS_MOCKWORKERHEARTBEAT_H

#include "build_service/test/test.h"
#include "build_service/test/unittest.h"
#include "build_service/util/Log.h"
#include "build_service/worker/WorkerHeartbeat.h"

namespace build_service { namespace worker {

class MockWorkerHeartbeat : public WorkerHeartbeat
{
public:
    MockWorkerHeartbeat() : WorkerHeartbeat(NULL) {}
    ~MockWorkerHeartbeat() {}

public:
    MOCK_METHOD0(start, bool());
    MOCK_METHOD0(stop, void());
};

typedef ::testing::StrictMock<MockWorkerHeartbeat> StrictMockWorkerHeartbeat;
typedef ::testing::NiceMock<MockWorkerHeartbeat> NiceMockWorkerHeartbeat;

}} // namespace build_service::worker

#endif // ISEARCH_BS_MOCKWORKERHEARTBEAT_H
