#pragma once

#include "build_service/test/test.h"
#include "build_service/test/unittest.h"
#include "build_service/util/Log.h"
#include "build_service/worker/WorkerHeartbeatExecutor.h"

namespace build_service { namespace worker {

class MockWorkerHeartbeatExecutor : public WorkerHeartbeatExecutor
{
public:
    MockWorkerHeartbeatExecutor(proto::WorkerNodeBase* workerNode, WorkerStateHandler* handler,
                                WorkerHeartbeat* heartbeat)
        : WorkerHeartbeatExecutor(workerNode, handler, heartbeat)
        , _exitFlag(false)
    {
    }

    MockWorkerHeartbeatExecutor() : MockWorkerHeartbeatExecutor(NULL, NULL, NULL) {}

    ~MockWorkerHeartbeatExecutor()
    {
        // avoid _loopThreadOPtr invoke workLoop after mock class destory, which caused "pure virtual method called"
        _loopThreadPtr.reset();
        std::cout << "Destructor for MockWorkerHeartbeatExecutor" << std::endl;
    }

public:
    MOCK_METHOD0(start, bool());

protected:
    MOCK_METHOD0(workLoop, void());
    void exit(const std::string& message) { _exitFlag = true; };

private:
    bool _exitFlag;
};
typedef ::testing::NiceMock<MockWorkerHeartbeatExecutor> NiceMockWorkerHeartbeatExecutor;
typedef ::testing::StrictMock<MockWorkerHeartbeatExecutor> StrictMockWorkerHeartbeatExecutor;
}} // namespace build_service::worker
