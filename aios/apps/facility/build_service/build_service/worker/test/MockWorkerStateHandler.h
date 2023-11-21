#pragma once

#include "build_service/proto/ErrorCollector.h"
#include "build_service/test/test.h"
#include "build_service/test/unittest.h"
#include "build_service/util/Log.h"
#include "build_service/worker/WorkerStateHandler.h"

namespace build_service { namespace worker {

class MockWorkerStateHandler : public WorkerStateHandler
{
public:
    MockWorkerStateHandler(const std::string& zkPath, const std::string& adminServiceName)
        : WorkerStateHandler(proto::PartitionId(), NULL, zkPath, adminServiceName, "")
    {
        ON_CALL(*this, hasFatalError()).WillByDefault(Return(false));
    }
    ~MockWorkerStateHandler() {}

    void collectErrorInfos(const std::vector<proto::ErrorInfo>& errorInfos, std::vector<proto::ErrorInfo>* output)
    {
        proto::ErrorCollector collector;
        collector.addErrorInfos(errorInfos);
        fillErrorInfo(&collector);

        output->assign(_errorInfos.begin(), _errorInfos.end());
    }

public:
    MOCK_METHOD2(doHandleTargetState, void(const std::string&, bool hasResourceUpdated));
    MOCK_METHOD1(getCurrentState, void(std::string&));
    MOCK_METHOD0(hasFatalError, bool());
    MOCK_METHOD1(needSuspendTask, bool(const std::string&));
};

typedef ::testing::StrictMock<MockWorkerStateHandler> StrictMockWorkerStateHandler;
typedef ::testing::NiceMock<MockWorkerStateHandler> NiceMockWorkerStateHandler;

}} // namespace build_service::worker
