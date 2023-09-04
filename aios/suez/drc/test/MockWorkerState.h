#pragma once

#include "unittest/unittest.h"
#include "worker_framework/WorkerState.h"

namespace suez {

using WorkerState = worker_framework::WorkerState;
using ErrorCode = WorkerState::ErrorCode;

class MockWorkerState : public WorkerState {
public:
    MOCK_METHOD1(write, ErrorCode(const std::string &));
    MOCK_METHOD1(read, ErrorCode(std::string &));
};

typedef ::testing::StrictMock<MockWorkerState> StrictMockWorkerState;
typedef ::testing::NiceMock<MockWorkerState> NiceMockWorkerState;

} // namespace suez
