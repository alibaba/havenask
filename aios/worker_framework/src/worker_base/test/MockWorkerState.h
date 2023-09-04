#pragma once

#include "gmock/gmock.h"

#include "unittest/unittest.h"
#include "worker_framework/WorkerState.h"

namespace worker_framework {
namespace worker_base {

class MockWorkerState : public WorkerState {
public:
    MOCK_METHOD1(write, ErrorCode(const std::string &));
    MOCK_METHOD1(read, ErrorCode(std::string &));
};

} // namespace worker_base
} // namespace worker_framework
