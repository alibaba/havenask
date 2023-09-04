#pragma once

#include "gmock/gmock.h"

#include "unittest/unittest.h"
#include "worker_framework/WorkerBase.h"

namespace worker_framework {

class MockWorkerBase : public WorkerBase {
public:
    MOCK_METHOD0(doInitOptions, void());
    MOCK_METHOD0(doExtractOptions, void());
    MOCK_METHOD0(doInit, bool());
    MOCK_METHOD0(doStart, bool());
    MOCK_METHOD0(doStop, void());
};

}; // namespace worker_framework
