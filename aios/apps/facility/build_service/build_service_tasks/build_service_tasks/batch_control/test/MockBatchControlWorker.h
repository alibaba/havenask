#include "aios/apps/facility/cm2/cm_basic/util/zk_wrapper.h"
#include "build_service_tasks/batch_control/BatchControlTask.h"
#include "build_service_tasks/batch_control/BatchControlWorker.h"
#include "build_service_tasks/batch_control/BatchReporter.h"
#include "build_service_tasks/test/unittest.h"
#include "worker_framework/ZkState.h"

using namespace testing;

using namespace worker_framework;

namespace build_service { namespace task_base {

class MockBatchControlTask : public BatchControlTask
{
public:
    MockBatchControlTask() {}

public:
    MOCK_METHOD2(initSwiftReader, bool(const std::string&, const std::string&));
};

class MockBatchControlWorker : public BatchControlWorker
{
public:
    MockBatchControlWorker(bool async = false) : BatchControlWorker(async) {}

public:
    MOCK_METHOD4(start, bool(swift::client::SwiftReader*, int64_t, const std::string&, int32_t));
    MOCK_METHOD2(resetHost, bool(const std::string&, int32_t));
    MOCK_METHOD2(initZkState, bool(const std::string&, const build_service::proto::BuildId&));
};

class MockBatchReporter : public BatchReporter
{
public:
    MockBatchReporter() {}

public:
    MOCK_METHOD3(report, bool(const BatchControlWorker::BatchInfo&, int64_t, int64_t));
};

class MockZkState : public worker_framework::ZkState
{
public:
    MockZkState(cm_basic::ZkWrapper* zk, const std::string& stateFile) : worker_framework::ZkState(zk, stateFile) {}

public:
    MOCK_METHOD1(write, WorkerState::ErrorCode(const std::string&));
    MOCK_METHOD1(read, WorkerState::ErrorCode(std::string&));
};

}} // namespace build_service::task_base
