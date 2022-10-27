#ifndef ISEARCH_BS_MOCKMERGETASK_H
#define ISEARCH_BS_MOCKMERGETASK_H

#include "build_service/test/unittest.h"
#include "build_service/test/test.h"
#include "build_service/util/Log.h"
#include "build_service/task_base/MergeTask.h"

DECLARE_REFERENCE_CLASS(util, CounterMap);

namespace build_service {
namespace task_base {

class MockMergeTask : public MergeTask
{
public:
    MockMergeTask() {
        ON_CALL(*this, getMergeStatus()).WillByDefault(
            Return(this->_mergeStatus));
        ON_CALL(*this, run(_,_,_,_))
            .WillByDefault(Invoke(this, &MockMergeTask::defaultRun));
    }
    ~MockMergeTask() {}

public:
    MOCK_METHOD4(run, bool(const std::string&, uint32_t, Mode, bool));
    MOCK_METHOD1(getMergedVersion, IE_NAMESPACE(index_base)::Version(const proto::PartitionId&));
    MOCK_METHOD0(getMergeStatus, MergeTask::MergeStatus());
    MOCK_METHOD4(CreateIncParallelPartitionMerger, IE_NAMESPACE(merger)::IndexPartitionMergerPtr(
                     const proto::PartitionId& pid,
                    const std::string& targetIndexPath,
                    const IE_NAMESPACE(config)::IndexPartitionOptions& options,
                    uint32_t workerPathVersion));

    bool defaultRun(const std::string &jobParam, uint32_t instanceId,
                     Mode mode, bool optimize)
    {
        return MergeTask::run(jobParam, instanceId, mode, optimize);
    }
};

typedef ::testing::StrictMock<MockMergeTask> StrictMockMergeTask;
typedef ::testing::NiceMock<MockMergeTask> NiceMockMergeTask;

}
}

#endif //ISEARCH_BS_MOCKMERGETASK_H
