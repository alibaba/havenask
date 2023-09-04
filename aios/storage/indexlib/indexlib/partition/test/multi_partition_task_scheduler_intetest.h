#ifndef __INDEXLIB_MULTIPARTITIONTASKSCHEDULERINTETEST_H
#define __INDEXLIB_MULTIPARTITIONTASKSCHEDULERINTETEST_H

#include <queue>

#include "autil/Lock.h"
#include "indexlib/common_define.h"
#include "indexlib/test/multi_thread_test_base.h"
#include "indexlib/test/partition_state_machine.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

namespace indexlib { namespace partition {

class MultiPartitionTaskSchedulerIntetest : public test::MultiThreadTestBase
{
public:
    MultiPartitionTaskSchedulerIntetest();
    ~MultiPartitionTaskSchedulerIntetest();

    DECLARE_CLASS_NAME(MultiPartitionTaskSchedulerIntetest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSimpleProcess();

private:
    void DoWrite() override;
    void DoRead(int* errCode) override;

    void DoRtBuild();
    void DoIncBuild();

    std::string MakeDocStr();

    void PushDoc(const std::string& doc)
    {
        autil::ScopedLock lock(mDocQueueLock);
        mDocQueue.push(doc);
    }

    std::string PopDoc()
    {
        autil::ScopedLock lock(mDocQueueLock);
        std::string res = "";
        if (!mDocQueue.empty()) {
            res = mDocQueue.front();
            mDocQueue.pop();
        }
        return res;
    }

private:
    std::vector<test::PartitionStateMachinePtr> mPsms;
    int64_t mTimestamp;
    std::queue<std::string> mDocQueue;
    mutable autil::ThreadMutex mDocQueueLock;
    mutable autil::ThreadMutex mMachineLock;
    partition::IndexPartitionResource mPartitionResource;

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(MultiPartitionTaskSchedulerIntetest, TestSimpleProcess);
}} // namespace indexlib::partition

#endif //__INDEXLIB_MULTIPARTITIONTASKSCHEDULERINTETEST_H
