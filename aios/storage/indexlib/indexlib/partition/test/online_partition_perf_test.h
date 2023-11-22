#pragma once

#include <queue>

#include "autil/Lock.h"
#include "indexlib/common_define.h"
#include "indexlib/test/multi_thread_test_base.h"
#include "indexlib/test/partition_state_machine.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

namespace indexlib { namespace partition {

class OnlinePartitionPerfTest : public test::MultiThreadTestBase
{
public:
    OnlinePartitionPerfTest();
    ~OnlinePartitionPerfTest();

    DECLARE_CLASS_NAME(OnlinePartitionPerfTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;

    void TestReopen();
    void TestReopenWithAsyncDump();

protected:
    void DoWrite() override;
    void DoRead(int* errCode) override;

    void DoReopen();
    void DoRtBuild();
    void DoIncBuild();

    virtual std::string MakeDocStr();

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

protected:
    test::PartitionStateMachinePtr mPsm;
    config::IndexPartitionSchemaPtr mSchema;
    config::IndexPartitionOptions mOptions;
    int64_t mTimestamp;
    std::queue<std::string> mDocQueue;
    mutable autil::ThreadMutex mDocQueueLock;

private:
    IE_LOG_DECLARE();
};
}} // namespace indexlib::partition
