#pragma once

#include "indexlib/common_define.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

DECLARE_REFERENCE_CLASS(test, TableManager);
namespace indexlib { namespace partition {

class CustomTablePerfTest : public INDEXLIB_TESTBASE
{
public:
    CustomTablePerfTest();
    ~CustomTablePerfTest();

    DECLARE_CLASS_NAME(CustomTablePerfTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSimpleProcess();
    void TestAlwaysForceReopen();

private:
    void DoMultiThreadTest(size_t readThreadNum, int64_t duration);

    void Write(bool isOffline)
    {
        while (!mIsRun) {
            usleep(0);
        }
        while (!mIsFinish) {
            DoBuild(isOffline);
            sleep(10);
        }
    }
    void Read(int* status)
    {
        while (!mIsRun) {
            usleep(0);
        }
        while (!mIsFinish) {
            DoRead(status);
            usleep(100);
        }
    }
    void DoBuild(bool isOffline);
    void DoRead(int* status);
    bool IsFinished() { return mIsFinish; }

    std::string CreateRawDocString(int batchIdx, int64_t beginTs, int batchNum);

private:
    bool volatile mIsFinish;
    bool volatile mIsRun;
    test::TableManagerPtr mTableManager;

    volatile int mIncBuildIdx;
    volatile int mRtBuildIdx;
    volatile int mIncBatchNum;
    volatile int mRtBatchNum;

private:
    // case parameters
    std::vector<int> mIncBatchSize;
    bool mNeedForceReopen;

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(CustomTablePerfTest, TestSimpleProcess);
INDEXLIB_UNIT_TEST_CASE(CustomTablePerfTest, TestAlwaysForceReopen);
}} // namespace indexlib::partition
