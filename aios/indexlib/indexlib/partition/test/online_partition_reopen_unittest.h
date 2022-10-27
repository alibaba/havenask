#ifndef __INDEXLIB_ONLINEPARTITIONREOPENTEST_H
#define __INDEXLIB_ONLINEPARTITIONREOPENTEST_H

#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/partition/online_partition.h"
#include "indexlib/test/partition_state_machine.h"

IE_NAMESPACE_BEGIN(partition);

class OnlinePartitionReopenTest : public INDEXLIB_TESTBASE
{
public:
    OnlinePartitionReopenTest();
    ~OnlinePartitionReopenTest();

    DECLARE_CLASS_NAME(OnlinePartitionReopenTest);
public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestReopen();
    void TestForceReopen();
    void TestSimpleProcess();
    void TestAsyncDumpReopenFail();
    void TestAsyncDumpReopenSuccess();
    void TestForceOpen();
    void TestAsyncDumpForceOpen();
private:
    void DoTest(std::function<void(int64_t, int64_t, bool&, bool&)> tester);
    void InnerTestReopen(int64_t triggerErrorIdx, int64_t continueFailIdx,
                         bool& firstErrorTriggered, bool& secondErrorTriggered);
    void InnerTestForceReopen(int64_t triggerErrorIdx, int64_t continueFailIdx,
                              bool& firstErrorTriggered, bool& secondErrorTriggered);
    void InnerTestRollBackWithoutRT(int64_t triggerErrorIdx);

    void InnerTestKVReopen(int64_t triggerErrorIdx, int64_t continueFailIdx,
                           bool& firstErrorTriggered, bool& secondErrorTriggered);

    void InnerTestKVForceReopen(int64_t triggerErrorIdx, int64_t continueFailIdx,
                                bool& firstErrorTriggered, bool& secondErrorTriggered);
    void InnerTestAsyncDumpReopen(size_t triggerErrorIdx, const std::string& incDoc,
                                  const std::string& reopenQuery, const std::string& reopenResult);
    void InnerTestForceOpen(int64_t triggerErrorIdx, int64_t continueFailIdx,
                            bool& firstErrorTriggered, bool& secondErrorTriggered);
    
private:
    static const size_t NO_ERROR = 100000000;
    config::IndexPartitionSchemaPtr mSchema;
    std::string mFullDocs;
    std::string mIncDocs;
    std::string mRtDocs;
    std::string mRtDocs1;
    std::string mRtDocs2; 

    std::string mKVFullDocs;
    std::string mKVRtDocs0;
    std::string mKVRtDocs1;
    std::string mKVIncDocs;
    std::string mKVRtDocs2;

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(OnlinePartitionReopenTest, TestSimpleProcess);
INDEXLIB_UNIT_TEST_CASE(OnlinePartitionReopenTest, TestReopen);
INDEXLIB_UNIT_TEST_CASE(OnlinePartitionReopenTest, TestForceReopen);
INDEXLIB_UNIT_TEST_CASE(OnlinePartitionReopenTest, TestAsyncDumpReopenFail);
INDEXLIB_UNIT_TEST_CASE(OnlinePartitionReopenTest, TestAsyncDumpReopenSuccess);
INDEXLIB_UNIT_TEST_CASE(OnlinePartitionReopenTest, TestForceOpen);
INDEXLIB_UNIT_TEST_CASE(OnlinePartitionReopenTest, TestAsyncDumpForceOpen);

IE_NAMESPACE_END(partition);

#endif //__INDEXLIB_ONLINEPARTITIONREOPENTEST_H
