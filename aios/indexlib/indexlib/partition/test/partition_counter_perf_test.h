#ifndef __INDEXLIB_PARTITIONCOUNTERPERFTEST_H
#define __INDEXLIB_PARTITIONCOUNTERPERFTEST_H

#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/test/partition_state_machine.h"
#include <autil/AtomicCounter.h>
#include <unordered_map>

DECLARE_REFERENCE_CLASS(util, AccumulativeCounter);
IE_NAMESPACE_BEGIN(partition);

class PartitionCounterPerfTest : public INDEXLIB_TESTBASE
{
public:
    PartitionCounterPerfTest();
    ~PartitionCounterPerfTest();

    DECLARE_CLASS_NAME(PartitionCounterPerfTest);
public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestMultiThreadUpdateCounters();
    void TestMultiThreadReadWithAccessCounters();
    
private:
    typedef std::unordered_map<std::string, util::AccumulativeCounterPtr> TestAccessCounterMap;
    typedef std::unordered_map<std::string, autil::AtomicCounter> CompAccessCounterMap;
    
private:
    void UpdateCounters(bool useTestCounters, int64_t duration);
    void Query(int64_t duration);    
    
private:
    std::string mRootDir;
    config::IndexPartitionOptions mOptions;
    config::IndexPartitionSchemaPtr mSchema;
    test::PartitionStateMachinePtr mPsm;    
    
    bool volatile mIsFinish;
    bool volatile mIsRun;
    std::vector<std::string> mFieldNames;
    TestAccessCounterMap mTestAccessCounters;
    CompAccessCounterMap mCompAccessCounters;

    util::AccumulativeCounterPtr mSingleAccCounter;
    autil::AtomicCounter mSingleAtomicCounter;
    
private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(PartitionCounterPerfTest, TestMultiThreadUpdateCounters);
INDEXLIB_UNIT_TEST_CASE(PartitionCounterPerfTest, TestMultiThreadReadWithAccessCounters);

IE_NAMESPACE_END(partition);

#endif //__INDEXLIB_PARTITIONCOUNTERPERFTEST_H
