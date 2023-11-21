#pragma once

#include <unordered_map>

#include "autil/AtomicCounter.h"
#include "indexlib/common_define.h"
#include "indexlib/test/partition_state_machine.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

DECLARE_REFERENCE_CLASS(util, AccumulativeCounter);
namespace indexlib { namespace partition {

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
}} // namespace indexlib::partition
