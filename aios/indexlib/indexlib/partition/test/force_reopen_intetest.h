#ifndef __INDEXLIB_FORCEREOPENINTETEST_H
#define __INDEXLIB_FORCEREOPENINTETEST_H

#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/partition/test/online_partition_intetest.h"

IE_NAMESPACE_BEGIN(partition);

class ForceReopenInteTest : public OnlinePartitionInteTest
{
public:
    ForceReopenInteTest();
    ~ForceReopenInteTest();

    DECLARE_CLASS_NAME(ForceReopenInteTest);
public:
    void CaseSetUp() override;
    void CaseTearDown() override;

    void TestForceReopen();
    void TestForceReopenWithCleanRtSegments();
    void TestForceReopenWithCleanJoinSegments();
    void TestForceReopenFailed();
    void TestBuildRtAndForceReopenAfterReopenFailed();
    void TestForceReopenWithCleanSliceFile();
    void TestForceReopenWithUsingReader();
    void TestReopenWithRollbackVersion();
    void TestReopenIncWithUpdatePatch();
    void TestForceReopenMemoryControl();
    void TestCleanReadersAndInMemoryIndex();

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(ForceReopenInteTest, TestForceReopen);
INDEXLIB_UNIT_TEST_CASE(ForceReopenInteTest, TestForceReopenWithCleanRtSegments);
INDEXLIB_UNIT_TEST_CASE(ForceReopenInteTest, TestForceReopenWithCleanJoinSegments);
INDEXLIB_UNIT_TEST_CASE(ForceReopenInteTest, TestForceReopenFailed);
INDEXLIB_UNIT_TEST_CASE(ForceReopenInteTest, TestBuildRtAndForceReopenAfterReopenFailed);

INDEXLIB_UNIT_TEST_CASE(ForceReopenInteTest, TestRtAddDocHavingSamePkWithIncDoc);
INDEXLIB_UNIT_TEST_CASE(ForceReopenInteTest, TestAddIncDocTsBetweenTwoRtDocsWithSamePk);
INDEXLIB_UNIT_TEST_CASE(ForceReopenInteTest, TestRtRemoveIncDoc);
INDEXLIB_UNIT_TEST_CASE(ForceReopenInteTest, TestRtRemoveArrivingIncDoc);
INDEXLIB_UNIT_TEST_CASE(ForceReopenInteTest, TestRtRemoveCurrentAndArrivingIncDoc);
INDEXLIB_UNIT_TEST_CASE(ForceReopenInteTest, TestRtUpdateIncDoc);
INDEXLIB_UNIT_TEST_CASE(ForceReopenInteTest, TestRtUpdateArrivingIncDoc);
INDEXLIB_UNIT_TEST_CASE(ForceReopenInteTest, TestRtUpdateCurrentAndArrivingIncDoc);
INDEXLIB_UNIT_TEST_CASE(ForceReopenInteTest, TestPartitionInfoWithIncReopen);

INDEXLIB_UNIT_TEST_CASE(ForceReopenInteTest, TestReopenIncIndex);
INDEXLIB_UNIT_TEST_CASE(ForceReopenInteTest, TestReopenIncIndexMultiTimes);
INDEXLIB_UNIT_TEST_CASE(ForceReopenInteTest, TestReopenIncIndexWithRtNotReclaimed);
INDEXLIB_UNIT_TEST_CASE(ForceReopenInteTest, TestReopenIncIndexWithRtAllReclaimed);
INDEXLIB_UNIT_TEST_CASE(ForceReopenInteTest, TestReopenIncIndexWithRtPartlyReclaimed);
INDEXLIB_UNIT_TEST_CASE(ForceReopenInteTest, TestReopenIncIndexWithSamePkRtDocsPartlyReclaimed);
INDEXLIB_UNIT_TEST_CASE(ForceReopenInteTest, TestReopenIncIndexWithRtReclaimedBySameDocAndSameTs);
INDEXLIB_UNIT_TEST_CASE(ForceReopenInteTest, TestReopenIncIndexWithRtReclaimedTwice);
INDEXLIB_UNIT_TEST_CASE(ForceReopenInteTest, TestReopenIncIndexWhenRtIndexTimestampEqualToInc);
INDEXLIB_UNIT_TEST_CASE(ForceReopenInteTest, TestReopenIncIndexWithRtUpdateInc);
INDEXLIB_UNIT_TEST_CASE(ForceReopenInteTest, TestReopenIncTwiceWithNoRtAfterFirstInc);
INDEXLIB_UNIT_TEST_CASE(ForceReopenInteTest, TestReopenIncWithAddAddDelCombo);
INDEXLIB_UNIT_TEST_CASE(ForceReopenInteTest, TestReopenIncWithAddDelAddCombo);
INDEXLIB_UNIT_TEST_CASE(ForceReopenInteTest, TestReopenIncIndexWithTrim);
INDEXLIB_UNIT_TEST_CASE(ForceReopenInteTest, TestReopenIncIndexWithMultiNewVersion);
INDEXLIB_UNIT_TEST_CASE(ForceReopenInteTest, TestForceReopenWithCleanSliceFile);
//INDEXLIB_UNIT_TEST_CASE(ForceReopenInteTest, TestForceReopenWithUsingReader);
INDEXLIB_UNIT_TEST_CASE(ForceReopenInteTest, TestReopenWithRollbackVersion);
INDEXLIB_UNIT_TEST_CASE(ForceReopenInteTest, TestReopenIncWithUpdatePatch);
INDEXLIB_UNIT_TEST_CASE(ForceReopenInteTest, TestForceReopenMemoryControl);
INDEXLIB_UNIT_TEST_CASE(ForceReopenInteTest, TestCleanReadersAndInMemoryIndex);

IE_NAMESPACE_END(partition);

#endif //__INDEXLIB_FORCEREOPENINTETEST_H
