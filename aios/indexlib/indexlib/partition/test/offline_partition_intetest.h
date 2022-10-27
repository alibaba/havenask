#ifndef __INDEXLIB_OFFLINEPARTITIONINTETEST_H
#define __INDEXLIB_OFFLINEPARTITIONINTETEST_H

#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/partition/index_builder.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/config/index_partition_options.h"
#include "indexlib/test/partition_state_machine.h"
#include "indexlib/index_base/index_meta/parallel_build_info.h"

IE_NAMESPACE_BEGIN(partition);

class OfflinePartitionInteTest : 
    public INDEXLIB_TESTBASE_WITH_PARAM<std::tr1::tuple<bool, bool> >
{
public:
    OfflinePartitionInteTest();
    ~OfflinePartitionInteTest();

    DECLARE_CLASS_NAME(OfflinePartitionInteTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestAddDocument();
    void TestMergeErrorWithBuildFileLost();
    void TestMergeErrorWithMergeFileLost();
    void TestDelDocument();
    void TestDedupAddDocument();
    void TestUpdateDocument();
    void TestAddDocWithoutPkWhenSchemaHasPkIndex();
    void TestBuildingSegmentDedup();
    void TestBuiltSegmentsDedup();
    void TestPartitionMetrics();
    void TestOpenWithSpecificVersion();
    void TestRecoverFromLegacySegmentDirName();
    void TestRecoverFromLegacySegmentDirNameWithNoVersion();
    void TestRecoverWithNoVersion();
    
private:
    void HasMetricsValue(const test::PartitionStateMachine& psm,
                         const std::string &name,
                         kmonitor::MetricType metricType,
                         const std::string &unit);
    void DoStepMerge(const std::string& step,
                     const index_base::ParallelBuildInfo& info,
                     bool optimize);
private:
    std::string mRootDir;
    config::IndexPartitionSchemaPtr mSchema;
    config::IndexPartitionOptions mOptions;

private:
    IE_LOG_DECLARE();
};

INDEXLIB_INSTANTIATE_TEST_CASE_ONE_P(OfflinePartitionInteTestMode, 
                                     OfflinePartitionInteTest, 
                                     testing::Values(
                                             std::tr1::tuple<bool, bool>(true, true),
                                             std::tr1::tuple<bool, bool>(false, false)));

INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(OfflinePartitionInteTestMode, TestAddDocument);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(OfflinePartitionInteTestMode, TestMergeErrorWithBuildFileLost);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(OfflinePartitionInteTestMode, TestMergeErrorWithMergeFileLost);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(OfflinePartitionInteTestMode, TestDelDocument);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(OfflinePartitionInteTestMode, TestDedupAddDocument);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(OfflinePartitionInteTestMode, TestUpdateDocument);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(OfflinePartitionInteTestMode, TestAddDocWithoutPkWhenSchemaHasPkIndex);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(OfflinePartitionInteTestMode, TestBuildingSegmentDedup);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(OfflinePartitionInteTestMode, TestBuiltSegmentsDedup);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(OfflinePartitionInteTestMode, TestPartitionMetrics);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(OfflinePartitionInteTestMode, TestOpenWithSpecificVersion);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(OfflinePartitionInteTestMode, TestRecoverFromLegacySegmentDirName);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(OfflinePartitionInteTestMode, TestRecoverFromLegacySegmentDirNameWithNoVersion);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(OfflinePartitionInteTestMode, TestRecoverWithNoVersion);

IE_NAMESPACE_END(partition);

#endif //__INDEXLIB_OFFLINEPARTITIONINTETEST_H
