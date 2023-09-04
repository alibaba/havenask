#ifndef __INDEXLIB_OFFLINEPARTITIONINTETEST_H
#define __INDEXLIB_OFFLINEPARTITIONINTETEST_H

#include "indexlib/common_define.h"
#include "indexlib/config/index_partition_options.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/index_base/index_meta/parallel_build_info.h"
#include "indexlib/partition/index_builder.h"
#include "indexlib/test/partition_state_machine.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

namespace indexlib { namespace partition {

class OfflinePartitionInteTest : public INDEXLIB_TESTBASE_WITH_PARAM<std::tuple<bool, bool, int>>
{
public:
    OfflinePartitionInteTest();
    ~OfflinePartitionInteTest();

    DECLARE_CLASS_NAME(OfflinePartitionInteTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestAddDocument();
    void TestIgnoreDocument();
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
    void TestNullField();
    void TestNullFieldWithUpdate();
    void TestNullFieldSortWithUpdate();
    void TestBuildSource();
    void TestFileCompressWithTemperatureLayer();
    void TestExcludedFileCompressWithTemperatureLayer();
    void TestUpdateFileCompressSchema();

private:
    void HasMetricsValue(const test::PartitionStateMachine& psm, const std::string& name,
                         kmonitor::MetricType metricType, const std::string& unit);
    void DoStepMerge(const std::string& step, const index_base::ParallelBuildInfo& info, bool optimize);

    void InnerTestFileCompressWithTemperatureLayer(const std::string& excludePattern);

private:
    std::string mRootDir;
    config::IndexPartitionSchemaPtr mSchema;
    config::IndexPartitionOptions mOptions;

private:
    IE_LOG_DECLARE();
};

INSTANTIATE_TEST_CASE_P(OfflinePartitionInteTestMode, OfflinePartitionInteTest,
                        testing::Values(std::tuple<bool, bool, int>(false, false, 0),
                                        std::tuple<bool, bool, int>(true, true, 0)));

INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(OfflinePartitionInteTest, TestAddDocument);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(OfflinePartitionInteTest, TestIgnoreDocument);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(OfflinePartitionInteTest, TestMergeErrorWithBuildFileLost);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(OfflinePartitionInteTest, TestMergeErrorWithMergeFileLost);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(OfflinePartitionInteTest, TestDelDocument);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(OfflinePartitionInteTest, TestDedupAddDocument);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(OfflinePartitionInteTest, TestUpdateDocument);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(OfflinePartitionInteTest, TestAddDocWithoutPkWhenSchemaHasPkIndex);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(OfflinePartitionInteTest, TestBuildingSegmentDedup);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(OfflinePartitionInteTest, TestBuiltSegmentsDedup);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(OfflinePartitionInteTest, TestPartitionMetrics);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(OfflinePartitionInteTest, TestOpenWithSpecificVersion);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(OfflinePartitionInteTest, TestRecoverFromLegacySegmentDirName);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(OfflinePartitionInteTest, TestRecoverFromLegacySegmentDirNameWithNoVersion);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(OfflinePartitionInteTest, TestRecoverWithNoVersion);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(OfflinePartitionInteTest, TestNullField);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(OfflinePartitionInteTest, TestNullFieldWithUpdate);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(OfflinePartitionInteTest, TestNullFieldSortWithUpdate);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(OfflinePartitionInteTest, TestBuildSource);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(OfflinePartitionInteTest, TestFileCompressWithTemperatureLayer);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(OfflinePartitionInteTest, TestExcludedFileCompressWithTemperatureLayer);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(OfflinePartitionInteTest, TestUpdateFileCompressSchema);

}} // namespace indexlib::partition

#endif //__INDEXLIB_OFFLINEPARTITIONINTETEST_H
