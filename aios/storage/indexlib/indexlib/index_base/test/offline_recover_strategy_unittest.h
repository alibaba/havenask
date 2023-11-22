#pragma once

#include "indexlib/common_define.h"
#include "indexlib/config/index_partition_options.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/index_base/index_meta/version.h"
#include "indexlib/index_base/offline_recover_strategy.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

namespace indexlib { namespace index_base {

class OfflineRecoverStrategyTest : public INDEXLIB_TESTBASE
{
public:
    OfflineRecoverStrategyTest();
    ~OfflineRecoverStrategyTest();

    DECLARE_CLASS_NAME(OfflineRecoverStrategyTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;

    void TestCaseForRecoverEmptyIndex();
    void TestCaseForRecoverNormalIndex();
    void TestCaseForRecoverAfterMerge();
    void TestCaseForRecoverBrokenSegment();
    void TestCaseForRecoverWithInvalidPrefixSegment();
    void TestCaseForRecoverWithInvalidTempDir();
    void TestCaseForVersionLevelRecover();

private:
    // currentSegInfos = segId,docCount,timestamp;...
    // lostSegInfos = [broken|temp|normal|merge]|segId,docCount,timestamp#...
    void InnerTestRecover(const std::string& currentSegInfos, const std::string& lostSegInfos, bool generateLocator,
                          index_base::Version& expectVersion);

private:
    std::string mRootDir;
    config::IndexPartitionSchemaPtr mSchema;
    config::IndexPartitionOptions mOptions;

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(OfflineRecoverStrategyTest, TestCaseForRecoverEmptyIndex);
INDEXLIB_UNIT_TEST_CASE(OfflineRecoverStrategyTest, TestCaseForRecoverNormalIndex);
INDEXLIB_UNIT_TEST_CASE(OfflineRecoverStrategyTest, TestCaseForRecoverAfterMerge);
INDEXLIB_UNIT_TEST_CASE(OfflineRecoverStrategyTest, TestCaseForRecoverBrokenSegment);
INDEXLIB_UNIT_TEST_CASE(OfflineRecoverStrategyTest, TestCaseForRecoverWithInvalidPrefixSegment);
INDEXLIB_UNIT_TEST_CASE(OfflineRecoverStrategyTest, TestCaseForRecoverWithInvalidTempDir);
INDEXLIB_UNIT_TEST_CASE(OfflineRecoverStrategyTest, TestCaseForVersionLevelRecover);
}} // namespace indexlib::index_base
