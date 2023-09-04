#include "indexlib/common_define.h"
#include "indexlib/config/truncate_option_config.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

namespace indexlib { namespace config {

class TruncateOptionConfigTest : public INDEXLIB_TESTBASE
{
public:
    DECLARE_CLASS_NAME(TruncateOptionConfigTest);
    void CaseSetUp() override;
    void CaseTearDown() override;

public:
    void TestCaseForInvalidDistinctConfig();
    void TestCaseTruncateStrategyWithZeroLimit();
    void TestCaseForInvalidTruncLimitAndDistinctCount();
    void TestCaseForInvalidTruncateProfile();
    void TestCaseForInvalidStrategyName();
    void TestCaseForTruncateMetaStrategy();
    void TestCaseForIsFilterByTimeStamp();
    void TestCaseForMultiLevelSortParam();
    void TestCaseForInvalidFilter();
    void TestCaseForUpdateTruncateIndexConfig();
    void TestCaseForBadProfileName();
    void TestCaseForMultiShardingIndex();

private:
    void CheckTruncateProperties(const IndexSchemaPtr& indexSchema, const TruncateOptionConfigPtr& truncateConfig,
                                 const std::string& indexName);

    void CheckTruncatePropertiesForIndex(const IndexSchemaPtr& indexSchema, const IndexConfigPtr& indexConfig,
                                         const TruncateOptionConfigPtr& truncateConfig);
    void CheckProfileName(const IndexConfig* indexConfig, const TruncateProfileSchema* truncateProfileSchema,
                          const std::vector<std::string>& exsitedProfileNames,
                          const std::vector<std::string>& nonExsitedProfileNames);
    std::string LoadJsonString(const std::string& optionFile);
    TruncateOptionConfigPtr LoadFromJsonStr(const std::string& jsonStr);

private:
    std::string mJsonString;
    std::string mJsonString2;
    IndexPartitionSchemaPtr mSchema;
    IndexPartitionSchemaPtr mSchema2;

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(TruncateOptionConfigTest, TestCaseForInvalidDistinctConfig);
INDEXLIB_UNIT_TEST_CASE(TruncateOptionConfigTest, TestCaseForInvalidTruncLimitAndDistinctCount);
INDEXLIB_UNIT_TEST_CASE(TruncateOptionConfigTest, TestCaseForInvalidTruncateProfile);
INDEXLIB_UNIT_TEST_CASE(TruncateOptionConfigTest, TestCaseForInvalidStrategyName);
INDEXLIB_UNIT_TEST_CASE(TruncateOptionConfigTest, TestCaseForInvalidFilter);
INDEXLIB_UNIT_TEST_CASE(TruncateOptionConfigTest, TestCaseForMultiLevelSortParam);
INDEXLIB_UNIT_TEST_CASE(TruncateOptionConfigTest, TestCaseForTruncateMetaStrategy);
INDEXLIB_UNIT_TEST_CASE(TruncateOptionConfigTest, TestCaseForIsFilterByTimeStamp);
INDEXLIB_UNIT_TEST_CASE(TruncateOptionConfigTest, TestCaseForUpdateTruncateIndexConfig);
INDEXLIB_UNIT_TEST_CASE(TruncateOptionConfigTest, TestCaseTruncateStrategyWithZeroLimit);
INDEXLIB_UNIT_TEST_CASE(TruncateOptionConfigTest, TestCaseForBadProfileName);
INDEXLIB_UNIT_TEST_CASE(TruncateOptionConfigTest, TestCaseForMultiShardingIndex);
}} // namespace indexlib::config
