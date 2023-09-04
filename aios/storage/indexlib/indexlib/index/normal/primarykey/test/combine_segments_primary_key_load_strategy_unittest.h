#ifndef __INDEXLIB_COMBINESEGMENTSPRIMARYKEYLOADSTRATEGYTEST_H
#define __INDEXLIB_COMBINESEGMENTSPRIMARYKEYLOADSTRATEGYTEST_H

#include "indexlib/common_define.h"
#include "indexlib/index/normal/primarykey/combine_segments_primary_key_load_strategy.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

namespace indexlib { namespace index {

class CombineSegmentsPrimaryKeyLoadStrategyTest : public INDEXLIB_TESTBASE_WITH_PARAM<int>
{
public:
    CombineSegmentsPrimaryKeyLoadStrategyTest();
    ~CombineSegmentsPrimaryKeyLoadStrategyTest();

    DECLARE_CLASS_NAME(CombineSegmentsPrimaryKeyLoadStrategyTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestCreatePrimaryKeyLoadPlans();

private:
    void InnerTestCreatePrimaryKeyLoadPlans(const std::string& offlineDocs, const std::string& rtDocs,
                                            const std::string& combineParam, size_t expectPlanNum,
                                            const std::string& expectPlanParam);

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(CombineSegmentsPrimaryKeyLoadStrategyTest, TestCreatePrimaryKeyLoadPlans);

INSTANTIATE_TEST_CASE_P(BuildMode, CombineSegmentsPrimaryKeyLoadStrategyTest, testing::Values(0, 1, 2));
}} // namespace indexlib::index

#endif //__INDEXLIB_COMBINESEGMENTSPRIMARYKEYLOADSTRATEGYTEST_H
