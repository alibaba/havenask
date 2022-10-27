#ifndef __INDEXLIB_SHAPEINDEXQUERYSTRATEGYTEST_H
#define __INDEXLIB_SHAPEINDEXQUERYSTRATEGYTEST_H

#include "indexlib/common_define.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/common/field_format/spatial/shape_index_query_strategy.h"

IE_NAMESPACE_BEGIN(common);

class ShapeIndexQueryStrategyTest : public INDEXLIB_TESTBASE
{
public:
    ShapeIndexQueryStrategyTest();
    ~ShapeIndexQueryStrategyTest();

    DECLARE_CLASS_NAME(ShapeIndexQueryStrategyTest);
public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestCalculateTerms();
private:
    void ExtractExpectedTerms(const std::string& geoHashStr,
                              std::set<uint64_t>& terms,
                              const std::string& notLeafGeoHashStr);
    bool CheckTerms(const std::vector<std::string>& terms,
                    const std::set<uint64_t>& expectedTerms);

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(ShapeIndexQueryStrategyTest, TestCalculateTerms);

IE_NAMESPACE_END(common);

#endif //__INDEXLIB_SHAPEINDEXQUERYSTRATEGYTEST_H
