#ifndef __INDEXLIB_LOCATION_INDEXQUERYSTRATEGYTEST_H
#define __INDEXLIB_LOCATION_INDEXQUERYSTRATEGYTEST_H

#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/common/field_format/spatial/location_index_query_strategy.h"

IE_NAMESPACE_BEGIN(common);

class LocationIndexQueryStrategyTest : public INDEXLIB_TESTBASE
{
public:
    LocationIndexQueryStrategyTest();
    ~LocationIndexQueryStrategyTest();

    DECLARE_CLASS_NAME(LocationIndexQueryStrategyTest);
public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSimpleProcess();
    void TestCircle();
    void TestCalculateTerms();

private:
    void InnerTestStrategy(const std::string& shapeName, 
                           const std::string& shapeStr,
                           const uint8_t expectDetailLevel);
    void ExtractExpectedTerms(const std::string& geoHashStr,
                              std::set<uint64_t>& terms);
    bool CheckTerms(const std::vector<std::string>& terms,
                    const std::set<uint64_t>& expectedTerms);
private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(LocationIndexQueryStrategyTest, TestSimpleProcess);
INDEXLIB_UNIT_TEST_CASE(LocationIndexQueryStrategyTest, TestCircle);
INDEXLIB_UNIT_TEST_CASE(LocationIndexQueryStrategyTest, TestCalculateTerms);

IE_NAMESPACE_END(common);

#endif //__INDEXLIB_LOCATION_INDEXQUERYSTRATEGYTEST_H
