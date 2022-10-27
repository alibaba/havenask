#ifndef __INDEXLIB_QUERYSTRATEGYTEST_H
#define __INDEXLIB_QUERYSTRATEGYTEST_H

#include "indexlib/common_define.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/common/field_format/spatial/query_strategy.h"

IE_NAMESPACE_BEGIN(common);

class QueryStrategyTest : public INDEXLIB_TESTBASE
{
public:
    QueryStrategyTest();
    ~QueryStrategyTest();

    DECLARE_CLASS_NAME(QueryStrategyTest);
public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestDistanceToLevel();
private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(QueryStrategyTest, TestDistanceToLevel);

IE_NAMESPACE_END(common);

#endif //__INDEXLIB_QUERYSTRATEGYTEST_H
