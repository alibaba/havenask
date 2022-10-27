#ifndef __INDEXLIB_SPATIALOPTIMIZESTRATEGYTEST_H
#define __INDEXLIB_SPATIALOPTIMIZESTRATEGYTEST_H

#include "indexlib/common_define.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/common/field_format/spatial/spatial_optimize_strategy.h"

IE_NAMESPACE_BEGIN(common);

class SpatialOptimizeStrategyTest : public INDEXLIB_TESTBASE
{
public:
    SpatialOptimizeStrategyTest();
    ~SpatialOptimizeStrategyTest();

    DECLARE_CLASS_NAME(SpatialOptimizeStrategyTest);
public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSimpleProcess();
private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(SpatialOptimizeStrategyTest, TestSimpleProcess);

IE_NAMESPACE_END(common);

#endif //__INDEXLIB_SPATIALOPTIMIZESTRATEGYTEST_H
