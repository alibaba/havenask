#ifndef __INDEXLIB_RECTANGLEINTERSECTOPERATORTEST_H
#define __INDEXLIB_RECTANGLEINTERSECTOPERATORTEST_H

#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/common/field_format/spatial/shape/rectangle_intersect_operator.h"

IE_NAMESPACE_BEGIN(common);

class RectangleIntersectOperatorTest : public INDEXLIB_TESTBASE
{
public:
    RectangleIntersectOperatorTest();
    ~RectangleIntersectOperatorTest();

    DECLARE_CLASS_NAME(RectangleIntersectOperatorTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestContainsForPolygon();
    void TestWithinsForPolygon();
    void TestIntersectsForPolygon();
    void TestDisjointsForPolygon();
    void TestContainsForLine();
    void TestIntersectsForLine();
    void TestDisjointsForLine();

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(RectangleIntersectOperatorTest, TestContainsForPolygon);
INDEXLIB_UNIT_TEST_CASE(RectangleIntersectOperatorTest, TestWithinsForPolygon);
INDEXLIB_UNIT_TEST_CASE(RectangleIntersectOperatorTest, TestIntersectsForPolygon);
INDEXLIB_UNIT_TEST_CASE(RectangleIntersectOperatorTest, TestDisjointsForPolygon);
INDEXLIB_UNIT_TEST_CASE(RectangleIntersectOperatorTest, TestContainsForLine);
INDEXLIB_UNIT_TEST_CASE(RectangleIntersectOperatorTest, TestIntersectsForLine);
INDEXLIB_UNIT_TEST_CASE(RectangleIntersectOperatorTest, TestDisjointsForLine);

IE_NAMESPACE_END(common);

#endif //__INDEXLIB_RECTANGLEINTERSECTOPERATORTEST_H
