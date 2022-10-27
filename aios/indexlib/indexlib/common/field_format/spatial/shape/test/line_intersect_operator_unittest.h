#ifndef __INDEXLIB_LINEINTERSECTOPERATORTEST_H
#define __INDEXLIB_LINEINTERSECTOPERATORTEST_H

#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/common/field_format/spatial/shape/line_intersect_operator.h"

IE_NAMESPACE_BEGIN(common);

class LineIntersectOperatorTest : public INDEXLIB_TESTBASE
{
public:
    LineIntersectOperatorTest();
    ~LineIntersectOperatorTest();

    DECLARE_CLASS_NAME(LineIntersectOperatorTest);
public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestNoIntersection();
    void TestPointIntersection();
    void TestCrossIntersection();
    void TestColineIntersection_Q1OnLeftOfP1();
    void TestColineIntersection_Q1IsSameWithP1();
    void TestColineIntersection_Q1BetweenP1P2();
    void TestColineIntersection_Q1IsSameWithP2();
    void TestColineIntersection_Q1OnRightOfP2();
private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(LineIntersectOperatorTest, TestNoIntersection);
INDEXLIB_UNIT_TEST_CASE(LineIntersectOperatorTest, TestPointIntersection);
INDEXLIB_UNIT_TEST_CASE(LineIntersectOperatorTest, TestCrossIntersection);
INDEXLIB_UNIT_TEST_CASE(LineIntersectOperatorTest,
                        TestColineIntersection_Q1OnLeftOfP1);
INDEXLIB_UNIT_TEST_CASE(LineIntersectOperatorTest,
                        TestColineIntersection_Q1IsSameWithP1);
INDEXLIB_UNIT_TEST_CASE(LineIntersectOperatorTest,
                        TestColineIntersection_Q1BetweenP1P2);
INDEXLIB_UNIT_TEST_CASE(LineIntersectOperatorTest,
                        TestColineIntersection_Q1IsSameWithP2);
INDEXLIB_UNIT_TEST_CASE(LineIntersectOperatorTest,
                        TestColineIntersection_Q1OnRightOfP2);

IE_NAMESPACE_END(common);

#endif //__INDEXLIB_LINEINTERSECTOPERATORTEST_H
