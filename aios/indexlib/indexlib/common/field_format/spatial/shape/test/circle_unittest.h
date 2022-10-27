#ifndef __INDEXLIB_CIRCLETEST_H
#define __INDEXLIB_CIRCLETEST_H

#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/common/field_format/spatial/shape/circle.h"

IE_NAMESPACE_BEGIN(common);

class CircleTest : public INDEXLIB_TESTBASE
{
public:
    CircleTest();
    ~CircleTest();

    DECLARE_CLASS_NAME(CircleTest);
public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestGetBoundingBox();
    void TestFromString();
    void TestInsideRectangle();
    void TestIsInnerCoordinate();
    
private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(CircleTest, TestGetBoundingBox);
INDEXLIB_UNIT_TEST_CASE(CircleTest, TestFromString);
INDEXLIB_UNIT_TEST_CASE(CircleTest, TestInsideRectangle);
INDEXLIB_UNIT_TEST_CASE(CircleTest, TestIsInnerCoordinate);

IE_NAMESPACE_END(common);

#endif //__INDEXLIB_CIRCLETEST_H
