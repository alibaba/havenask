#ifndef __INDEXLIB_RECTANGLETEST_H
#define __INDEXLIB_RECTANGLETEST_H

#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/common/field_format/spatial/shape/rectangle.h"

IE_NAMESPACE_BEGIN(common);

class RectangleTest : public INDEXLIB_TESTBASE
{
public:
    RectangleTest();
    ~RectangleTest();

    DECLARE_CLASS_NAME(RectangleTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestFromString();
    void TestGetRelation();
    void TestGetRelationWithSpecailPlace();
    void TestIsInnerCoordinate();

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(RectangleTest, TestFromString);
INDEXLIB_UNIT_TEST_CASE(RectangleTest, TestGetRelation);
INDEXLIB_UNIT_TEST_CASE(RectangleTest, TestGetRelationWithSpecailPlace);
INDEXLIB_UNIT_TEST_CASE(RectangleTest, TestIsInnerCoordinate);

IE_NAMESPACE_END(common);

#endif //__INDEXLIB_RECTANGLETEST_H
