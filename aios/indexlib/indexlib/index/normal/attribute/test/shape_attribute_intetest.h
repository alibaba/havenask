#ifndef __INDEXLIB_SHAPEATTRIBUTEINTETEST_H
#define __INDEXLIB_SHAPEATTRIBUTEINTETEST_H

#include "indexlib/common_define.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

IE_NAMESPACE_BEGIN(index);

class ShapeAttributeInteTest : public INDEXLIB_TESTBASE
{
public:
    ShapeAttributeInteTest();
    ~ShapeAttributeInteTest();

    DECLARE_CLASS_NAME(ShapeAttributeInteTest);
public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestPolygonAttr();
    void TestLineAttr();
    void TestInputIllegalPolygon();
    void TestInputIllegalLine();
private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(ShapeAttributeInteTest, TestPolygonAttr);
INDEXLIB_UNIT_TEST_CASE(ShapeAttributeInteTest, TestLineAttr);
INDEXLIB_UNIT_TEST_CASE(ShapeAttributeInteTest, TestInputIllegalPolygon);
INDEXLIB_UNIT_TEST_CASE(ShapeAttributeInteTest, TestInputIllegalLine);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_SHAPEATTRIBUTEINTETEST_H
