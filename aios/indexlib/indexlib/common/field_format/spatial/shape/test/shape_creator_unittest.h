#ifndef __INDEXLIB_SHAPECREATORTEST_H
#define __INDEXLIB_SHAPECREATORTEST_H

#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/common/field_format/spatial/shape/shape_creator.h"

IE_NAMESPACE_BEGIN(common);

class ShapeCreatorTest : public INDEXLIB_TESTBASE
{
public:
    ShapeCreatorTest();
    ~ShapeCreatorTest();

    DECLARE_CLASS_NAME(ShapeCreatorTest);
public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestCreatePoint();
    void TestCreateCircle();
    void TestCreateRectangle();

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(ShapeCreatorTest, TestCreatePoint);
INDEXLIB_UNIT_TEST_CASE(ShapeCreatorTest, TestCreateCircle);
INDEXLIB_UNIT_TEST_CASE(ShapeCreatorTest, TestCreateRectangle);

IE_NAMESPACE_END(common);

#endif //__INDEXLIB_SHAPECREATORTEST_H
