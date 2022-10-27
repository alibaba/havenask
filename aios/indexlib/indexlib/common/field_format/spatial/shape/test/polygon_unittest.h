#ifndef __INDEXLIB_POLYGONTEST_H
#define __INDEXLIB_POLYGONTEST_H

#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/common/field_format/spatial/shape/polygon.h"

IE_NAMESPACE_BEGIN(common);

class PolygonTest : public INDEXLIB_TESTBASE
{
public:
    PolygonTest();
    ~PolygonTest();

    DECLARE_CLASS_NAME(PolygonTest);
public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestFromString();
    void TestIsInPolygon();
    void TestGetRelationForConvex();
    void TestGetRelationForConcave();
    void TestGetRelationForPole();
    void TestGetRelationForDateLine();

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(PolygonTest, TestFromString);
INDEXLIB_UNIT_TEST_CASE(PolygonTest, TestIsInPolygon);
INDEXLIB_UNIT_TEST_CASE(PolygonTest, TestGetRelationForConvex);
INDEXLIB_UNIT_TEST_CASE(PolygonTest, TestGetRelationForConcave);
INDEXLIB_UNIT_TEST_CASE(PolygonTest, TestGetRelationForPole);
INDEXLIB_UNIT_TEST_CASE(PolygonTest, TestGetRelationForDateLine);

IE_NAMESPACE_END(common);

#endif //__INDEXLIB_POLYGONTEST_H
