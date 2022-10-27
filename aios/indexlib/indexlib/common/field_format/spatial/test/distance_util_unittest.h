#ifndef __INDEXLIB_DISTANCEUTILTEST_H
#define __INDEXLIB_DISTANCEUTILTEST_H

#include "indexlib/common_define.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/common/field_format/spatial/distance_util.h"

IE_NAMESPACE_BEGIN(common);

class DistanceUtilTest : public INDEXLIB_TESTBASE
{
public:
    DistanceUtilTest();
    ~DistanceUtilTest();

    DECLARE_CLASS_NAME(DistanceUtilTest);
public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestCalculateBoundingBoxFromPointDeg();
private:
    void InnerTestCalculateBoundingBoxFromPointDeg(const std::string& pointStr,
            double distDEG, const std::string& rectangleStr);

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(DistanceUtilTest, TestCalculateBoundingBoxFromPointDeg);

IE_NAMESPACE_END(common);

#endif //__INDEXLIB_DISTANCEUTILTEST_H
