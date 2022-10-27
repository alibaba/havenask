#ifndef __INDEXLIB_POINTTEST_H
#define __INDEXLIB_POINTTEST_H

#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/common/field_format/spatial/shape/point.h"

IE_NAMESPACE_BEGIN(common);

class PointTest : public INDEXLIB_TESTBASE
{
public:
    PointTest();
    ~PointTest();

    DECLARE_CLASS_NAME(PointTest);
public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSimpleProcess();
    void TestFromString();
private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(PointTest, TestSimpleProcess);
INDEXLIB_UNIT_TEST_CASE(PointTest, TestFromString);

IE_NAMESPACE_END(common);

#endif //__INDEXLIB_POINTTEST_H
