#ifndef __INDEXLIB_LINETEST_H
#define __INDEXLIB_LINETEST_H

#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/common/field_format/spatial/shape/line.h"

IE_NAMESPACE_BEGIN(common);

class LineTest : public INDEXLIB_TESTBASE
{
public:
    LineTest();
    ~LineTest();

    DECLARE_CLASS_NAME(LineTest);
public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestGetRelation();
private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(LineTest, TestGetRelation);

IE_NAMESPACE_END(common);

#endif //__INDEXLIB_LINETEST_H
