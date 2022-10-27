#ifndef __INDEXLIB_SLSEGTEST_H
#define __INDEXLIB_SLSEGTEST_H

#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/common/field_format/spatial/shape/slseg.h"

IE_NAMESPACE_BEGIN(common);

class SlsegTest : public INDEXLIB_TESTBASE
{
public:
    SlsegTest();
    ~SlsegTest();

    DECLARE_CLASS_NAME(SlsegTest);
public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSimpleProcess();
private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(SlsegTest, TestSimpleProcess);

IE_NAMESPACE_END(common);

#endif //__INDEXLIB_SLSEGTEST_H
