#ifndef __INDEXLIB_ADAPTIVEATTRIBUTEOFFSETDUMPERTEST_H
#define __INDEXLIB_ADAPTIVEATTRIBUTEOFFSETDUMPERTEST_H

#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/index/normal/attribute/adaptive_attribute_offset_dumper.h"

IE_NAMESPACE_BEGIN(index);

class AdaptiveAttributeOffsetDumperTest : public INDEXLIB_TESTBASE
{
public:
    AdaptiveAttributeOffsetDumperTest();
    ~AdaptiveAttributeOffsetDumperTest();

    DECLARE_CLASS_NAME(AdaptiveAttributeOffsetDumperTest);
public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSimpleProcess();
private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(AdaptiveAttributeOffsetDumperTest, TestSimpleProcess);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_ADAPTIVEATTRIBUTEOFFSETDUMPERTEST_H
