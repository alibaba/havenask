#ifndef __INDEXLIB_TYPEDSLICELISTTEST_H
#define __INDEXLIB_TYPEDSLICELISTTEST_H

#include <autil/mem_pool/Pool.h>
#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/common/typed_slice_list.h"

IE_NAMESPACE_BEGIN(common);

class TypedSliceListTest : public INDEXLIB_TESTBASE {
public:
    TypedSliceListTest();
    ~TypedSliceListTest();

    DECLARE_CLASS_NAME(TypedSliceListTest);
public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestPushBackTypedValue();
    void TestUpdate();
    void TestRead();
    void TestOperatorBracket();

private:
    autil::mem_pool::Pool mByteSlicePool;

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(TypedSliceListTest, TestPushBackTypedValue);
INDEXLIB_UNIT_TEST_CASE(TypedSliceListTest, TestUpdate);
INDEXLIB_UNIT_TEST_CASE(TypedSliceListTest, TestRead);
INDEXLIB_UNIT_TEST_CASE(TypedSliceListTest, TestOperatorBracket);

IE_NAMESPACE_END(common);

#endif //__INDEXLIB_TYPEDSLICELISTTEST_H
