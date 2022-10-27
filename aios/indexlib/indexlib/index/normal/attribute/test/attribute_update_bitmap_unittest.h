#ifndef __INDEXLIB_ATTRIBUTEUPDATEBITMAPTEST_H
#define __INDEXLIB_ATTRIBUTEUPDATEBITMAPTEST_H

#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/index/normal/attribute/attribute_update_bitmap.h"

IE_NAMESPACE_BEGIN(index);

class AttributeUpdateBitmapTest : public INDEXLIB_TESTBASE
{
public:
    AttributeUpdateBitmapTest();
    ~AttributeUpdateBitmapTest();

    DECLARE_CLASS_NAME(AttributeUpdateBitmapTest);
public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSimpleProcess();
private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(AttributeUpdateBitmapTest, TestSimpleProcess);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_ATTRIBUTEUPDATEBITMAPTEST_H
