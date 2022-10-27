#ifndef __INDEXLIB_BYTESLICELISTITERATORTEST_H
#define __INDEXLIB_BYTESLICELISTITERATORTEST_H

#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/util/byte_slice_list/byte_slice_list_iterator.h"
#include <autil/mem_pool/Pool.h>

IE_NAMESPACE_BEGIN(util);

class ByteSliceListIteratorTest : public INDEXLIB_TESTBASE
{
public:
    ByteSliceListIteratorTest();
    ~ByteSliceListIteratorTest();

    DECLARE_CLASS_NAME(ByteSliceListIteratorTest);
public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSimpleProcess();
    void TestSeekSlice();
    void TestNext();

private:
    ByteSlice* CreateSlice(uint32_t itemCount);

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(ByteSliceListIteratorTest, TestSimpleProcess);
INDEXLIB_UNIT_TEST_CASE(ByteSliceListIteratorTest, TestSeekSlice);
INDEXLIB_UNIT_TEST_CASE(ByteSliceListIteratorTest, TestNext);

IE_NAMESPACE_END(util);

#endif //__INDEXLIB_BYTESLICELISTITERATORTEST_H
