#ifndef __INDEXLIB_VARNUMATTRIBUTEDATAITERATORTEST_H
#define __INDEXLIB_VARNUMATTRIBUTEDATAITERATORTEST_H

#include <autil/mem_pool/Pool.h>
#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/index/normal/attribute/accessor/var_num_attribute_data_iterator.h"

IE_NAMESPACE_BEGIN(index);

class VarNumAttributeDataIteratorTest : public INDEXLIB_TESTBASE {
public:
    VarNumAttributeDataIteratorTest();
    ~VarNumAttributeDataIteratorTest();

    DECLARE_CLASS_NAME(VarNumAttributeDataIteratorTest);
public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestHasNext();
    void TestNext();

private:
    VarNumAttributeDataIteratorPtr PrepareIterator(const char* data, size_t length);

private:
    autil::mem_pool::Pool mPool;
private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(VarNumAttributeDataIteratorTest, TestHasNext);
INDEXLIB_UNIT_TEST_CASE(VarNumAttributeDataIteratorTest, TestNext);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_VARNUMATTRIBUTEDATAITERATORTEST_H
