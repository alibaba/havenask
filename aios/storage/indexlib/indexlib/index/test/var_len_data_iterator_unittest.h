#pragma once

#include "autil/mem_pool/Pool.h"
#include "indexlib/common_define.h"
#include "indexlib/index/normal/attribute/accessor/var_num_attribute_data_iterator.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

namespace indexlib { namespace index {

class VarLenDataIteratorTest : public INDEXLIB_TESTBASE
{
public:
    VarLenDataIteratorTest();
    ~VarLenDataIteratorTest();

    DECLARE_CLASS_NAME(VarLenDataIteratorTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestHasNext();
    void TestNext();

private:
    VarLenDataIteratorPtr PrepareIterator(const char* data, size_t length);

private:
    autil::mem_pool::Pool mPool;

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(VarLenDataIteratorTest, TestHasNext);
INDEXLIB_UNIT_TEST_CASE(VarLenDataIteratorTest, TestNext);
}} // namespace indexlib::index
