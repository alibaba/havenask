#pragma once

#include "autil/mem_pool/Pool.h"
#include "indexlib/common_define.h"
#include "indexlib/index/normal/attribute/accessor/in_mem_var_num_attribute_reader.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

namespace indexlib { namespace index {

class InMemVarNumAttributeReaderTest : public INDEXLIB_TESTBASE
{
public:
    InMemVarNumAttributeReaderTest();
    ~InMemVarNumAttributeReaderTest();

    DECLARE_CLASS_NAME(InMemVarNumAttributeReaderTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestRead();
    void TestReadNull();

private:
    autil::mem_pool::Pool mPool;
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(InMemVarNumAttributeReaderTest, TestRead);
INDEXLIB_UNIT_TEST_CASE(InMemVarNumAttributeReaderTest, TestReadNull);
}} // namespace indexlib::index
