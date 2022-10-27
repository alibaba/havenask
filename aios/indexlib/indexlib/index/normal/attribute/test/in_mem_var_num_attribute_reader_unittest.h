#ifndef __INDEXLIB_INMEMVARNUMATTRIBUTEREADERTEST_H
#define __INDEXLIB_INMEMVARNUMATTRIBUTEREADERTEST_H

#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/index/normal/attribute/accessor/in_mem_var_num_attribute_reader.h"
#include <autil/mem_pool/Pool.h>

IE_NAMESPACE_BEGIN(index);

class InMemVarNumAttributeReaderTest : public INDEXLIB_TESTBASE {
public:
    InMemVarNumAttributeReaderTest();
    ~InMemVarNumAttributeReaderTest();

    DECLARE_CLASS_NAME(InMemVarNumAttributeReaderTest);
public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestRead();
private:
    autil::mem_pool::Pool mPool;
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(InMemVarNumAttributeReaderTest, TestRead);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_INMEMVARNUMATTRIBUTEREADERTEST_H
