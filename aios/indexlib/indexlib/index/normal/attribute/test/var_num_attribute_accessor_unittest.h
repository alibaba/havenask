#ifndef __INDEXLIB_VARNUMATTRIBUTEACCESSORTEST_H
#define __INDEXLIB_VARNUMATTRIBUTEACCESSORTEST_H

#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/index/normal/attribute/accessor/var_num_attribute_accessor.h"
#include <autil/mem_pool/Pool.h>

IE_NAMESPACE_BEGIN(index);

class VarNumAttributeAccessorTest : public INDEXLIB_TESTBASE {
public:
    VarNumAttributeAccessorTest();
    ~VarNumAttributeAccessorTest();

    DECLARE_CLASS_NAME(VarNumAttributeAccessorTest);
public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestAddEncodedField();
    void TestUpdateEncodedField();
    void TestAddNormalField();
    void TestUpdateNormalField();
    void TestAppendData();
    void TestReadData();

private:
    autil::mem_pool::Pool mPool;
private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(VarNumAttributeAccessorTest, TestAddEncodedField);
INDEXLIB_UNIT_TEST_CASE(VarNumAttributeAccessorTest, TestUpdateEncodedField);
INDEXLIB_UNIT_TEST_CASE(VarNumAttributeAccessorTest, TestAppendData);
INDEXLIB_UNIT_TEST_CASE(VarNumAttributeAccessorTest, TestAddNormalField);
INDEXLIB_UNIT_TEST_CASE(VarNumAttributeAccessorTest, TestUpdateNormalField);
INDEXLIB_UNIT_TEST_CASE(VarNumAttributeAccessorTest, TestReadData);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_VARNUMATTRIBUTEACCESSORTEST_H
