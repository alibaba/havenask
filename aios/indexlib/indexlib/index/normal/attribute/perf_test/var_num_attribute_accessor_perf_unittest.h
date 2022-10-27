#ifndef __INDEXLIB_VARNUMATTRIBUTEACCESSORPERFTEST_H
#define __INDEXLIB_VARNUMATTRIBUTEACCESSORPERFTEST_H

#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/index/normal/attribute/accessor/var_num_attribute_accessor.h"
#include <autil/mem_pool/Pool.h>

IE_NAMESPACE_BEGIN(index);

class VarNumAttributeAccessorPerfTest : public INDEXLIB_TESTBASE {
public:
    VarNumAttributeAccessorPerfTest();
    ~VarNumAttributeAccessorPerfTest();

    DECLARE_CLASS_NAME(VarNumAttributeAccessorPerfTest);
public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestAddNormalField();
    void TestAddEncodedField();
private:
    autil::mem_pool::Pool mPool;
private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(VarNumAttributeAccessorPerfTest, TestAddNormalField);
INDEXLIB_UNIT_TEST_CASE(VarNumAttributeAccessorPerfTest, TestAddEncodedField);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_VARNUMATTRIBUTEACCESSORPERFTEST_H
