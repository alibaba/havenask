#ifndef __INDEXLIB_VARLENDATAACCESSORTEST_H
#define __INDEXLIB_VARLENDATAACCESSORTEST_H

#include "autil/mem_pool/Pool.h"
#include "indexlib/common_define.h"
#include "indexlib/index/data_structure/var_len_data_accessor.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

namespace indexlib { namespace index {

class VarLenDataAccessorTest : public INDEXLIB_TESTBASE
{
public:
    VarLenDataAccessorTest();
    ~VarLenDataAccessorTest();

    DECLARE_CLASS_NAME(VarLenDataAccessorTest);

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

// NEED FIX
// INDEXLIB_UNIT_TEST_CASE(VarLenDataAccessorTest, TestAddEncodedField);
// INDEXLIB_UNIT_TEST_CASE(VarLenDataAccessorTest, TestUpdateEncodedField);
INDEXLIB_UNIT_TEST_CASE(VarLenDataAccessorTest, TestAppendData);
INDEXLIB_UNIT_TEST_CASE(VarLenDataAccessorTest, TestAddNormalField);
INDEXLIB_UNIT_TEST_CASE(VarLenDataAccessorTest, TestUpdateNormalField);
INDEXLIB_UNIT_TEST_CASE(VarLenDataAccessorTest, TestReadData);
}} // namespace indexlib::index

#endif //__INDEXLIB_VARLENDATAACCESSORTEST_H
