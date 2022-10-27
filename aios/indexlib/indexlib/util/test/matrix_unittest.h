#ifndef __INDEXLIB_MATRIXTEST_H
#define __INDEXLIB_MATRIXTEST_H

#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/util/matrix.h"

IE_NAMESPACE_BEGIN(util);

class MatrixTest : public INDEXLIB_TESTBASE {
public:
    MatrixTest();
    ~MatrixTest();

    DECLARE_CLASS_NAME(MatrixTest);
public:
    void CaseSetUp() override;
    void CaseTearDown() override;

    void TestSimpleProcess();
    void TestTwoRow();
    void TestMaxCapacity();
    void TestInvalidPushBack();
    void TestSizeChange();
    void TestClear();
    void TestDisorderPushBack();
    void TestCopyTypedValues();
    void TestSnapShot();
    void TestSnapShotWithNotEmptyMatrix();

private:
    void CheckMatrix(uint32_t* expectMatrix, size_t rowNum, 
                     size_t colNum, Matrix* matrix = NULL,
                     uint8_t* capacity = NULL);

    void CheckEqual(uint32_t* expectMatrix, size_t rowNum, 
                    size_t colNum, Matrix* matrix);

private:
    static const size_t CHUNK_SIZE_FOR_UT = 100 * 1024;

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(MatrixTest, TestSimpleProcess);
INDEXLIB_UNIT_TEST_CASE(MatrixTest, TestTwoRow);
INDEXLIB_UNIT_TEST_CASE(MatrixTest, TestMaxCapacity);
INDEXLIB_UNIT_TEST_CASE(MatrixTest, TestInvalidPushBack);
INDEXLIB_UNIT_TEST_CASE(MatrixTest, TestSizeChange);
INDEXLIB_UNIT_TEST_CASE(MatrixTest, TestClear);
INDEXLIB_UNIT_TEST_CASE(MatrixTest, TestDisorderPushBack);
INDEXLIB_UNIT_TEST_CASE(MatrixTest, TestCopyTypedValues);
INDEXLIB_UNIT_TEST_CASE(MatrixTest, TestSnapShot);
INDEXLIB_UNIT_TEST_CASE(MatrixTest, TestSnapShotWithNotEmptyMatrix);

IE_NAMESPACE_END(util);

#endif //__INDEXLIB_MATRIXTEST_H
