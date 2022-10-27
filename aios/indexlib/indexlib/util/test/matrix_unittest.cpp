#include "indexlib/util/test/matrix_unittest.h"
#include <autil/mem_pool/RecyclePool.h>

using namespace std;

IE_NAMESPACE_BEGIN(util);
IE_LOG_SETUP(util, MatrixTest);

MatrixTest::MatrixTest()
{
}

MatrixTest::~MatrixTest()
{
}

void MatrixTest::CaseSetUp()
{
}

void MatrixTest::CaseTearDown()
{
}

void MatrixTest::TestSimpleProcess()
{
    Matrix matrix(1);

    INDEXLIB_TEST_EQUAL((uint8_t)0, matrix.Capacity());
    INDEXLIB_TEST_EQUAL((uint8_t)0, matrix.Size());

    INDEXLIB_TEST_TRUE(matrix.PushBack(0, 100));
    INDEXLIB_TEST_EQUAL((uint8_t)2, matrix.Capacity());
    INDEXLIB_TEST_EQUAL((uint8_t)1, matrix.Size());
}

void MatrixTest::TestTwoRow()
{
    uint32_t expectMatrix[] = {
        100, 101, 102,
        200, 201, 202
    };
    CheckMatrix((uint32_t*)expectMatrix, 2, 3);
}

void MatrixTest::TestMaxCapacity()
{
    Matrix matrix(8);

    uint32_t expectMatrix[128 * 8];
    for (size_t c = 0; c < 128; ++c)
    {
        for (size_t r = 0; r < 8; ++r)
        {
            expectMatrix[c + r * 128] = random();
        }
    }
    CheckMatrix((uint32_t*)expectMatrix, 8, 128, &matrix);

    matrix.Clear();

    uint8_t capcity = 128;
    CheckMatrix((uint32_t*)expectMatrix, 8, 128, &matrix, &capcity);
}

void MatrixTest::TestInvalidPushBack()
{
    {
        // push same row repeatedly
        Matrix matrix(2);

        INDEXLIB_TEST_TRUE(matrix.PushBack(0, 100));
        INDEXLIB_TEST_TRUE(!matrix.PushBack(0, 200));
    }
    {
        // push when matrix is full and hit max capacity
        Matrix matrix(1);
        for (size_t col = 0; col < 128; ++col)
        {
            INDEXLIB_TEST_TRUE(matrix.PushBack(0, col));
        }
        INDEXLIB_TEST_TRUE(!matrix.PushBack(0, 128));
    }
    {
        Matrix matrix(1);
        INDEXLIB_TEST_TRUE(!matrix.PushBack(100, 100));
    }
}

void MatrixTest::TestSizeChange()
{
    Matrix matrix(2);

    INDEXLIB_TEST_TRUE(matrix.PushBack(0, 100));
    INDEXLIB_TEST_EQUAL((uint8_t)0, matrix.Size());

    INDEXLIB_TEST_TRUE(matrix.PushBack(1, 200));
    INDEXLIB_TEST_EQUAL((uint8_t)1, matrix.Size());
}

void MatrixTest::TestDisorderPushBack()
{
    Matrix matrix(3);

    INDEXLIB_TEST_EQUAL((uint8_t)0, matrix.Capacity());
    INDEXLIB_TEST_EQUAL((uint8_t)0, matrix.Size());

    INDEXLIB_TEST_TRUE(matrix.PushBack(2, 100));

    INDEXLIB_TEST_EQUAL((uint8_t)2, matrix.Capacity());
    INDEXLIB_TEST_EQUAL((uint8_t)0, matrix.Size());

    INDEXLIB_TEST_TRUE(matrix.PushBack(0, 100));

    INDEXLIB_TEST_EQUAL((uint8_t)2, matrix.Capacity());
    INDEXLIB_TEST_EQUAL((uint8_t)0, matrix.Size());

    INDEXLIB_TEST_TRUE(matrix.PushBack(1, 100));

    INDEXLIB_TEST_EQUAL((uint8_t)2, matrix.Capacity());
    INDEXLIB_TEST_EQUAL((uint8_t)1, matrix.Size());
}

void MatrixTest::TestClear()
{
    Matrix matrix(1);
    matrix.PushBack(0, 100);
    INDEXLIB_TEST_EQUAL(1, matrix.Size());
    INDEXLIB_TEST_EQUAL(2, matrix.Capacity());

    matrix.Clear();
    INDEXLIB_TEST_EQUAL(0, matrix.Size());
    INDEXLIB_TEST_EQUAL(2, matrix.Capacity());
}

void MatrixTest::CheckMatrix(uint32_t *expectMatrix, size_t rowNum, 
                             size_t colNum, Matrix* matrix, uint8_t* capacity)
{
    unique_ptr<Matrix> matrixPtr;
    if (!matrix)
    {
        matrix = new Matrix(rowNum);
        matrixPtr.reset(matrix);
    }
    uint8_t curCapacity = capacity == NULL ? 0 : *capacity;
    INDEXLIB_TEST_EQUAL(curCapacity, matrix->Capacity());
    INDEXLIB_TEST_EQUAL((uint8_t)0, matrix->Size());
    INDEXLIB_TEST_EQUAL(rowNum, matrix->GetRowCount());

    for (size_t c = 0; c < colNum; ++c)
    {
        for (size_t r = 0; r < rowNum; ++r)
        {
            INDEXLIB_TEST_TRUE(matrix->PushBack(r, expectMatrix[c + r * colNum]));
        }
        INDEXLIB_TEST_EQUAL((uint8_t)(c + 1), matrix->Size());
        uint8_t curCapacity = capacity == NULL ? matrix->AllocatePlan(c) : *capacity;
        INDEXLIB_TEST_EQUAL(curCapacity, matrix->Capacity());
    }

    CheckEqual(expectMatrix, rowNum, colNum, matrix);
}

void MatrixTest::CheckEqual(uint32_t* expectMatrix, size_t rowNum, size_t colNum,
                            Matrix* matrix)
{
    INDEXLIB_TEST_EQUAL(rowNum, (size_t)matrix->GetRowCount());
    INDEXLIB_TEST_EQUAL(colNum, (size_t)matrix->Size());
    for (size_t r = 0; r < rowNum; ++r)
    {
        Matrix::ValueType* row = matrix->GetRow(r);
        for (size_t c = 0; c < colNum; ++c)
        {
            INDEXLIB_TEST_EQUAL(expectMatrix[c + r * colNum], row[c]);
            INDEXLIB_TEST_EQUAL(expectMatrix[c + r * colNum], (*matrix)[r][c]);
        }
    }
}

void MatrixTest::TestCopyTypedValues()
{
    uint32_t buf32[256];
    for (size_t i = 0; i < 256; ++i)
    {
        buf32[i] = i;
    }

    uint8_t buf8[256];
    Matrix::CopyTypedValues(buf8, buf32, 256);

    uint16_t buf16[256];
    Matrix::CopyTypedValues(buf16, buf8, 256);

    for (size_t i = 0; i < 256; ++i)
    {
        INDEXLIB_TEST_EQUAL(buf32[i], (uint32_t)buf16[i]);
    }
}

void MatrixTest::TestSnapShot()
{
    Matrix matrix(2);    

    uint32_t expectMatrix[] = {
        100, 101, 102,
        200, 201, 202
    };
    CheckMatrix((uint32_t*)expectMatrix, 2, 3, &matrix);

    Matrix snapShotMatrix(2);
    INDEXLIB_TEST_TRUE(matrix.SnapShot(snapShotMatrix));

    CheckEqual((uint32_t*)expectMatrix, 2, 3, &snapShotMatrix);    
    INDEXLIB_TEST_EQUAL((uint8_t)3, snapShotMatrix.Capacity());
}

void MatrixTest::TestSnapShotWithNotEmptyMatrix()
{
    Matrix matrix(2);    
    uint32_t expectMatrix[] = {
        100,
        200
    };
    CheckMatrix((uint32_t*)expectMatrix, 2, 1, &matrix);

    uint32_t expectSnapShotMatrix[] = {
        100, 101, 102,
        200, 201, 202
    };
    Matrix snapShotMatrix(2);
    CheckMatrix((uint32_t*)expectSnapShotMatrix, 2, 3, &snapShotMatrix);

    INDEXLIB_TEST_TRUE(matrix.SnapShot(snapShotMatrix));

    CheckEqual((uint32_t*)expectMatrix, 2, 1, &snapShotMatrix);
    INDEXLIB_TEST_EQUAL((uint8_t)16, snapShotMatrix.Capacity());
}

IE_NAMESPACE_END(util);

