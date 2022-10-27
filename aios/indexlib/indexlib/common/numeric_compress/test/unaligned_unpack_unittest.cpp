#include "indexlib/common_define.h"
#include "indexlib/test/unittest.h"
#include "indexlib/common/numeric_compress/unaligned_unpack.h"
#include "indexlib/common/numeric_compress/pfor_delta_compressor.h"
#include <vector>

using namespace std;

IE_NAMESPACE_BEGIN(common);

class UnAlignedUnpackTest : public INDEXLIB_TESTBASE
{
public:
    DECLARE_CLASS_NAME(UnAlignedUnpackTest);
    void CaseSetUp() override
    {
    }

    void CaseTearDown() override
    {
    }

    template <typename T>
    void TestCaseForUnpack()
    {
        for (size_t i = 1; i < 31; i++)
        {
            for (size_t frameBits = 1; frameBits < 31; ++frameBits)
            {
                vector<T> data;
                GenerateData<T>(data, i, (((T)1 << frameBits) - 1));
                TestCaseForUnpack<T>((T*)&(data[0]), (uint32_t)data.size(), frameBits);
            }
        }
    }

    void TestCaseForUnpackUInt32()
    {
        TestCaseForUnpack<uint32_t>();
    }


    void TestCaseForUnpackUInt16()
    {
        TestCaseForUnpack<uint16_t>();
    }

    void TestCaseForUnpackUInt8()
    {
        TestCaseForUnpack<uint8_t>();
    }


private:
    template <typename T>
    void GenerateData(vector<T>& data, size_t n, uint64_t maxValue)
    {
        data.resize(n);
        srand(time(NULL));
        for (size_t i = 0; i < n; ++i)
        {
            if ((T)(maxValue + 1) == 0)//overflow
            {
                data[i] = (T)rand();
            }
            else 
            {
                data[i] = ((T)rand() % (T)(maxValue + 1));
            }
        }
    }

    template <typename T>
    void TestCaseForUnpack(T* src, uint32_t n, size_t frameBits)
    {
        typedef void (*unpack_function)(T*, const uint32_t*, uint32_t n);
        unpack_function unpack_x[31] =
        {
            unaligned_unpack_1<T>,  unaligned_unpack_2<T>,  unaligned_unpack_3<T>,
            unaligned_unpack_4<T>,  unaligned_unpack_5<T>,  unaligned_unpack_6<T>,
            unaligned_unpack_7<T>,  unaligned_unpack_8<T>, unaligned_unpack_9<T>,
            unaligned_unpack_10<T>, unaligned_unpack_11<T>, unaligned_unpack_12<T>, 
            unaligned_unpack_13<T>, unaligned_unpack_14<T>, unaligned_unpack_15<T>, 
            unaligned_unpack_16<T>, unaligned_unpack_17<T>, unaligned_unpack_18<T>,
            unaligned_unpack_19<T>, unaligned_unpack_20<T>, unaligned_unpack_21<T>,
            unaligned_unpack_22<T>, unaligned_unpack_23<T>, unaligned_unpack_24<T>,
            unaligned_unpack_25<T>, unaligned_unpack_26<T>, unaligned_unpack_27<T>,
            unaligned_unpack_28<T>, unaligned_unpack_29<T>, unaligned_unpack_30<T>,
            unaligned_unpack_31<T>
        };

        uint32_t* encode = new uint32_t[n];
        memset(encode, 0, sizeof(uint32_t) * n);
        T* dest = new T[n];
        PforDeltaCompressor::Pack<T>(encode, src, n, frameBits);
        (*unpack_x[frameBits - 1])(dest, encode, n);

        for (uint32_t i = 0; i < n; ++i)
        {
            assert(src[i] == dest[i]);
            INDEXLIB_TEST_EQUAL(src[i], dest[i]);
        }
        delete[] encode;
        delete[] dest;
    }
};

INDEXLIB_UNIT_TEST_CASE(UnAlignedUnpackTest, TestCaseForUnpackUInt32);
INDEXLIB_UNIT_TEST_CASE(UnAlignedUnpackTest, TestCaseForUnpackUInt16);
INDEXLIB_UNIT_TEST_CASE(UnAlignedUnpackTest, TestCaseForUnpackUInt8);

IE_NAMESPACE_END(common);
