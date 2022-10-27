#include "indexlib/common_define.h"
#include "indexlib/test/unittest.h"
#include "indexlib/common/numeric_compress/pack.h"
#include "indexlib/common/numeric_compress/unpack.h"
#include "indexlib/common/numeric_compress/pfor_delta_compressor.h"
#include <vector>

using namespace std;

IE_NAMESPACE_BEGIN(common);

class PackTest : public INDEXLIB_TESTBASE
{
public:
    DECLARE_CLASS_NAME(PackTest);
    void CaseSetUp() override
    {
    }

    void CaseTearDown() override
    {
    }

    template <typename T>
    void TestCaseForPack()
    {
        for (size_t i = 1; i < 128; i++)
        {
            for (size_t frameBits = 1; frameBits < 31; ++frameBits)
            {
                vector<T> data;
                GenerateData<T>(data, i, (((T)1 << frameBits) - 1));
                TestCaseForPack<T>((T*)&(data[0]), (uint32_t)data.size(), frameBits);
            }
        }
    }

    void TestCaseForPackUInt32()
    {
        TestCaseForPack<uint32_t>();
    }


    void TestCaseForPackUInt16()
    {
        TestCaseForPack<uint16_t>();
    }

    void TestCaseForPackUInt8()
    {
        TestCaseForPack<uint8_t>();
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
    void TestCaseForPack(T* src, uint32_t n, size_t frameBits)
    {
        typedef void (*unpack_function)(T*, const uint32_t*, uint32_t n);
        static unpack_function unpack_x[31] =
        {
            unpack_1<T>,  unpack_2<T>,  unpack_3<T>,
            unpack_4<T>,  unpack_5<T>,  unpack_6<T>,
            unpack_7<T>,  unpack_8<T>, unpack_9<T>,
            unpack_10<T>, unpack_11<T>, unpack_12<T>, 
            unpack_13<T>, unpack_14<T>, unpack_15<T>, 
            unpack_16<T>, unpack_17<T>, unpack_18<T>,
            unpack_19<T>, unpack_20<T>, unpack_21<T>,
            unpack_22<T>, unpack_23<T>, unpack_24<T>,
            unpack_25<T>, unpack_26<T>, unpack_27<T>,
            unpack_28<T>, unpack_29<T>, unpack_30<T>,
            unpack_31<T>
        };

        typedef void (*pack_function)(uint32_t* , const T*, uint32_t n);
        static pack_function pack_x[31] =
        {
            pack_1<T>,  pack_2<T>,  pack_3<T>,
            pack_4<T>,  pack_5<T>,  pack_6<T>,
            pack_7<T>,  pack_8<T>, pack_9<T>,
            pack_10<T>, pack_11<T>, pack_12<T>, 
            pack_13<T>, pack_14<T>, pack_15<T>, 
            pack_16<T>, pack_17<T>, pack_18<T>,
            pack_19<T>, pack_20<T>, pack_21<T>,
            pack_22<T>, pack_23<T>, pack_24<T>,
            pack_25<T>, pack_26<T>, pack_27<T>,
            pack_28<T>, pack_29<T>, pack_30<T>,
            pack_31<T>
        };

        uint32_t* encode = new uint32_t[n];
        memset(encode, 0, n * sizeof(uint32_t));
        (*pack_x[frameBits - 1])(encode, src, n);

        T* dest = new T[n];
        (*unpack_x[frameBits - 1])(dest, encode, n);

        for (uint32_t i = 0; i < n; ++i)
        {
            if (src[i] != dest[i])
            {
                cout << "frameBits: " << frameBits << "i: " << i << endl;
                assert(src[i] == dest[i]);
            }
            INDEXLIB_TEST_EQUAL(src[i], dest[i]);
        }
        delete[] encode;
        delete[] dest;
    }
};

INDEXLIB_UNIT_TEST_CASE(PackTest, TestCaseForPackUInt32);
INDEXLIB_UNIT_TEST_CASE(PackTest, TestCaseForPackUInt16);
INDEXLIB_UNIT_TEST_CASE(PackTest, TestCaseForPackUInt8);

IE_NAMESPACE_END(common);
