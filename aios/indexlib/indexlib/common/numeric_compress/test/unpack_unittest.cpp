#include "indexlib/common_define.h"
#include "indexlib/test/unittest.h"
#include "indexlib/common/numeric_compress/unpack.h"
#include "indexlib/common/numeric_compress/pfor_delta_compressor.h"
#include "indexlib/common/numeric_compress/new_pfordelta_compressor.h"
#include <vector>
#include "sys/mman.h"

using namespace std;

IE_NAMESPACE_BEGIN(common);

class UnpackTest : public INDEXLIB_TESTBASE
{
public:
    DECLARE_CLASS_NAME(UnpackTest);
    void CaseSetUp() override
    {
    }

    void CaseTearDown() override
    {
    }

    template <typename T>
    void TestCaseForUnpack()
    {
        for (size_t i = 1; i < 128; i++)
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

    void TestCaseForUnpack6Bug242177()
    {
        srand(time(NULL));
        uint32_t data[32];
        // all 32 integers are fit in 6 bit
        for (uint32_t i = 0; i < 32; ++i) {
            data[i] = rand() % 64;
        }

        // unreadable memory
        char *dump = (char *)mmap(NULL, 4096, PROT_NONE,
                MAP_SHARED | MAP_ANON, -1, 0);
        (void)dump;
        
        char *base = (char *)mmap(NULL, 4096, PROT_READ | PROT_WRITE,
                MAP_SHARED | MAP_ANON, -1, 0);
        // use pack_6 to pack these 32 integers, so we need 24 bytes.
        // store these 24 bytes from (base + 0xfe7) to (base + 0xfff)
        NewPForDeltaCompressor::Pack((uint32_t*)(base + 0xfe7), data, 32, 6);

        uint32_t dest[32];
        // unpack from (base + 0xfe7), if we read over boundary, it will go
        // "dump"'s memory, which can not be readed
        unpack_6<uint32_t>(dest, (uint32_t*)(base + 0xfe7), 32);

        for (uint32_t i = 0; i < 32; ++i) {
            INDEXLIB_TEST_EQUAL(data[i], dest[i]);            
        }
    }

    void TestCaseForUnpack21Bug9827870()
    {
        // test case for Unpack21 bug
        // for detail, see: https://aone.alibaba-inc.com/project/82726/issue/9827870
        // run with asan 
        srand(time(NULL));
        uint32_t data[3];
        for (size_t i = 0; i <3; ++i)
        {
            data[i] = rand() % 65535;
        }

        // three 21bit number takes 8 bytes
        uint32_t encodeBuf[2];
        memset(encodeBuf, 0, 2 * sizeof(uint32_t));
        NewPForDeltaCompressor::Pack((uint32_t*)encodeBuf, data, 3, 21);

        uint32_t decodeBuf[3];
        memset(decodeBuf, 0, 3 * sizeof(uint32_t));
        unpack_21<uint32_t>(decodeBuf, encodeBuf, 3);

        for (size_t i = 0; i < 3; ++i)
        {
            INDEXLIB_TEST_EQUAL(data[i], decodeBuf[i]);
        }
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

INDEXLIB_UNIT_TEST_CASE(UnpackTest, TestCaseForUnpackUInt32);
INDEXLIB_UNIT_TEST_CASE(UnpackTest, TestCaseForUnpackUInt16);
INDEXLIB_UNIT_TEST_CASE(UnpackTest, TestCaseForUnpackUInt8);
INDEXLIB_UNIT_TEST_CASE(UnpackTest, TestCaseForUnpack6Bug242177);
INDEXLIB_UNIT_TEST_CASE(UnpackTest, TestCaseForUnpack21Bug9827870);

IE_NAMESPACE_END(common);
