#include "indexlib/common_define.h"
#include "indexlib/test/unittest.h"
#include "indexlib/util/key_hasher_typed.h"

using namespace std;

IE_NAMESPACE_USE(util);
IE_NAMESPACE_BEGIN(util);

class KeyHasherTypedTest : public INDEXLIB_TESTBASE
{
public:
    DECLARE_CLASS_NAME(KeyHasherTypedTest);
    void CaseSetUp() override
    {
    }

    void CaseTearDown() override
    {
    }

    void TestCaseForInvalidToken()
    {
        string invalidNumberStr("invalid1234");
        InternalTestGetHashKey<int8_t>(invalidNumberStr, false, 0);
        InternalTestGetHashKey<uint8_t>(invalidNumberStr, false, 0);
        InternalTestGetHashKey<int16_t>(invalidNumberStr, false, 0);
        InternalTestGetHashKey<uint16_t>(invalidNumberStr, false, 0);
        InternalTestGetHashKey<int32_t>(invalidNumberStr, false, 0);
        InternalTestGetHashKey<uint32_t>(invalidNumberStr, false, 0);
        InternalTestGetHashKey<int64_t>(invalidNumberStr, false, 0);
        InternalTestGetHashKey<uint64_t>(invalidNumberStr, false, 0);
    }

    void TestCaseForGetHashKey()
    {
        // int8_t 
        InternalTestGetHashKey<int8_t>("-1", true, (dictkey_t)-1);
        InternalTestGetHashKey<int8_t>("127", true, (dictkey_t)127);
        InternalTestGetHashKey<int8_t>("130", false, 0);

        InternalTestGetHashKey<int8_t>("-1", true, ((dictkey_t)-1 | ((dictkey_t)1 << 48)), 1);
        InternalTestGetHashKey<int8_t>("127", true, ((dictkey_t)127 | ((dictkey_t)1 << 48)), 1);
        InternalTestGetHashKey<int8_t>("130", false, 0, 1);

        // uint8_t
        InternalTestGetHashKey<uint8_t>("-1", false, (dictkey_t)0);
        InternalTestGetHashKey<uint8_t>("255", true, (dictkey_t)255);
        InternalTestGetHashKey<uint8_t>("260", false, (dictkey_t)255);

        InternalTestGetHashKey<uint8_t>("-1", false, (dictkey_t)0);
        InternalTestGetHashKey<uint8_t>("255", true, ((dictkey_t)255 | ((dictkey_t)2 << 48)), 2);
        InternalTestGetHashKey<uint8_t>("260", false, (dictkey_t)255);
        
        // int16_t
        InternalTestGetHashKey<int16_t>("-1", true, (dictkey_t)-1);
        InternalTestGetHashKey<int16_t>("32767", true, (dictkey_t)32767);
        InternalTestGetHashKey<int16_t>("32768", false, (dictkey_t)32768);

        InternalTestGetHashKey<int16_t>("-1", true, ((dictkey_t)-1 | ((dictkey_t)3 << 48)), 3);
        InternalTestGetHashKey<int16_t>("32767", true, ((dictkey_t)32767 | ((dictkey_t)3 << 48)), 3);
        InternalTestGetHashKey<int16_t>("32768", false, (dictkey_t)32768);

        // uint16_t
        InternalTestGetHashKey<uint16_t>("-1", false, (dictkey_t)-1);
        InternalTestGetHashKey<uint16_t>("32768", true, (dictkey_t)32768);
        InternalTestGetHashKey<uint16_t>("65535", true, (dictkey_t)65535);
        InternalTestGetHashKey<uint16_t>("65536", false, (dictkey_t)0);

        InternalTestGetHashKey<uint16_t>("-1", false, (dictkey_t)-1);
        InternalTestGetHashKey<uint16_t>("32768", true, ((dictkey_t)32768 | ((dictkey_t)4 << 48)), 4);
        InternalTestGetHashKey<uint16_t>("65535", true, ((dictkey_t)65535 | ((dictkey_t)4 << 48)), 4);
        InternalTestGetHashKey<uint16_t>("65536", false, (dictkey_t)0);

        // int32_t
        InternalTestGetHashKey<int32_t>("-1", true, (dictkey_t)-1);
        InternalTestGetHashKey<int32_t>("2147483647", true, (dictkey_t)2147483647);
        InternalTestGetHashKey<int32_t>("4294967295", false, (dictkey_t)4294967295);

        InternalTestGetHashKey<int32_t>("-1", true, ((dictkey_t)-1 | ((dictkey_t)5 << 48)), 5);
        InternalTestGetHashKey<int32_t>("2147483647", true, ((dictkey_t)2147483647 | ((dictkey_t)5 << 48)), 5);
        InternalTestGetHashKey<int32_t>("4294967295", false, (dictkey_t)4294967295);

        // uint32_t
        InternalTestGetHashKey<uint32_t>("-1", false, (dictkey_t)-1);
        InternalTestGetHashKey<uint32_t>("2147483647", true, (dictkey_t)2147483647);
        InternalTestGetHashKey<uint32_t>("4294967295", true, (dictkey_t)4294967295);        
        InternalTestGetHashKey<uint32_t>("4294967296", false, (dictkey_t)4294967295);        

        InternalTestGetHashKey<uint32_t>("-1", false, (dictkey_t)-1);
        InternalTestGetHashKey<uint32_t>("2147483647", true, ((dictkey_t)2147483647 | ((dictkey_t)6 << 48)), 6);
        InternalTestGetHashKey<uint32_t>("4294967295", true, ((dictkey_t)4294967295 | ((dictkey_t)6 << 48)), 6);
        InternalTestGetHashKey<uint32_t>("4294967296", false, (dictkey_t)4294967295);        

        // int64_t 
        InternalTestGetHashKey<int64_t>("-1", true, (dictkey_t)-1);
        InternalTestGetHashKey<int64_t>("4294967296", true, (dictkey_t)4294967296);
        InternalTestGetHashKey<int64_t>("9223372036854775807", true, (dictkey_t)9223372036854775807);
        InternalTestGetHashKey<int64_t>("9223372036854775808", false, (dictkey_t)0);

        InternalTestGetHashKey<int64_t>("-1", true, ((dictkey_t)-1 | ((dictkey_t)7 << 48)), 7);
        InternalTestGetHashKey<int64_t>("4294967296", true, ((dictkey_t)4294967296 | ((dictkey_t)7 << 48)), 7);
        InternalTestGetHashKey<int64_t>("9223372036854775807", true,
                ((dictkey_t)9223372036854775807 | ((dictkey_t)7 << 48)), 7);
        InternalTestGetHashKey<int64_t>("9223372036854775808", false, (dictkey_t)0);
        
        // uint64_t 
        InternalTestGetHashKey<uint64_t>("-1", false, (dictkey_t)-1);
        InternalTestGetHashKey<uint64_t>("9223372036854775808", true, (dictkey_t)9223372036854775808ul);
        InternalTestGetHashKey<uint64_t>("18446744073709551615", true, (dictkey_t)18446744073709551615ul);
        InternalTestGetHashKey<uint64_t>("18446744073709551616", false, (dictkey_t)0);

        InternalTestGetHashKey<uint64_t>("-1", false, (dictkey_t)-1);
        InternalTestGetHashKey<uint64_t>("9223372036854775808", true,
                ((dictkey_t)9223372036854775808ul | ((dictkey_t)8 << 48)), 8);
        InternalTestGetHashKey<uint64_t>("18446744073709551615", true,
                ((dictkey_t)18446744073709551615ul | ((dictkey_t)8 << 48)), 8);
        InternalTestGetHashKey<uint64_t>("18446744073709551616", false, (dictkey_t)0);
    }

    void TestCaseForGetHashKeyConstString()
    {
        auto Check = [](int32_t line, HashFunctionType hasherType, const char* data, size_t size,
                        bool expectedResult, dictkey_t expectedAnswer, bool sizeFlag = false) {
            string msg = string(__FILE__) + ":" + autil::StringUtil::toString(line) + ": Location";
            dictkey_t actualValue = 0;
            bool actualResult = GetHashKey(hasherType, autil::ConstString(data, size), actualValue);
            EXPECT_EQ(expectedResult, actualResult) << msg << endl;
            if (actualResult)
            {
                EXPECT_EQ(expectedAnswer, actualValue) << msg << endl;
            }
        };

        // hft_uint64
        Check(__LINE__, hft_uint64, "0", 1, true, 0);
        Check(__LINE__, hft_uint64, "0 ", 2, false, 0);
        Check(__LINE__, hft_uint64, "+0", 2, true, 0);
        Check(__LINE__, hft_uint64, "-0", 2, false, 0);
        Check(__LINE__, hft_uint64, "+1", 2, true, 1);
        Check(__LINE__, hft_uint64, "-1", 2, false, 1);
        Check(__LINE__, hft_uint64, "18446744073709551615", 20, true, 18446744073709551615ul);
        Check(__LINE__, hft_uint64, "  18446744073709551615", 22, true, 18446744073709551615ul);
        Check(__LINE__, hft_uint64, "\t18446744073709551615", 22, true, 18446744073709551615ul);
        Check(__LINE__, hft_uint64, "+ 1", 3, false, 0);
        Check(__LINE__, hft_uint64, "  18446744073709551616", 22, false, 0);
        Check(__LINE__, hft_uint64, "  184467440737095516150", 23, false, 0);
        Check(__LINE__, hft_uint64, "1F", 2, false, 0);
        Check(__LINE__, hft_uint64, "0x1", 3, false, 0);
        Check(__LINE__, hft_uint64, "0X1", 3, false, 0);
        Check(__LINE__, hft_uint64, "001", 3, true, 1);
        Check(__LINE__, hft_uint64, "00031", 5, true, 31);
        Check(__LINE__, hft_uint64, NULL, 10, false, 0);
        Check(__LINE__, hft_uint64, "\0", 1, false, 0);
        Check(__LINE__, hft_uint64, "X", 1, false, 0);
        Check(__LINE__, hft_uint64, "+", 1, false, 0);
        Check(__LINE__, hft_uint64, "-", 1, false, 0);
        char data[] = {'1', '2', '\0', '4'};
        Check(__LINE__, hft_uint64, data, 4, true, 12);
        data[0] = '\0';
        Check(__LINE__, hft_uint64, data, 1, false, 0);
        Check(__LINE__, hft_uint64, "1234", 4, true, 1234);
        Check(__LINE__, hft_uint64, "1234", 5, true, 1234);

        // hft_int64
        Check(__LINE__, hft_int64, "+0", 2, true, 0);
        Check(__LINE__, hft_int64, "-0", 2, true, 0);
        Check(__LINE__, hft_int64, "0 ", 2, false, 0);
        Check(__LINE__, hft_int64, "+1", 2, true, 1);
        Check(__LINE__, hft_int64, "-1", 2, true, -1);
        Check(__LINE__, hft_int64, "1", 1, true, 1);
        Check(__LINE__, hft_int64, " 1", 2, true, 1);
        Check(__LINE__, hft_int64, " +1", 3, true, 1);
        Check(__LINE__, hft_int64, "+ 1", 3, false, 0);
        Check(__LINE__, hft_int64, "\t1", 2, true, 1);
        Check(__LINE__, hft_int64, "\t-1", 3, true, -1);
        Check(__LINE__, hft_int64, "-\t1", 3, false, 0);
        Check(__LINE__, hft_int64, "+", 1, false, 0);
        Check(__LINE__, hft_int64, "-", 1, false, 0);

        Check(__LINE__, hft_int64, "+18446744073709551615", 21, false, 0);
        Check(__LINE__, hft_int64, "9223372036854775807", 19, true, 9223372036854775807ul);
        Check(__LINE__, hft_int64, "+9223372036854775806", 20, true, 9223372036854775806ul);
        Check(__LINE__, hft_int64, "+9223372036854775807", 20, true, 9223372036854775807ul);
        Check(__LINE__, hft_int64, "+9223372036854775808", 20, false, 0);
        Check(__LINE__, hft_int64, "-9223372036854775807", 20, true, 9223372036854775809ul);
        Check(__LINE__, hft_int64, "-9223372036854775808", 20, true, 9223372036854775808ul);
        Check(__LINE__, hft_int64, "-9223372036854775809", 20, false, 0);
        Check(__LINE__, hft_int64, "+92233720368547758070", 21, false, 0);
        Check(__LINE__, hft_int64, "-92233720368547758080", 21, false, 0);
        Check(__LINE__, hft_int64, "  +18446744073709551615", 23, false, 0);
        Check(__LINE__, hft_int64, "\t+18446744073709551615", 23, false, 0);
        Check(__LINE__, hft_int64, "-1", 2, true, 18446744073709551615ul);
        Check(__LINE__, hft_int64, "  18446744073709551616", 22, false, 0);
        Check(__LINE__, hft_int64, "  184467440737095516150", 23, false, 0);
        Check(__LINE__, hft_int64, "1F", 2, false, 0);
        Check(__LINE__, hft_int64, "0x1", 3, false, 0);
        Check(__LINE__, hft_int64, "0X1", 3, false, 0);
        Check(__LINE__, hft_int64, "001", 3, true, 1);
        Check(__LINE__, hft_int64, "00031", 5, true, 31);
        Check(__LINE__, hft_int64, NULL, 10, false, 0);
        Check(__LINE__, hft_int64, "\0", 1, false, 0);
        Check(__LINE__, hft_int64, "X", 1, false, 0);
        char data1[] = {'1', '2', '\0', '4'};
        Check(__LINE__, hft_int64, data1, 4, true, 12);
        data1[0] = '\0';
        Check(__LINE__, hft_int64, data1, 1, false, 0);
        Check(__LINE__, hft_int64, "1234", 4, true, 1234);
        Check(__LINE__, hft_int64, "1234", 5, true, 1234);

        // for const string size
        Check(__LINE__, hft_uint64, "1234", 1, true, 1);
        Check(__LINE__, hft_uint64, "+1234", 1, false, 1);
        Check(__LINE__, hft_uint64, "+1234", 2, true, 1);
        Check(__LINE__, hft_uint64, "1", 0, false, 0);
        Check(__LINE__, hft_int64, "1234", 1, true, 1);
        Check(__LINE__, hft_int64, "-1234", 1, false, 1);
        Check(__LINE__, hft_int64, "-1234", 2, true, (dictkey_t)-1);
        Check(__LINE__, hft_int64, "1", 0, false, 0);

        // hft_murmur
    }

private:
    template <typename KeyType>
    void InternalTestGetHashKey(const string &numberStr, bool expectedResult, dictkey_t expectedAnswer, int salt = -1)
    {
        KeyHasherPtr hasher(CreateHasher<KeyType>(salt));
        dictkey_t answer;
        bool ok = hasher->GetHashKey(numberStr.c_str(), answer);
        INDEXLIB_TEST_EQUAL(expectedResult, ok);
        if (ok)
        {
            size_t size = numberStr.size();
            dictkey_t newAnswer;
            INDEXLIB_TEST_TRUE(hasher->GetHashKey(numberStr.c_str(), size, newAnswer));
            INDEXLIB_TEST_EQUAL(newAnswer, answer);            
            INDEXLIB_TEST_EQUAL(expectedAnswer, answer);
        }
    }

    template <typename KeyType>
    KeyHasher* CreateHasher(int salt);
};

#define CreateHasherForUnsignedInt(BITS)                                \
    template<>                                                          \
    KeyHasher* KeyHasherTypedTest::CreateHasher<uint##BITS##_t>(int salt) \
    {                                                                   \
        return (salt == -1) ? new UInt##BITS##NumberHasher() : new UInt##BITS##NumberHasher((uint16_t)salt); \
    }

#define CreateHasherForSignedInt(BITS)                                  \
    template<>                                                          \
    KeyHasher* KeyHasherTypedTest::CreateHasher<int##BITS##_t>(int salt) \
    {                                                                   \
        return (salt == -1) ? new Int##BITS##NumberHasher() : new Int##BITS##NumberHasher((uint16_t)salt); \
    }

CreateHasherForUnsignedInt(8)
CreateHasherForUnsignedInt(16)
CreateHasherForUnsignedInt(32)
CreateHasherForUnsignedInt(64)

CreateHasherForSignedInt(8)
CreateHasherForSignedInt(16)
CreateHasherForSignedInt(32)
CreateHasherForSignedInt(64)

#undef CreateHasherForUnsignedInt
#undef CreateHasherForSignedInt

INDEXLIB_UNIT_TEST_CASE(KeyHasherTypedTest, TestCaseForInvalidToken);
INDEXLIB_UNIT_TEST_CASE(KeyHasherTypedTest, TestCaseForGetHashKey);
INDEXLIB_UNIT_TEST_CASE(KeyHasherTypedTest, TestCaseForGetHashKeyConstString);

IE_NAMESPACE_END(util);
