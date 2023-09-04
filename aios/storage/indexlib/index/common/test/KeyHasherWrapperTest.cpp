#include "indexlib/index/common/KeyHasherWrapper.h"

#include "indexlib/util/testutil/unittest.h"

using namespace std;

using namespace indexlib::util;
namespace indexlib::index {

class KeyHasherWrapperTest : public INDEXLIB_TESTBASE
{
public:
    DECLARE_CLASS_NAME(KeyHasherWrapperTest);
    void CaseSetUp() override {}

    void CaseTearDown() override {}

    void TestCaseForInvalidToken()
    {
        string invalidNumberStr("invalid1234");
        InternalTestGetHashKey(it_number_int8, invalidNumberStr, false, 0);
        InternalTestGetHashKey(it_number_uint8, invalidNumberStr, false, 0);
        InternalTestGetHashKey(it_number_int16, invalidNumberStr, false, 0);
        InternalTestGetHashKey(it_number_uint16, invalidNumberStr, false, 0);
        InternalTestGetHashKey(it_number_int32, invalidNumberStr, false, 0);
        InternalTestGetHashKey(it_number_uint32, invalidNumberStr, false, 0);
        InternalTestGetHashKey(it_number_int64, invalidNumberStr, false, 0);
        InternalTestGetHashKey(it_number_uint64, invalidNumberStr, false, 0);
    }

    void TestCaseForGetHashKey()
    {
        // int8_t
        InternalTestGetHashKey(it_number_int8, "-1", true, (dictkey_t)-1);
        InternalTestGetHashKey(it_number_int8, "127", true, (dictkey_t)127);
        InternalTestGetHashKey(it_number_int8, "130", false, 0);

        InternalTestGetHashKey(it_number_int8, "-1", true, ((dictkey_t)-1 | ((dictkey_t)1 << 48)), 1);
        InternalTestGetHashKey(it_number_int8, "127", true, ((dictkey_t)127 | ((dictkey_t)1 << 48)), 1);
        InternalTestGetHashKey(it_number_int8, "130", false, 0, 1);

        // uint8_t
        InternalTestGetHashKey(it_number_uint8, "-1", false, (dictkey_t)0);
        InternalTestGetHashKey(it_number_uint8, "255", true, (dictkey_t)255);
        InternalTestGetHashKey(it_number_uint8, "260", false, (dictkey_t)255);

        InternalTestGetHashKey(it_number_uint8, "-1", false, (dictkey_t)0);
        InternalTestGetHashKey(it_number_uint8, "255", true, ((dictkey_t)255 | ((dictkey_t)2 << 48)), 2);
        InternalTestGetHashKey(it_number_uint8, "260", false, (dictkey_t)255);

        // int16_t
        InternalTestGetHashKey(it_number_int16, "-1", true, (dictkey_t)-1);
        InternalTestGetHashKey(it_number_int16, "32767", true, (dictkey_t)32767);
        InternalTestGetHashKey(it_number_int16, "32768", false, (dictkey_t)32768);

        InternalTestGetHashKey(it_number_int16, "-1", true, ((dictkey_t)-1 | ((dictkey_t)3 << 48)), 3);
        InternalTestGetHashKey(it_number_int16, "32767", true, ((dictkey_t)32767 | ((dictkey_t)3 << 48)), 3);
        InternalTestGetHashKey(it_number_int16, "32768", false, (dictkey_t)32768);

        // uint16_t
        InternalTestGetHashKey(it_number_uint16, "-1", false, (dictkey_t)-1);
        InternalTestGetHashKey(it_number_uint16, "32768", true, (dictkey_t)32768);
        InternalTestGetHashKey(it_number_uint16, "65535", true, (dictkey_t)65535);
        InternalTestGetHashKey(it_number_uint16, "65536", false, (dictkey_t)0);

        InternalTestGetHashKey(it_number_uint16, "-1", false, (dictkey_t)-1);
        InternalTestGetHashKey(it_number_uint16, "32768", true, ((dictkey_t)32768 | ((dictkey_t)4 << 48)), 4);
        InternalTestGetHashKey(it_number_uint16, "65535", true, ((dictkey_t)65535 | ((dictkey_t)4 << 48)), 4);
        InternalTestGetHashKey(it_number_uint16, "65536", false, (dictkey_t)0);

        // int32_t
        InternalTestGetHashKey(it_number_int32, "-1", true, (dictkey_t)-1);
        InternalTestGetHashKey(it_number_int32, "2147483647", true, (dictkey_t)2147483647);
        InternalTestGetHashKey(it_number_int32, "4294967295", false, (dictkey_t)4294967295);

        InternalTestGetHashKey(it_number_int32, "-1", true, ((dictkey_t)-1 | ((dictkey_t)5 << 48)), 5);
        InternalTestGetHashKey(it_number_int32, "2147483647", true, ((dictkey_t)2147483647 | ((dictkey_t)5 << 48)), 5);
        InternalTestGetHashKey(it_number_int32, "4294967295", false, (dictkey_t)4294967295);

        // uint32_t
        InternalTestGetHashKey(it_number_uint32, "-1", false, (dictkey_t)-1);
        InternalTestGetHashKey(it_number_uint32, "2147483647", true, (dictkey_t)2147483647);
        InternalTestGetHashKey(it_number_uint32, "4294967295", true, (dictkey_t)4294967295);
        InternalTestGetHashKey(it_number_uint32, "4294967296", false, (dictkey_t)4294967295);

        InternalTestGetHashKey(it_number_uint32, "-1", false, (dictkey_t)-1);
        InternalTestGetHashKey(it_number_uint32, "2147483647", true, ((dictkey_t)2147483647 | ((dictkey_t)6 << 48)), 6);
        InternalTestGetHashKey(it_number_uint32, "4294967295", true, ((dictkey_t)4294967295 | ((dictkey_t)6 << 48)), 6);
        InternalTestGetHashKey(it_number_uint32, "4294967296", false, (dictkey_t)4294967295);

        // int64_t
        InternalTestGetHashKey(it_number_int64, "-1", true, (dictkey_t)-1);
        InternalTestGetHashKey(it_number_int64, "4294967296", true, (dictkey_t)4294967296);
        InternalTestGetHashKey(it_number_int64, "9223372036854775807", true, (dictkey_t)9223372036854775807);
        InternalTestGetHashKey(it_number_int64, "9223372036854775808", false, (dictkey_t)0);

        InternalTestGetHashKey(it_number_int64, "-1", true, ((dictkey_t)-1 | ((dictkey_t)7 << 48)), 7);
        InternalTestGetHashKey(it_number_int64, "4294967296", true, ((dictkey_t)4294967296 | ((dictkey_t)7 << 48)), 7);
        InternalTestGetHashKey(it_number_int64, "9223372036854775807", true,
                               ((dictkey_t)9223372036854775807 | ((dictkey_t)7 << 48)), 7);
        InternalTestGetHashKey(it_number_int64, "9223372036854775808", false, (dictkey_t)0);

        // uint64_t
        InternalTestGetHashKey(it_number_uint64, "-1", false, (dictkey_t)-1);
        InternalTestGetHashKey(it_number_uint64, "9223372036854775808", true, (dictkey_t)9223372036854775808ul);
        InternalTestGetHashKey(it_number_uint64, "18446744073709551615", true, (dictkey_t)18446744073709551615ul);
        InternalTestGetHashKey(it_number_uint64, "18446744073709551616", false, (dictkey_t)0);

        InternalTestGetHashKey(it_number_uint64, "-1", false, (dictkey_t)-1);
        InternalTestGetHashKey(it_number_uint64, "9223372036854775808", true,
                               ((dictkey_t)9223372036854775808ul | ((dictkey_t)8 << 48)), 8);
        InternalTestGetHashKey(it_number_uint64, "18446744073709551615", true,
                               ((dictkey_t)18446744073709551615ul | ((dictkey_t)8 << 48)), 8);
        InternalTestGetHashKey(it_number_uint64, "18446744073709551616", false, (dictkey_t)0);
    }

    void TestCaseForGetHashKeyConstString()
    {
        auto Check = [](int32_t line, HashFunctionType hasherType, const char* data, size_t size, bool expectedResult,
                        dictkey_t expectedAnswer, bool sizeFlag = false) {
            string msg = string(__FILE__) + ":" + autil::StringUtil::toString(line) + ": Location";
            dictkey_t actualValue = 0;
            bool actualResult = GetHashKey(hasherType, autil::StringView(data, size), actualValue);
            EXPECT_EQ(expectedResult, actualResult) << msg << endl;
            if (actualResult) {
                EXPECT_EQ(expectedAnswer, actualValue) << msg << endl;
            }
        };

        // hft_uint64
        Check(__LINE__, hft_uint64, "0", 1, true, 0);
        Check(__LINE__, hft_uint64, "0 ", 2, true, 0);
        Check(__LINE__, hft_uint64, "0d", 1, true, 0);
        Check(__LINE__, hft_uint64, "0d", 2, false, 0);
        Check(__LINE__, hft_uint64, "+0", 2, true, 0);
        Check(__LINE__, hft_uint64, "-0", 2, false, 0);
        Check(__LINE__, hft_uint64, "+1", 2, true, 1);
        Check(__LINE__, hft_uint64, "-1", 2, false, 1);
        Check(__LINE__, hft_uint64, "18446744073709551615", 20, true, 18446744073709551615ul);
        Check(__LINE__, hft_uint64, "  18446744073709551615", 22, true, 18446744073709551615ul);
        Check(__LINE__, hft_uint64, "\t18446744073709551615", 22, false, 18446744073709551615ul);
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
        Check(__LINE__, hft_uint64, data, 4, false, 12);
        data[0] = '\0';
        Check(__LINE__, hft_uint64, data, 1, false, 0);
        Check(__LINE__, hft_uint64, "1234", 4, true, 1234);
        Check(__LINE__, hft_uint64, "1234", 5, false, 1234);

        // hft_int64
        Check(__LINE__, hft_int64, "+0", 2, true, 0);
        Check(__LINE__, hft_int64, "-0", 2, true, 0);
        Check(__LINE__, hft_int64, "0 ", 2, true, 0);
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
        Check(__LINE__, hft_int64, data1, 4, false, 12);
        data1[0] = '\0';
        Check(__LINE__, hft_int64, data1, 1, false, 0);
        Check(__LINE__, hft_int64, "1234", 4, true, 1234);
        Check(__LINE__, hft_int64, "1234", 5, false, 1234);

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
    void InternalTestGetHashKey(InvertedIndexType indexType, const string& numberStr, bool expectedResult,
                                dictkey_t expectedAnswer, int salt = -1)
    {
        dictkey_t answer;
        bool ok;
        if (salt == -1) {
            ok =
                KeyHasherWrapper::GetHashKeyByInvertedIndexType(indexType, numberStr.c_str(), numberStr.size(), answer);
        } else {
            ok = KeyHasherWrapper::GetHashKeyByInvertedIndexType(indexType, numberStr.c_str(), numberStr.size(), answer,
                                                                 salt);
        }
        ASSERT_EQ(expectedResult, ok);
        if (ok) {
            size_t size = numberStr.size();
            dictkey_t newAnswer;
            if (salt == -1) {
                ASSERT_TRUE(
                    KeyHasherWrapper::GetHashKeyByInvertedIndexType(indexType, numberStr.c_str(), size, newAnswer));
            } else {
                ASSERT_TRUE(KeyHasherWrapper::GetHashKeyByInvertedIndexType(indexType, numberStr.c_str(), size,
                                                                            newAnswer, salt));
            }
            ASSERT_EQ(newAnswer, answer);
            ASSERT_EQ(expectedAnswer, answer);
        }
    }
};

INDEXLIB_UNIT_TEST_CASE(KeyHasherWrapperTest, TestCaseForInvalidToken);
INDEXLIB_UNIT_TEST_CASE(KeyHasherWrapperTest, TestCaseForGetHashKey);
INDEXLIB_UNIT_TEST_CASE(KeyHasherWrapperTest, TestCaseForGetHashKeyConstString);
} // namespace indexlib::index
