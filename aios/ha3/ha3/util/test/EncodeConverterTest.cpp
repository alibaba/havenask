#include<unittest/unittest.h>
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/util/EncodeConverter.h>

using namespace std;
BEGIN_HA3_NAMESPACE(util);

class EncodeConverterTest : public TESTBASE {
public:
    EncodeConverterTest();
    ~EncodeConverterTest();
public:
    void setUp();
    void tearDown();
protected:
    HA3_LOG_DECLARE();
};

 HA3_LOG_SETUP(util, EncodeConverterTest);


EncodeConverterTest::EncodeConverterTest() { 
}

EncodeConverterTest::~EncodeConverterTest() { 
}

void EncodeConverterTest::setUp() { 
    HA3_LOG(DEBUG, "setUp!");
}

void EncodeConverterTest::tearDown() { 
    HA3_LOG(DEBUG, "tearDown!");
}

TEST_F(EncodeConverterTest, testUtf8ToUtf16) { 
    HA3_LOG(DEBUG, "Begin Test!");

    const char *str = "a阿　Ａ";
    uint16_t buf[128];
    int32_t len = EncodeConverter::utf8ToUtf16(str, strlen(str), buf);
    ASSERT_EQ(4, len);
    ASSERT_EQ(uint16_t('a'), buf[0]);
    ASSERT_EQ(uint16_t(38463), buf[1]);
    ASSERT_EQ(uint16_t(0x3000), buf[2]);
    ASSERT_EQ(uint16_t('A' + 0xFEE0), buf[3]);
}

TEST_F(EncodeConverterTest, testUtf8ToUtf16WithInvalidUtf8) { 
    HA3_LOG(DEBUG, "Begin Test!");

    const char *str = "\x74\xCF\xDD\xEF\xBF\xBF\xFF\x20\xEF\xBF\x59";
    uint16_t buf[128];
    int32_t len = EncodeConverter::utf8ToUtf16(str, strlen(str), buf);
    ASSERT_EQ(4, len);
    ASSERT_EQ(uint16_t(0x74), buf[0]);
    ASSERT_EQ(uint16_t(0xFFFF), buf[1]);
    ASSERT_EQ(uint16_t(0x20), buf[2]);
    ASSERT_EQ(uint16_t(0x59), buf[3]);
}

TEST_F(EncodeConverterTest, testUtf16ToUtf8) { 
    HA3_LOG(DEBUG, "Begin Test!");

    const char *str = "a阿　Ａ";
    int32_t slen = strlen(str);
    uint16_t buf1[128];
    char buf2[128 * 3];
    int32_t len1 = EncodeConverter::utf8ToUtf16(str, slen, buf1);
    int32_t len2 = EncodeConverter::utf16ToUtf8(buf1, len1, buf2);
    ASSERT_EQ(slen, len2);
    buf2[len2] = '\0';
    ASSERT_EQ(string(str), string(buf2));
}

TEST_F(EncodeConverterTest, testUtf16ToUtf8WithAllUtf16) { 
    HA3_LOG(DEBUG, "Begin Test!");

    vector<uint16_t> v16;
    for (int i = 0; i < 0x10000; ++i) {
        v16.push_back(i);
    }

    vector<char> v8(0x10000 * 3);
    int32_t len1 = EncodeConverter::utf16ToUtf8(&v16[0], 0x10000, &v8[0]);
    int32_t v8Len = 0x10000 * 3 - 0x800 - 0x80;
    ASSERT_EQ(v8Len, len1);
    int32_t len2 = EncodeConverter::utf8ToUtf16(&v8[0], v8Len, &v16[0]);

    ASSERT_EQ(0x10000, len2);
    bool f = true;
    for (int i = 0; i < 0x10000; ++i) {
        if (v16[i] != i)
            f = false;
    }

    ASSERT_TRUE(f);
}

END_HA3_NAMESPACE(util);

