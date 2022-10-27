#include<unittest/unittest.h>
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/util/NormalizeTable.h>
#include <ha3/util/EncodeConverter.h>

BEGIN_HA3_NAMESPACE(util);

class NormalizeTableTest : public TESTBASE {
public:
    NormalizeTableTest();
    ~NormalizeTableTest();
public:
    void setUp();
    void tearDown();
protected:
    HA3_LOG_DECLARE();
};

 HA3_LOG_SETUP(util, NormalizeTableTest);


NormalizeTableTest::NormalizeTableTest() { 
}

NormalizeTableTest::~NormalizeTableTest() { 
}

void NormalizeTableTest::setUp() { 
    HA3_LOG(DEBUG, "setUp!");
}

void NormalizeTableTest::tearDown() { 
    HA3_LOG(DEBUG, "tearDown!");
}

TEST_F(NormalizeTableTest, testU2LAndS2D) { 
    HA3_LOG(DEBUG, "Begin Test!");
    const char *str = "ａｚＡＺ０９　，．；：＇＂！～";
    uint16_t u16[64];
    EncodeConverter::utf8ToUtf16(str, strlen(str), u16);
    const uint16_t width_a = u16[0];
    const uint16_t width_z = u16[1];
    const uint16_t width_A = u16[2];
    const uint16_t width_Z = u16[3];
    const uint16_t width_0 = u16[4];
    const uint16_t width_9 = u16[5];
    const uint16_t width_space = u16[6];
    const uint16_t width_comma = u16[7];
    const uint16_t width_period = u16[8];
    const uint16_t width_semicolon = u16[9];
    const uint16_t width_colon = u16[10];
    const uint16_t width_singleQuote = u16[11];
    const uint16_t width_Qutotes = u16[12];
    const uint16_t width_ExclamationMark = u16[13];
    const uint16_t width_tilde = u16[14];

    {
        NormalizeTable table(false, false, false, NULL);
        ASSERT_EQ(uint16_t('a'), table['a']);
        ASSERT_EQ(uint16_t('z'), table['z']);
        ASSERT_EQ(uint16_t('a'), table['A']);
        ASSERT_EQ(uint16_t('z'), table['Z']);
        ASSERT_EQ(uint16_t(' '), table[width_space]);
        ASSERT_EQ(uint16_t('a'), table[width_a]);
        ASSERT_EQ(uint16_t('z'), table[width_z]);
        ASSERT_EQ(uint16_t('a'), table[width_A]);
        ASSERT_EQ(uint16_t('z'), table[width_Z]);
        ASSERT_EQ(uint16_t('0'), table[width_0]);
        ASSERT_EQ(uint16_t('9'), table[width_9]);
        ASSERT_EQ(uint16_t('a' - 1), table[width_a - 1]);
        ASSERT_EQ(uint16_t('z' + 1), table[width_z + 1]);
        ASSERT_EQ(uint16_t('A' - 1), table[width_A - 1]);
        ASSERT_EQ(uint16_t('Z' + 1), table[width_Z + 1]);
        ASSERT_EQ(uint16_t('0' - 1), table[width_0 - 1]);
        ASSERT_EQ(uint16_t('9' + 1), table[width_9 + 1]);
        ASSERT_EQ(uint16_t(','), table[width_comma]);
        ASSERT_EQ(uint16_t('.'), table[width_period]);
        ASSERT_EQ(uint16_t(';'), table[width_semicolon]);
        ASSERT_EQ(uint16_t(':'), table[width_colon]);
        ASSERT_EQ(uint16_t('\''), table[width_singleQuote]);
        ASSERT_EQ(uint16_t('"'), table[width_Qutotes]);
        ASSERT_EQ(uint16_t('!'), table[width_ExclamationMark]);
        ASSERT_EQ(uint16_t('~'), table[width_tilde]);
        ASSERT_EQ(uint16_t(0xFF5F), table[width_tilde + 1]);
        ASSERT_EQ(uint16_t(0xFF00), table[width_ExclamationMark - 1]);
    }

    {
        NormalizeTable table(true, false, false, NULL);
        ASSERT_EQ(uint16_t('a'), table['a']);
        ASSERT_EQ(uint16_t('z'), table['z']);
        ASSERT_EQ(uint16_t('A'), table['A']);
        ASSERT_EQ(uint16_t('Z'), table['Z']);
        ASSERT_EQ(uint16_t(' '), table[width_space]);
        ASSERT_EQ(uint16_t('a'), table[width_a]);
        ASSERT_EQ(uint16_t('z'), table[width_z]);
        ASSERT_EQ(uint16_t('A'), table[width_A]);
        ASSERT_EQ(uint16_t('Z'), table[width_Z]);
        ASSERT_EQ(uint16_t('0'), table[width_0]);
        ASSERT_EQ(uint16_t('9'), table[width_9]);
    }

    {
        NormalizeTable table(false, false, true, NULL);
        ASSERT_EQ(uint16_t('a'), table['a']);
        ASSERT_EQ(uint16_t('z'), table['z']);
        ASSERT_EQ(uint16_t('a'), table['A']);
        ASSERT_EQ(uint16_t('z'), table['Z']);
        ASSERT_EQ(uint16_t(width_space), table[width_space]);
        ASSERT_EQ(uint16_t(width_a), table[width_a]);
        ASSERT_EQ(uint16_t(width_z), table[width_z]);
        ASSERT_EQ(uint16_t(width_a), table[width_A]);
        ASSERT_EQ(uint16_t(width_z), table[width_Z]);
        ASSERT_EQ(uint16_t(width_0), table[width_0]);
        ASSERT_EQ(uint16_t(width_9), table[width_9]);
    }

    {
        NormalizeTable table(true, false, true, NULL);
        ASSERT_EQ(uint16_t('a'), table['a']);
        ASSERT_EQ(uint16_t('z'), table['z']);
        ASSERT_EQ(uint16_t('A'), table['A']);
        ASSERT_EQ(uint16_t('Z'), table['Z']);
        ASSERT_EQ(uint16_t(width_space), table[width_space]);
        ASSERT_EQ(uint16_t(width_a), table[width_a]);
        ASSERT_EQ(uint16_t(width_z), table[width_z]);
        ASSERT_EQ(uint16_t(width_A), table[width_A]);
        ASSERT_EQ(uint16_t(width_Z), table[width_Z]);
        ASSERT_EQ(uint16_t(width_0), table[width_0]);
        ASSERT_EQ(uint16_t(width_9), table[width_9]);
    }
}

TEST_F(NormalizeTableTest, testT2S) { 
    HA3_LOG(DEBUG, "Begin Test!");
    const char *str1 = "中华人民共和国";
    const char *str2 = "中華人民共和國";
    uint16_t u1[64];
    uint16_t u2[64];
    int32_t len1 = EncodeConverter::utf8ToUtf16(str1, strlen(str1), u1);
    int32_t len2 = EncodeConverter::utf8ToUtf16(str2, strlen(str2), u2);
    ASSERT_EQ(len1, len2);

    NormalizeTable table1(false, true, false, NULL);
    NormalizeTable table2(false, false, false, NULL);
    for (int i = 0; i < len1; ++i) {
        ASSERT_EQ(u1[i], table1[u1[i]]);
        ASSERT_EQ(u2[i], table1[u2[i]]);

        ASSERT_EQ(u1[i], table2[u1[i]]);
        ASSERT_EQ(u1[i], table2[u2[i]]);
    }
}

TEST_F(NormalizeTableTest, testT2SWithPatch) { 
    HA3_LOG(DEBUG, "Begin Test!");
    const char *str1 = "乾隆";
    const char *str2 = "著作";
    uint16_t u1[64];
    uint16_t u2[64];
    int32_t len1 = EncodeConverter::utf8ToUtf16(str1, strlen(str1), u1);
    int32_t len2 = EncodeConverter::utf8ToUtf16(str2, strlen(str2), u2);
    ASSERT_EQ(len1, len2);

    std::map<uint16_t, uint16_t> patch;
    patch[20094] = 20094;
    patch[33879] = 33879;

    NormalizeTable table(false, false, false, &patch);
    for (int i = 0; i < len1; ++i) {
        ASSERT_EQ(u1[i], table[u1[i]]);
        ASSERT_EQ(u2[i], table[u2[i]]);
    }
}

END_HA3_NAMESPACE(util);

