#include "build_service/analyzer/Normalizer.h"
#include "build_service/test/unittest.h"

using namespace std;
using namespace autil;
using namespace build_service::util;

namespace build_service {
namespace analyzer {

class NormalizerTest : public BUILD_SERVICE_TESTBASE {
public:
    void setUp();
    void tearDown();
};


#define ASSERT_NORMALIZE(x, y)                  \
    do {                                        \
        string s1;                              \
        normalizer.normalize(y, s1);            \
        string s2(x);                           \
        ASSERT_EQ(s2, s1);                      \
    } while (0)

void NormalizerTest::setUp() {
}

void NormalizerTest::tearDown() {
}

TEST_F(NormalizerTest, testNoNormalize) {
    BS_LOG(DEBUG, "Begin Test!");

    Normalizer normalizer(NormalizeOptions(true, true, true));

    ASSERT_NORMALIZE("", "");
    ASSERT_NORMALIZE("aBc", "aBc");
    ASSERT_NORMALIZE("a C", "a C");
    string str = "\345\233\255\345\214\272\345\244\252\345\244\247\345\256\271\346\230\223\350\277\267\350\267\257\357\274\214\"\345\257\274\350\210\252\345\234\250\346\211\213\"\\\345\276\210\351\207\215\350\246\201\345\223\246!";
    ASSERT_NORMALIZE(str, str);
    str = "园区太大容易迷路，\"导航在手\"\\\\很重要哦!";
    ASSERT_NORMALIZE(str, str);
}

TEST_F(NormalizerTest, testNormalizeEmpty) {
    BS_LOG(DEBUG, "Begin Test!");

    Normalizer normalizer(NormalizeOptions(false, true, true));
    ASSERT_NORMALIZE("", "");
}

TEST_F(NormalizerTest, testNormalizeCase) {
    BS_LOG(DEBUG, "Begin Test!");

    Normalizer normalizer(NormalizeOptions(false, true, true));
    ASSERT_NORMALIZE("", "");
    ASSERT_NORMALIZE("abc", "aBc");
    ASSERT_NORMALIZE("a c", "a C");
}


#define ASSERT_NORMALIZET2S_S2D(x, y)           \
    do {                                        \
        string s1;                              \
        normalizer.normalize(y, s1);            \
        string s2(x);                           \
        ASSERT_EQ(s2, s1);                      \
    } while (0)

TEST_F(NormalizerTest, testNormalizeT2S) {
    BS_LOG(DEBUG, "Begin Test!");

    Normalizer normalizer(NormalizeOptions(true, false, false));
    ASSERT_NORMALIZET2S_S2D("", "");
    ASSERT_NORMALIZET2S_S2D("中华azAZ09,。", "中華ａｚＡＺ０９，。");
    ASSERT_NORMALIZET2S_S2D("", "\xdd\xdd");
}


}
}
