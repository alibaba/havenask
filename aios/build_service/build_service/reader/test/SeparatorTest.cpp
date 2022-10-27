#include "build_service/test/unittest.h"
#include "build_service/reader/Separator.h"

using namespace std;
using namespace testing;

namespace build_service {
namespace reader {

class SeparatorTest : public BUILD_SERVICE_TESTBASE {
public:
    void setUp();
    void tearDown();
};

void SeparatorTest::setUp() {
}

void SeparatorTest::tearDown() {
}

TEST_F(SeparatorTest, testNextBuffer) {
    Separator sep("abbccccdef");
    EXPECT_EQ(0, sep._next[(int)'a']);
    EXPECT_EQ(-2, sep._next[(int)'b']);
    EXPECT_EQ(-6, sep._next[(int)'c']);
    EXPECT_EQ(-7, sep._next[(int)'d']);
    EXPECT_EQ(-8, sep._next[(int)'e']);
    EXPECT_EQ(-9, sep._next[(int)'f']);
    EXPECT_EQ(1, sep._next[(int)'g']);
}

static inline int findFirst(const string &sepStr, const string &bufferString) {
    Separator sep(sepStr);
    const char *ret = sep.findInBuffer(bufferString.data(), 
            bufferString.data() + bufferString.size());
    if (ret == NULL) return -1;
    return ret - bufferString.data();
}

TEST_F(SeparatorTest, testFindSuccess) {
    EXPECT_EQ(0, findFirst("", "abc"));
    EXPECT_EQ(0, findFirst("a", "a"));
    EXPECT_EQ(0, findFirst("a", "aaasepaaaa"));
    EXPECT_EQ(3, findFirst("a", "bcda"));
    EXPECT_EQ(3, findFirst("a", "bcdabb"));
    EXPECT_EQ(3, findFirst("sep", "aaasepaaaa"));
    EXPECT_EQ(1, findFirst("aba", "aababbababa"));
    EXPECT_EQ(11, findFirst("aabbcc", "ababjhaabbcaabbcc"));
    EXPECT_EQ(12, findFirst("aabbcc", "aabbcdaabbcdaabbcc"));
    EXPECT_EQ(0, findFirst("aabbcc", "aabbccaabbcdaabbcc"));
    EXPECT_EQ(1, findFirst("aabbcc", "1aabbccaabbcdaabbcc"));
    EXPECT_EQ(2, findFirst("aabbcc", "22aabbccaabbcdaabbcc"));
    EXPECT_EQ(3, findFirst("aabbcc", "333aabbccaabbcdaabbcc"));
    EXPECT_EQ(4, findFirst("aabbcc", "4444aabbccaabbcdaabbcc"));
    EXPECT_EQ(5, findFirst("aabbcc", "55555aabbccaabbcdaabbcc"));
    EXPECT_EQ(6, findFirst("aabbcc", "666666aabbccaabbcdaabbcc"));
    EXPECT_EQ(7, findFirst("aabbcc", "7777777aabbccaabbcdaabbcc"));
    EXPECT_EQ(9, findFirst("aaab", "aaaaaaaaaaaab"));
    EXPECT_EQ(9, findFirst("aaab", "aaaaaaaaaaaabaaaaaaaa"));
    EXPECT_EQ(9, findFirst("aaab", "aaaaaaaaaaaabbbbbbbbb"));
    EXPECT_EQ(8, findFirst("aabbcca", "aabbccssaabbcca"));
    EXPECT_EQ(0, findFirst("aabbcca", "aabbccaajhfjdks"));
    EXPECT_EQ(0, findFirst("aabbcca", "aabbcca"));
}


TEST_F(SeparatorTest, testFindFailed) {
    EXPECT_EQ(-1, findFirst("a", "bcdef"));
    EXPECT_EQ(-1, findFirst("aabbcc", "aabbc"));
    EXPECT_EQ(-1, findFirst("aabbcc", "aabbcaabbc"));
    EXPECT_EQ(-1, findFirst("aaaaaa", "aaaaabaaaaabaaaaabaaaaab"));
}

}
}
