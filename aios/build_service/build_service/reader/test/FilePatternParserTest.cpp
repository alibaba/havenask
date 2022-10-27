#include "build_service/test/unittest.h"
#include "build_service/reader/FilePatternParser.h"
#include "build_service/reader/FilePattern.h"
#include "build_service/reader/EmptyFilePattern.h"

using namespace std;

namespace build_service {
namespace reader {

class FilePatternParserTest : public BUILD_SERVICE_TESTBASE {
public:
    void setUp();
    void tearDown();
};

void FilePatternParserTest::setUp() {
}

void FilePatternParserTest::tearDown() {
}

TEST_F(FilePatternParserTest, testParse) {
    {
        // file pattern str is empty
        FilePatterns patterns = FilePatternParser::parse("", "");
        EXPECT_EQ((size_t)1, patterns.size());
        ASSERT_CAST_AND_RETURN(EmptyFilePattern, patterns[0].get());
    }
    {
        FilePatterns patterns = FilePatternParser::parse("common-<8>.gz", "");
        FilePattern *pattern = ASSERT_CAST_AND_RETURN(FilePattern, patterns[0].get());
        EXPECT_EQ((size_t)1, patterns.size());
        EXPECT_EQ(FilePattern("", "common-", ".gz", 8), *pattern);
    }
    {
        FilePatterns patterns = FilePatternParser::parse("aa/common-<8>.gz;bb/common-<16>.gz", "");
        EXPECT_EQ((size_t)2, patterns.size());
        FilePattern *pattern1 = ASSERT_CAST_AND_RETURN(FilePattern, patterns[0].get());
        FilePattern *pattern2 = ASSERT_CAST_AND_RETURN(FilePattern, patterns[1].get());
        EXPECT_EQ(FilePattern("//aa//", "common-", ".gz", 8), *pattern1);
        EXPECT_EQ(FilePattern("bb/", "common-", ".gz", 16), *pattern2);
    }
    {
        FilePatterns patterns = FilePatternParser::parse(
                "aa//common-<8>.gz;bb/common-<16>.gz;aa/common-<8>.gz", "");
        EXPECT_EQ((size_t)2, patterns.size());
        FilePattern *pattern1 = ASSERT_CAST_AND_RETURN(FilePattern, patterns[0].get());
        FilePattern *pattern2 = ASSERT_CAST_AND_RETURN(FilePattern, patterns[1].get());
        EXPECT_EQ(FilePattern("aa/", "common-", ".gz", 8), *pattern1);
        EXPECT_EQ(FilePattern("bb/", "common-", ".gz", 16), *pattern2);
    }
    {
        EXPECT_TRUE(FilePatternParser::parse(
                        "aa//common-<8>.gz;bb/common-<16>.gz;aa/common-<16>.gz", "").empty());
        EXPECT_TRUE(FilePatternParser::parse(
                        "invalid_pattern_str", "").empty());
    }
}


}
}
