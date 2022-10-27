#include "build_service/test/unittest.h"
#include "build_service/reader/FilePatternBase.h"
#include "build_service/reader/test/MockFilePattern.h"

using namespace std;

namespace build_service {
namespace reader {

class FilePatternBaseTest : public BUILD_SERVICE_TESTBASE {
public:
    void setUp();
    void tearDown();
};

void FilePatternBaseTest::setUp() {
}

void FilePatternBaseTest::tearDown() {
}

TEST_F(FilePatternBaseTest, testSetDir) {
    MockFilePattern pattern;
    pattern.setDirPath("///a///b///c///");
    EXPECT_EQ(string("a/b/c"), pattern.getDirPath());
}

}
}
