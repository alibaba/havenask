#include "suez/deploy/IndexChecker.h"

#include <atomic>
#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>
#include <iosfwd>
#include <string>
#include <thread>

#include "fslib/common/common_type.h"
#include "fslib/fs/File.h"
#include "fslib/fs/FileSystem.h"
#include "suez/common/InnerDef.h"
#include "suez/sdk/PathDefine.h"
#include "unittest/unittest.h"

using namespace std;
using namespace testing;
using namespace fslib;
using namespace fslib::fs;

namespace suez {

class IndexCheckerTest : public TESTBASE {
public:
    void setUp();
    void tearDown();

private:
    string _indexRoot;
};

void IndexCheckerTest::setUp() { _indexRoot = GET_TEST_DATA_PATH() + "/indexRoot/"; }

void IndexCheckerTest::tearDown() { FileSystem::remove(_indexRoot); }

TEST_F(IndexCheckerTest, testInvalid) {
    IndexChecker checker;
    EXPECT_EQ(DS_FAILED, checker.waitIndexReady("invalid://abcdefg/ivalid_file"));
}

TEST_F(IndexCheckerTest, testSimple) {
    IndexChecker checker;
    auto checkFile = PathDefine::join(_indexRoot, "checkFile");
    ASSERT_EQ(EC_OK, FileSystem::writeFile(checkFile, ""));
    EXPECT_EQ(DS_DEPLOYDONE, checker.waitIndexReady(PathDefine::join(_indexRoot, "checkFile")));
}

TEST_F(IndexCheckerTest, testCancel) {
    IndexChecker checker;
    std::thread t1([&checker, this]() {
        EXPECT_EQ(DS_CANCELLED, checker.waitIndexReady(PathDefine::join(_indexRoot, "checkFile2")));
    });
    std::atomic<bool> check(true);
    std::thread t2([&checker, &check]() {
        while (check.load()) {
            checker.cancel();
        }
    });
    t1.join();
    check = false;
    t2.join();
}

} // namespace suez
