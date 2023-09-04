#include "swift/util/TargetRecorder.h"

#include <cstddef>
#include <string>
#include <unistd.h>
#include <vector>

#include "fslib/common/common_type.h"
#include "fslib/fs/FileSystem.h"
#include "unittest/unittest.h"

using namespace std;

namespace swift {
namespace util {

class TargetRecorderTest : public TESTBASE {};

TEST_F(TargetRecorderTest, testSimple) {
    string path;
    ASSERT_TRUE(TargetRecorder::getCurrentPath(path));
    TargetRecorder::setMaxFileCount(1);
    for (size_t i = 0; i < 3; i++) {
        TargetRecorder::newAdminTopic("admin");
        TargetRecorder::newAdminPartition("admin");
        sleep(1);
    }
    vector<string> files;
    EXPECT_EQ(fslib::EC_OK, fslib::fs::FileSystem::listDir(path + "admin_topic", files));
    EXPECT_EQ(size_t(2), files.size());
    files.clear();
    EXPECT_EQ(fslib::EC_OK, fslib::fs::FileSystem::listDir(path + "admin_partition", files));
    EXPECT_EQ(size_t(2), files.size());
}

} // namespace util
} // namespace swift
