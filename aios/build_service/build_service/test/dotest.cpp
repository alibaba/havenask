#include <gmock/gmock.h>
#include "build_service/util/Log.h"
#include "build_service/test/unittest.h"
#include "build_service/util/FileUtil.h"
#include "build_service/common/BeeperCollectorDefine.h"
#include <fslib/fs/FileSystem.h>
#include <beeper/beeper.h>

using namespace std;
using namespace ::testing;

BS_LOG_SETUP(unittest, BUILD_SERVICE_TESTBASE);

int main(int argc, char **argv)
{
    static char confFile[] = "build_service/test/bs_alog.conf";
    //static char confFile[] = "misc/bs_alog.conf";
    BS_LOG_CONFIG(confFile);
    InitGoogleMock(&argc, argv);
    int ret = RUN_ALL_TESTS();
    fslib::fs::FileSystem::close();
    BS_LOG_FLUSH();
    BEEPER_CLOSE();
    return ret;
}
