#include <gmock/gmock.h>
#include "indexlib/common_define.h"
#include <google/protobuf/stubs/common.h>

using namespace std;
using namespace ::testing;

int INDEXLIB_TEST_ARGC;
char** INDEXLIB_TEST_ARGV;

int main(int argc, char **argv)
{
    static char confFile[] = "test/logger.conf";
    IE_LOG_CONFIG(confFile);

    INDEXLIB_TEST_ARGC = argc;
    INDEXLIB_TEST_ARGV = argv;

    ::testing::InitGoogleMock(&argc, argv);
    int ret = RUN_ALL_TESTS();

    IE_LOG_FLUSH();
    google::protobuf::ShutdownProtobufLibrary();
    return ret;
}
