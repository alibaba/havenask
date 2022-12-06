#include <gmock/gmock.h>
#include <indexlib/misc/log.h>
#include <google/protobuf/stubs/common.h>

using namespace std;
using namespace ::testing;

int main(int argc, char **argv) {
    static char confFile[] = "test/logger.conf";
    IE_LOG_CONFIG(confFile);

    ::testing::InitGoogleMock(&argc, argv);
    int ret = RUN_ALL_TESTS();

    IE_LOG_SHUTDOWN();
    google::protobuf::ShutdownProtobufLibrary();
    std::cout << "configure file : " << confFile << std::endl;
    return ret;
}
