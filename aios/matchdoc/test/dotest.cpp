#include "gmock/gmock.h"
#include "gtest/gtest.h"

#include "autil/Log.h"

using namespace std;
using namespace ::testing;

int main(int argc, char **argv) {
    AUTIL_ROOT_LOG_CONFIG();

    InitGoogleMock(&argc, argv);
    int ret = RUN_ALL_TESTS();

    AUTIL_LOG_FLUSH();
    AUTIL_LOG_SHUTDOWN();

    return ret;
}
