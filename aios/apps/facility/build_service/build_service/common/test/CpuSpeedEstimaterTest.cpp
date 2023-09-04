#include "build_service/common/CpuSpeedEstimater.h"

#include <autil/LoopThread.h>
#include <autil/TimeUtility.h>

#include "build_service/test/unittest.h"

using namespace std;
using namespace testing;

namespace build_service { namespace common {

class CpuSpeedEstimaterTest : public BUILD_SERVICE_TESTBASE
{
public:
    void setUp();
    void tearDown();
};

void CpuSpeedEstimaterTest::setUp() {}

void CpuSpeedEstimaterTest::tearDown() {}

TEST_F(CpuSpeedEstimaterTest, testSimple)
{
    CpuSpeedEstimater a(nullptr);
    for (size_t i = 0; i < 20; i++) {
        a.WorkLoop(6);
    }
}

TEST_F(CpuSpeedEstimaterTest, testMultiThreadRead)
{
    int64_t beginTime = autil::TimeUtility::currentTimeInSeconds();
    int64_t endTime = beginTime;
    int64_t durationInSeconds = 30;
    int64_t intervalInMicroSeconds = 1000;

    CpuSpeedEstimater a(nullptr);
    a.Start(5, 2);

    autil::LoopThreadPtr t1 =
        autil::LoopThread::createLoopThread(std::bind(&CpuSpeedEstimater::GetMedianSpeed, &a), intervalInMicroSeconds);
    autil::LoopThreadPtr t2 =
        autil::LoopThread::createLoopThread(std::bind(&CpuSpeedEstimater::GetAvgSpeed, &a), intervalInMicroSeconds);
    autil::LoopThreadPtr t3 =
        autil::LoopThread::createLoopThread(std::bind(&CpuSpeedEstimater::GetCurrentSpeed, &a), intervalInMicroSeconds);

    while (endTime - beginTime < durationInSeconds) {
        sleep(1);
        endTime = autil::TimeUtility::currentTimeInSeconds();
    }
}

}} // namespace build_service::common
