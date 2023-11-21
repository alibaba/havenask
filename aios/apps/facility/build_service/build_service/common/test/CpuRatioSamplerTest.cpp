#include "build_service/common/CpuRatioSampler.h"

#include <ostream>
#include <unistd.h>

#include "build_service/test/unittest.h"
#include "unittest/unittest.h"

using namespace std;
using namespace testing;

namespace build_service { namespace common {

class CpuRatioSamplerTest : public BUILD_SERVICE_TESTBASE
{
public:
    void setUp();
    void tearDown();
};

void CpuRatioSamplerTest::setUp() {}

void CpuRatioSamplerTest::tearDown() {}

TEST_F(CpuRatioSamplerTest, testSimple)
{
    CpuRatioSampler sampler(10);
    sampler.TEST_setCpuQuota(100);
    int i = 0;
    while (i++ < 9) {
        sampler.workLoop();
        ASSERT_EQ(-1, sampler.getAvgCpuRatio()) << i << std::endl;
        sleep(1);
    }
    sampler.workLoop();
    ASSERT_GE(sampler.getAvgCpuRatio(), 0);
}

}} // namespace build_service::common
