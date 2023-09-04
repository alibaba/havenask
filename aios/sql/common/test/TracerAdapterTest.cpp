#include "sql/common/TracerAdapter.h"

#include <iosfwd>

#include "autil/Log.h"
#include "navi/common.h"
#include "turing_ops_util/variant/Tracer.h"
#include "unittest/unittest.h"

using namespace std;
using namespace testing;
using namespace navi;

namespace sql {

class TracerAdapterTest : public TESTBASE {
public:
    void setUp() override;
    void tearDown() override;

private:
    AUTIL_LOG_DECLARE();
};

AUTIL_LOG_SETUP(common, TracerAdapterTest);

void TracerAdapterTest::setUp() {}

void TracerAdapterTest::tearDown() {}

TEST_F(TracerAdapterTest, testSetNaviLevel_ScheduleLevel) {
    TracerAdapter tracer;
    tracer.setNaviLevel(LOG_LEVEL_SCHEDULE3);
    ASSERT_EQ(ISEARCH_TRACE_TRACE3, tracer._level);
}

TEST_F(TracerAdapterTest, testSetNaviLevel_NormalLevel) {
    TracerAdapter tracer;
    tracer.setNaviLevel(LOG_LEVEL_DEBUG);
    ASSERT_EQ(ISEARCH_TRACE_DEBUG, tracer._level);
}

TEST_F(TracerAdapterTest, testSetNaviLevel_OtherLevel) {
    TracerAdapter tracer;
    tracer.setNaviLevel(LOG_LEVEL_DISABLE);
    ASSERT_EQ(ISEARCH_TRACE_FATAL, tracer._level);
}

} // namespace sql
