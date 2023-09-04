#include "build_service/util/BsMetricTagsHandler.h"

#include "build_service/test/unittest.h"

using namespace std;
using namespace testing;
using namespace kmonitor;
using namespace build_service::proto;

namespace build_service { namespace util {

class BsMetricTagsHandlerTest : public BUILD_SERVICE_TESTBASE
{
public:
    void setUp();
    void tearDown();
};

void BsMetricTagsHandlerTest::setUp() {}

void BsMetricTagsHandlerTest::tearDown() {}

TEST_F(BsMetricTagsHandlerTest, testSimple)
{
    PartitionId pid;
    pid.set_role(ROLE_BUILDER);
    pid.mutable_buildid()->set_appname("app_name");
    pid.mutable_buildid()->set_generationid(1);
    pid.mutable_buildid()->set_datatable("data_table");
    pid.mutable_range()->set_from(0);
    pid.mutable_range()->set_to(65535);
    *pid.add_clusternames() = "mainse_searcher";
    pid.set_mergeconfigname("inc1");

    MetricsTags tags;
    BsMetricTagsHandler handler(pid);
    handler.getTags("file1", tags);

    ASSERT_EQ(string("mainse_searcher"), tags.FindTag("clusterName"));
    ASSERT_EQ(string("0_65535"), tags.FindTag("partition"));
    ASSERT_EQ(string("1"), tags.FindTag("generation"));
    ASSERT_EQ(string("full"), tags.FindTag("buildStep"));
    ASSERT_EQ(string("builder"), tags.FindTag("roleName"));
}

}} // namespace build_service::util
