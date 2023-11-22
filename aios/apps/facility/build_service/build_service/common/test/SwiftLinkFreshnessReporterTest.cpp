#include "build_service/common/SwiftLinkFreshnessReporter.h"

#include <iosfwd>
#include <stdint.h>
#include <string>
#include <unistd.h>

#include "autil/TimeUtility.h"
#include "build_service/proto/BasicDefs.pb.h"
#include "build_service/test/unittest.h"
#include "swift/protocol/SwiftMessage.pb.h"
#include "unittest/unittest.h"

using namespace std;
using namespace autil;
using namespace testing;

namespace build_service { namespace common {

class SwiftLinkFreshnessReporterTest : public BUILD_SERVICE_TESTBASE
{
public:
    void setUp();
    void tearDown();
};

void SwiftLinkFreshnessReporterTest::setUp() {}

void SwiftLinkFreshnessReporterTest::tearDown() {}

TEST_F(SwiftLinkFreshnessReporterTest, testSimple)
{
    proto::PartitionId partitionId;
    partitionId.mutable_buildid()->set_datatable("simple");
    partitionId.mutable_range()->set_from(0);
    partitionId.mutable_range()->set_to(32700);
    *partitionId.add_clusternames() = "simple_cluster";

    SwiftLinkFreshnessReporter reporter(partitionId);
    ASSERT_TRUE(reporter.init(indexlib::util::MetricProviderPtr(), /*totalSwiftPartitionCount=*/6, "topic_name"));

    sleep(10);

    int64_t ts = TimeUtility::currentTime() - 100 * 1000 * 1000;
    swift::protocol::Message msg;
    msg.set_timestamp(ts);
    msg.set_uint16payload(0);
    reporter.collectSwiftMessageMeta(msg);

    msg.set_uint16payload(17000);
    reporter.collectSwiftMessageMeta(msg);

    reporter.collectTimestampFieldValue(ts - 2000000, 0);
    reporter.collectTimestampFieldValue(ts - 10000000, 17000);
    reporter.collectSwiftMessageOffset(ts);

    sleep(5);

    msg.set_timestamp(ts);
    msg.set_uint16payload(24000);

    reporter.collectSwiftMessageMeta(msg);
    reporter.collectTimestampFieldValue(ts - 8000000, 24000);
    reporter.collectSwiftMessageOffset(ts + 80 * 1000 * 1000);
    sleep(5);
}

}} // namespace build_service::common
