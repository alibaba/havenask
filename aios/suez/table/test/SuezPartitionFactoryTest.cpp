#include "suez/table/SuezPartitionFactory.h"

#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>
#include <iosfwd>
#include <limits>
#include <memory>
#include <stdint.h>
#include <string>

#include "indexlib/partition/partition_group_resource.h"
#include "kmonitor/client/MetricsReporter.h"
#include "suez/common/TableMeta.h"
#include "suez/table/SuezIndexPartition.h"
#include "suez/table/SuezPartition.h"
#include "suez/table/SuezRawFilePartition.h"
#include "unittest/unittest.h"

namespace suez {
class SuezIndexPartition;
class SuezRawFilePartition;
} // namespace suez

using namespace std;
using namespace testing;

namespace suez {

class SuezPartitionFactoryTest : public TESTBASE {
public:
    void setUp();
    void tearDown();
};

void SuezPartitionFactoryTest::setUp() {}

void SuezPartitionFactoryTest::tearDown() {}

TEST_F(SuezPartitionFactoryTest, testCreate) {
    SuezPartitionFactory factory;
    PartitionId partitionId;
    TableResource tableResource(partitionId);
    tableResource.globalIndexResource = indexlib::partition::PartitionGroupResource::Create(
        numeric_limits<int64_t>::max(), kmonitor::MetricsReporterPtr(), "", "");
    CurrentPartitionMetaPtr partitionMeta(new PartitionMeta());
    SuezPartitionPtr suezPartition = factory.create(SPT_RAWFILE, tableResource, partitionMeta);
    ASSERT_TRUE(dynamic_pointer_cast<SuezRawFilePartition>(suezPartition));
    suezPartition = factory.create(SPT_INDEXLIB, tableResource, partitionMeta);
    ASSERT_TRUE(dynamic_pointer_cast<SuezIndexPartition>(suezPartition));
}

} // namespace suez
