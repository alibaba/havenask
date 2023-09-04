#include "suez/table/SuezRawFilePartition.h"

#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>
#include <iosfwd>
#include <limits>
#include <memory>
#include <stdint.h>
#include <string>

#include "indexlib/partition/partition_group_resource.h"
#include "indexlib/util/memory_control/MemoryQuotaController.h"
#include "suez/common/TableMeta.h"
#include "suez/table/SuezPartition.h"
#include "suez/table/SuezPartitionFactory.h"
#include "unittest/unittest.h"

using namespace std;
using namespace testing;

namespace suez {

class SuezRawFilePartitionTest : public TESTBASE {
public:
    void setUp();
    void tearDown();
};

void SuezRawFilePartitionTest::setUp() {}

void SuezRawFilePartitionTest::tearDown() {}

TEST_F(SuezRawFilePartitionTest, testGetDirSize) {
    SuezPartitionFactory factory;

    PartitionId partitionId;
    TableResource tableResource(partitionId);
    tableResource.globalIndexResource = indexlib::partition::PartitionGroupResource::Create(
        numeric_limits<int64_t>::max(), kmonitor::MetricsReporterPtr(), "", "");
    CurrentPartitionMetaPtr partitionMeta(new PartitionMeta());
    auto suezPartition =
        dynamic_pointer_cast<SuezRawFilePartition>(factory.create(SPT_RAWFILE, tableResource, partitionMeta));
    auto path = GET_TEST_DATA_PATH() + "table_test/for_test_listdir";
    EXPECT_EQ(11, suezPartition->getDirSize(path));
    EXPECT_EQ(-1, suezPartition->getDirSize(""));
    auto no_permission_path = GET_TEST_DATA_PATH() + "no_permission_path";
    EXPECT_EQ(-1, suezPartition->getDirSize(no_permission_path));
}

TEST_F(SuezRawFilePartitionTest, testLoadUnload) {
    SuezPartitionFactory factory;
    PartitionId partitionId;
    TableResource tableResource(partitionId);
    tableResource.globalIndexResource =
        indexlib::partition::PartitionGroupResource::Create(256 << 20, kmonitor::MetricsReporterPtr(), "", "");
    auto originFreeQuota = tableResource.globalIndexResource->GetMemoryQuotaController()->GetFreeQuota();
    CurrentPartitionMetaPtr partitionMeta(new PartitionMeta());
    auto suezPartition =
        dynamic_pointer_cast<SuezRawFilePartition>(factory.create(SPT_RAWFILE, tableResource, partitionMeta));
    auto localIndexPath = GET_TEST_DATA_PATH() + "table_test/raw_file_reserve/generation_0/partition_0_65535/";
    TargetPartitionMeta target;
    target.incVersion = 12;
    ASSERT_EQ(TS_LOADED, suezPartition->doLoad(localIndexPath).status);
    ASSERT_EQ(originFreeQuota - 11, tableResource.globalIndexResource->GetMemoryQuotaController()->GetFreeQuota());
    ASSERT_EQ(11, suezPartition->_dataSize);

    suezPartition->unload();
    ASSERT_EQ(originFreeQuota, tableResource.globalIndexResource->GetMemoryQuotaController()->GetFreeQuota());
    ASSERT_EQ(-1, suezPartition->_dataSize);

    localIndexPath = GET_TEST_DATA_PATH() + "table_test/raw_file_noreserve/generation_0/partition_0_65535/";
    ASSERT_EQ(TS_LOADED, suezPartition->doLoad(localIndexPath).status);
    ASSERT_EQ(originFreeQuota, tableResource.globalIndexResource->GetMemoryQuotaController()->GetFreeQuota());
    ASSERT_EQ(-1, suezPartition->_dataSize);

    suezPartition->unload();
    ASSERT_EQ(originFreeQuota, tableResource.globalIndexResource->GetMemoryQuotaController()->GetFreeQuota());
    ASSERT_EQ(-1, suezPartition->_dataSize);
}

} // namespace suez
