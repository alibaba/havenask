#include "indexlib/partition/test/memory_stat_collector_unittest.h"

#include "indexlib/config/test/schema_maker.h"
#include "indexlib/file_system/FileBlockCacheContainer.h"
#include "indexlib/partition/group_memory_reporter.h"
#include "indexlib/partition/memory_stat_reporter.h"
#include "indexlib/partition/online_partition.h"
#include "indexlib/test/partition_state_machine.h"
#include "indexlib/util/TaskScheduler.h"

using namespace std;
using namespace indexlib::util;
using namespace indexlib::test;
using namespace indexlib::common;
using namespace indexlib::config;

namespace indexlib { namespace partition {
IE_LOG_SETUP(partition, MemoryStatCollectorTest);

MemoryStatCollectorTest::MemoryStatCollectorTest() {}

MemoryStatCollectorTest::~MemoryStatCollectorTest() {}

void MemoryStatCollectorTest::CaseSetUp() {}

void MemoryStatCollectorTest::CaseTearDown() {}

void MemoryStatCollectorTest::TestSimpleProcess()
{
    setenv("TEST_QUICK_EXIT", "true", 1);
    util::MetricProviderPtr metricProvider(new util::MetricProvider(nullptr));

    TaskSchedulerPtr taskScheduler(new TaskScheduler);
    MemoryStatReporterPtr statReporter(new MemoryStatReporter);
    statReporter->Init("", util::SearchCachePtr(), file_system::FileBlockCacheContainerPtr(), taskScheduler,
                       metricProvider);

    MemoryStatCollectorPtr statCollector = statReporter->GetMemoryStatCollector();
    ASSERT_EQ(NULL, statCollector->mtotalPartitionIndexSize);
    ASSERT_EQ(NULL, statCollector->mtotalPartitionMemoryUse);
    ASSERT_EQ(NULL, statCollector->mtotalIncIndexMemoryUse);
    ASSERT_EQ(NULL, statCollector->mtotalRtIndexMemoryUse);
    ASSERT_EQ(NULL, statCollector->mtotalBuiltRtIndexMemoryUse);
    ASSERT_EQ(NULL, statCollector->mtotalBuildingSegmentMemoryUse);
    ASSERT_EQ(NULL, statCollector->mtotalOldInMemorySegmentMemoryUse);
    ASSERT_EQ(NULL, statCollector->mtotalPartitionMemoryQuotaUse);

    IndexPartitionResource partitionResource;
    partitionResource.partitionGroupName = "test_group";
    partitionResource.metricProvider = metricProvider;
    partitionResource.taskScheduler = taskScheduler;
    partitionResource.memoryStatReporter = statReporter;
    partitionResource.fileBlockCacheContainer.reset(new file_system::FileBlockCacheContainer());

    string field = "string1:string;";
    string index = "index1:string:string1;";
    IndexPartitionSchemaPtr schema = SchemaMaker::MakeSchema(field, index, "", "");
    INDEXLIB_TEST_TRUE(schema);

    IndexPartitionOptions options;
    string docString = "cmd=add,string1=hello;";
    PartitionStateMachine psm(false, partitionResource);
    INDEXLIB_TEST_TRUE(psm.Init(schema, options, GET_TEMP_DATA_PATH()));
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, docString, "index1:hello", "docid=0;"));

    GroupMemoryReporterPtr groupReporter = statCollector->GetGroupMemoryReporter(partitionResource.partitionGroupName);
    ASSERT_TRUE(groupReporter);

    CheckMetrics(groupReporter->mtotalPartitionIndexSizeMetric, metricProvider,
                 "indexlib/group/test_group/totalPartitionIndexSize", "byte");
    CheckMetrics(groupReporter->mtotalPartitionMemoryUseMetric, metricProvider,
                 "indexlib/group/test_group/totalPartitionMemoryUse", "byte");
    CheckMetrics(groupReporter->mtotalIncIndexMemoryUseMetric, metricProvider,
                 "indexlib/group/test_group/totalIncIndexMemoryUse", "byte");
    CheckMetrics(groupReporter->mtotalRtIndexMemoryUseMetric, metricProvider,
                 "indexlib/group/test_group/totalRtIndexMemoryUse", "byte");
    CheckMetrics(groupReporter->mtotalBuiltRtIndexMemoryUseMetric, metricProvider,
                 "indexlib/group/test_group/totalBuiltRtIndexMemoryUse", "byte");
    CheckMetrics(groupReporter->mtotalBuildingSegmentMemoryUseMetric, metricProvider,
                 "indexlib/group/test_group/totalBuildingSegmentMemoryUse", "byte");
    CheckMetrics(groupReporter->mtotalOldInMemorySegmentMemoryUseMetric, metricProvider,
                 "indexlib/group/test_group/totalOldInMemorySegmentMemoryUse", "byte");
    CheckMetrics(groupReporter->mtotalPartitionMemoryQuotaUseMetric, metricProvider,
                 "indexlib/group/test_group/totalPartitionMemoryQuotaUse", "byte");
}

void MemoryStatCollectorTest::CheckMetrics(util::MetricPtr targetMetric, util::MetricProviderPtr provider,
                                           const string& name, const string& unit)
{
    ASSERT_TRUE(targetMetric != nullptr);
    util::MetricPtr metric = provider->DeclareMetric(name, kmonitor::GAUGE);
    ASSERT_TRUE(metric != nullptr);
}
}} // namespace indexlib::partition
