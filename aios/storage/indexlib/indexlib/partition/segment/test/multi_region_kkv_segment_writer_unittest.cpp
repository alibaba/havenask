#include "indexlib/partition/segment/test/multi_region_kkv_segment_writer_unittest.h"

#include "indexlib/config/test/region_schema_maker.h"
#include "indexlib/framework/SegmentMetrics.h"
#include "indexlib/util/memory_control/QuotaControl.h"

using namespace std;
using namespace indexlib::test;
using namespace indexlib::config;
using namespace indexlib::index;
using namespace indexlib::index_base;

using namespace indexlib::util;
namespace indexlib { namespace partition {
IE_LOG_SETUP(partition, MultiRegionKkvSegmentWriterTest);

MultiRegionKkvSegmentWriterTest::MultiRegionKkvSegmentWriterTest() {}

MultiRegionKkvSegmentWriterTest::~MultiRegionKkvSegmentWriterTest() {}

void MultiRegionKkvSegmentWriterTest::CaseSetUp()
{
    file_system::FileSystemOptions fsOptions;
    fsOptions.isOffline = false;
    fsOptions.outputStorage = file_system::FSST_MEM;
    RESET_FILE_SYSTEM("ut", false, fsOptions);
}

void MultiRegionKkvSegmentWriterTest::CaseTearDown() {}

void MultiRegionKkvSegmentWriterTest::TestInit()
{
    string field = "uid:string;friendid:int32;value:uint32;";
    RegionSchemaMaker maker(field, "kkv");
    maker.AddRegion("region1", "uid", "friendid", "value;friendid;", "");
    maker.AddRegion("region2", "friendid", "uid", "value;uid;", "");
    IndexPartitionSchemaPtr schema = maker.GetSchema();
    IndexPartitionOptions options;

    BuildingSegmentData segmentData;
    segmentData.SetSegmentId(0);
    segmentData.SetSegmentDirName("test_segment");
    MAKE_SEGMENT_DIRECTORY(0);
    segmentData.Init(GET_SEGMENT_DIRECTORY(), false);

    util::QuotaControlPtr mQuotaControl(new util::QuotaControl(1024 * 1024 * 1024));
    std::shared_ptr<indexlib::framework::SegmentMetrics> metrics(new framework::SegmentMetrics);
    SegmentWriterPtr segWriter(new MultiRegionKKVSegmentWriter(schema, options));
    segWriter->Init(segmentData, metrics, mQuotaControl);

    MultiRegionKKVSegmentWriterPtr mrSegWriter = DYNAMIC_POINTER_CAST(MultiRegionKKVSegmentWriter, segWriter);

    ASSERT_TRUE(mrSegWriter);
    auto kkvConfig = mrSegWriter->mKKVConfig;
    auto suffixFieldConfig = kkvConfig->GetSuffixFieldConfig();
    ASSERT_EQ(ft_uint64, suffixFieldConfig->GetFieldType());
    ASSERT_EQ(string("uid"), kkvConfig->GetIndexName());
}
}} // namespace indexlib::partition
