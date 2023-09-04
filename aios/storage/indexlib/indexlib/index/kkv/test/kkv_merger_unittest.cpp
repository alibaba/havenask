#include "indexlib/index/kkv/test/kkv_merger_unittest.h"

#include "indexlib/common/file_system_factory.h"
#include "indexlib/partition/partition_data_creator.h"
#include "indexlib/test/partition_state_machine.h"
#include "indexlib/test/schema_maker.h"
#include "indexlib/util/ColumnUtil.h"

using namespace std;
using namespace indexlib::test;
using namespace indexlib::file_system;
using namespace indexlib::partition;
using namespace indexlib::index_base;
using namespace indexlib::config;
using namespace indexlib::common;

using namespace indexlib::util;
namespace indexlib { namespace index {
IE_LOG_SETUP(index, KKVMergerTest);

KKVMergerTest::KKVMergerTest() {}

KKVMergerTest::~KKVMergerTest() {}

void KKVMergerTest::CaseSetUp() {}

void KKVMergerTest::CaseTearDown() {}

void KKVMergerTest::TestEstimateMemoryUse()
{
    string field = "pkey:string;skey:int32;value:uint32;";
    IndexPartitionSchemaPtr schema = SchemaMaker::MakeKKVSchema(field, "pkey", "skey", "value;skey;");
    string docString = "cmd=add,pkey=pkey1,skey=1,value=1,ts=101;"
                       "cmd=add,pkey=pkey1,skey=2,value=2,ts=102;"
                       "cmd=add,pkey=pkey1,skey=3,value=2,ts=102;"
                       "cmd=add,pkey=pkey1,skey=4,value=2,ts=102";
    IndexPartitionOptions options;
    options.GetOfflineConfig().buildConfig.maxDocCount = 1;
    options.GetBuildConfig(false).buildTotalMemory = 20; // 20MB
    options.GetBuildConfig(true).buildTotalMemory = 20;  // 20MB
    PartitionStateMachine psm;
    ASSERT_TRUE(psm.Init(schema, options, GET_TEMP_DATA_PATH()));
    ASSERT_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, docString, "", ""));

    auto fsOptions = FileSystemFactory::CreateFileSystemOptions(
        GET_TEMP_DATA_PATH(), options, util::MemoryQuotaControllerCreator::CreatePartitionMemoryController(),
        file_system::FileBlockCacheContainerPtr(), "");
    IFileSystemPtr fs = FileSystemCreator::Create("KKVMergerTest", GET_TEMP_DATA_PATH(), fsOptions).GetOrThrow();
    fs->TEST_MountLastVersion();
    OnDiskPartitionDataPtr partitionData = OnDiskPartitionData::CreateOnDiskPartitionData(fs, schema);
    index_base::SegmentDataVector segmentDatas;
    uint32_t columnCount = 1;
    uint32_t columnIdx = 0;
    for (OnDiskPartitionData::Iterator iter = partitionData->Begin(); iter != partitionData->End(); iter++) {
        const SegmentData& segmentData = *iter;
        uint32_t segmentColumnCount = segmentData.GetSegmentInfo()->shardCount;

        for (size_t inSegmentIdx = 0; inSegmentIdx < segmentColumnCount; ++inSegmentIdx) {
            uint32_t idx = util::ColumnUtil::TransformColumnId(inSegmentIdx, columnCount, segmentColumnCount);
            if (idx == columnIdx) {
                const index_base::SegmentData& shardingSegmentData =
                    segmentData.CreateShardingSegmentData(inSegmentIdx);
                segmentDatas.push_back(shardingSegmentData);
            }
        }
    }
    auto primaryKeyConfig = schema->GetIndexSchema()->GetPrimaryKeyIndexConfig();
    const KKVIndexConfigPtr& kkvIndexConfig = DYNAMIC_POINTER_CAST(KKVIndexConfig, primaryKeyConfig);
    assert(kkvIndexConfig);
    (void)kkvIndexConfig;
    KKVMergerTyped<int32_t> kkvMerger(10000, schema);
    SegmentTopologyInfo topo;
    size_t mem = kkvMerger.EstimateMemoryUse(segmentDatas, topo);
    // hash size: hash vector 16*4 hash table: 16*2+16*2(specail value)+16(header)=144
    // buffer size + dumpTempSize + pool reset threadhold
    // hashMemUse = 80, iteratorMemUse = 64, RESET_POOL_THRESHOLD = 20971520
    // EstimateTempDumpMem = 303127, bufferSize = 23068672
    // hintBuffer = 65536
    // size_t expectMem = 44343458;
    size_t expectMem = 44408998;
    EXPECT_EQ(expectMem, mem);

    // test async read
    config::MergeIOConfig ioConfig;
    ioConfig.enableAsyncRead = true;
    kkvMerger.SetMergeIOConfig(ioConfig);
    mem = kkvMerger.EstimateMemoryUse(segmentDatas, topo);
    expectMem += segmentDatas.size() * ioConfig.readBufferSize * 2;
    ASSERT_EQ(expectMem, mem);

    // test async write
    ioConfig.enableAsyncWrite = true;
    kkvMerger.SetMergeIOConfig(ioConfig);
    mem = kkvMerger.EstimateMemoryUse(segmentDatas, topo);
    expectMem += ioConfig.writeBufferSize * 2;
    ASSERT_EQ(expectMem, mem);
}
}} // namespace indexlib::index
