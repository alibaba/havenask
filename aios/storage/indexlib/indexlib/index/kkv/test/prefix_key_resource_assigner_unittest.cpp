#include "indexlib/index/kkv/test/prefix_key_resource_assigner_unittest.h"

#include "indexlib/config/test/schema_maker.h"
#include "indexlib/framework/SegmentMetrics.h"
#include "indexlib/index/kkv/kkv_define.h"
#include "indexlib/index_base/segment/segment_writer.h"

using namespace std;
using namespace indexlib::test;
using namespace indexlib::partition;
using namespace indexlib::index_base;
using namespace indexlib::config;

namespace indexlib { namespace index {
IE_LOG_SETUP(index, PrefixKeyResourceAssignerTest);

PrefixKeyResourceAssignerTest::PrefixKeyResourceAssignerTest() {}

PrefixKeyResourceAssignerTest::~PrefixKeyResourceAssignerTest() {}

void PrefixKeyResourceAssignerTest::CaseSetUp()
{
    string fields = "single_int8:int8;single_int16:int16;pkey:uint64;skey:uint64";
    IndexPartitionSchemaPtr schema =
        SchemaMaker::MakeKKVSchema(fields, "pkey", "skey", "single_int8;single_int16;skey");

    const IndexSchemaPtr& indexSchema = schema->GetIndexSchema();
    mKKVIndexConfig = DYNAMIC_POINTER_CAST(KKVIndexConfig, indexSchema->GetPrimaryKeyIndexConfig());
}

void PrefixKeyResourceAssignerTest::CaseTearDown() {}

void PrefixKeyResourceAssignerTest::TestSimpleProcess()
{
    IndexPartitionOptions options;
    options.GetBuildConfig(false).buildTotalMemory = 4; // 4MB
    options.GetBuildConfig(true).buildTotalMemory = 4;  // 4MB
    util::QuotaControlPtr quotaControl(new util::QuotaControl(1 * 1024 * 1024));
    // online
    {
        // null metrics, null quota control
        PrefixKeyResourceAssigner assigner(mKKVIndexConfig, 0, 1);
        assigner.Init(util::QuotaControlPtr(), options);
        size_t size = assigner.Assign(std::shared_ptr<indexlib::framework::SegmentMetrics>());
        ASSERT_EQ(GetTotalQuota(4 * 1024 * 1024, 0.1), size);
    }

    {
        // empty metrics, use quota control
        PrefixKeyResourceAssigner assigner(mKKVIndexConfig, 0, 1);
        assigner.Init(quotaControl, options);
        std::shared_ptr<indexlib::framework::SegmentMetrics> metrics(new framework::SegmentMetrics);
        ;
        size_t size = assigner.Assign(metrics);
        ASSERT_EQ(GetTotalQuota(1 * 1024 * 1024, 0.1), size);
    }

    {
        // usefull metrics, use quota
        PrefixKeyResourceAssigner assigner(mKKVIndexConfig, 0, 1);
        assigner.Init(quotaControl, options);
        std::shared_ptr<indexlib::framework::SegmentMetrics> metrics(new framework::SegmentMetrics);
        SetRatio(metrics, 0.2);
        size_t size = assigner.Assign(metrics);
        ASSERT_EQ(GetTotalQuota(1 * 1024 * 1024, 0.2), size);
    }

    options.SetIsOnline(false);
    // offline, use 1/2 quota
    {
        PrefixKeyResourceAssigner assigner(mKKVIndexConfig, 0, 1);
        assigner.Init(util::QuotaControlPtr(), options);
        size_t size = assigner.Assign(std::shared_ptr<indexlib::framework::SegmentMetrics>());
        ASSERT_EQ(GetTotalQuota(2 * 1024 * 1024, 0.1), size);
    }

    {
        PrefixKeyResourceAssigner assigner(mKKVIndexConfig, 0, 1);
        assigner.Init(quotaControl, options);
        std::shared_ptr<indexlib::framework::SegmentMetrics> metrics(new framework::SegmentMetrics);
        ;
        size_t size = assigner.Assign(metrics);
        ASSERT_EQ(GetTotalQuota(512 * 1024, 0.1), size);
    }
}

void PrefixKeyResourceAssignerTest::TestGetDataRatio()
{
    std::shared_ptr<indexlib::framework::SegmentMetrics> metrics(new framework::SegmentMetrics);
    SetRatio(metrics, 0.1, 0);
    SetRatio(metrics, 0.2, 1);
    SetRatio(metrics, 0.3, 2);

    double totalPkeyRatio;
    double curPkeyRatio;
    double curMemoryRatio;
    {
        PrefixKeyResourceAssigner assigner(mKKVIndexConfig, 0, 3);
        assigner.GetDataRatio(metrics, totalPkeyRatio, curPkeyRatio, curMemoryRatio);
        ASSERT_EQ((double)0.2, totalPkeyRatio);
        ASSERT_EQ((double)0.1, curPkeyRatio);
        ASSERT_EQ((double)1 / 3, curMemoryRatio);
    }

    {
        PrefixKeyResourceAssigner assigner(mKKVIndexConfig, 1, 3);
        assigner.GetDataRatio(metrics, totalPkeyRatio, curPkeyRatio, curMemoryRatio);
        ASSERT_EQ((double)0.2, totalPkeyRatio);
        ASSERT_EQ((double)0.2, curPkeyRatio);
        ASSERT_EQ((double)1 / 3, curMemoryRatio);
    }

    {
        PrefixKeyResourceAssigner assigner(mKKVIndexConfig, 2, 3);
        assigner.GetDataRatio(metrics, totalPkeyRatio, curPkeyRatio, curMemoryRatio);
        ASSERT_EQ((double)0.2, totalPkeyRatio);
        ASSERT_EQ((double)0.3, curPkeyRatio);
        ASSERT_EQ((double)1 / 3, curMemoryRatio);
    }
}

size_t PrefixKeyResourceAssignerTest::GetTotalQuota(size_t totalMem, double ratio)
{
    size_t reserve = totalMem * 0.01;
    if (reserve < 1024) {
        reserve = 1024;
    }

    totalMem -= reserve;
    return (size_t)(totalMem * ratio);
}

void PrefixKeyResourceAssignerTest::SetRatio(const std::shared_ptr<indexlib::framework::SegmentMetrics>& metrics,
                                             double ratio, uint32_t columnIdx)
{
    size_t totalMem = 100000;
    size_t pkeyMem = (size_t)(totalMem * ratio);

    string groupName = SegmentWriter::GetMetricsGroupName(columnIdx);
    metrics->Set<size_t>(groupName, index::KKV_SEGMENT_MEM_USE, totalMem);
    metrics->Set<size_t>(groupName, index::KKV_PKEY_MEM_USE, pkeyMem);
}
}} // namespace indexlib::index
