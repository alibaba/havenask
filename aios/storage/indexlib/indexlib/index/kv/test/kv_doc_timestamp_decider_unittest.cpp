#include "indexlib/index/kv/test/kv_doc_timestamp_decider_unittest.h"

#include "indexlib/document/kv_document/kv_index_document.h"

using namespace std;
using namespace indexlib::config;

namespace indexlib { namespace index {
IE_LOG_SETUP(index, KvDocTimestampDeciderTest);

KvDocTimestampDeciderTest::KvDocTimestampDeciderTest() {}

KvDocTimestampDeciderTest::~KvDocTimestampDeciderTest() {}

void KvDocTimestampDeciderTest::CaseSetUp() {}

void KvDocTimestampDeciderTest::CaseTearDown() {}

void KvDocTimestampDeciderTest::TestSimpleProcess()
{
    KvDocTimestampDecider kvDocTimestampDecider;
    IndexPartitionOptions option;
    OfflineConfig offlineConfig;
    offlineConfig.buildConfig.useUserTimestamp = true;
    offlineConfig.buildConfig.buildPhase = BuildConfig::BP_FULL;
    option.SetOfflineConfig(offlineConfig);
    autil::mem_pool::Pool pool;
    document::KVIndexDocument doc(&pool);
    doc.SetTimestamp((int64_t)1000000);

    kvDocTimestampDecider.Init(option);
    ASSERT_EQ(1, kvDocTimestampDecider.GetTimestamp(&doc));

    doc.SetUserTimestamp((int64_t)2000000);
    ASSERT_EQ(2, kvDocTimestampDecider.GetTimestamp(&doc));

    offlineConfig.buildConfig.useUserTimestamp = false;
    option.SetOfflineConfig(offlineConfig);
    kvDocTimestampDecider.Init(option);
    ASSERT_EQ(1, kvDocTimestampDecider.GetTimestamp(&doc));

    offlineConfig.buildConfig.buildPhase = BuildConfig::BP_INC;
    offlineConfig.buildConfig.useUserTimestamp = true;
    option.SetOfflineConfig(offlineConfig);
    kvDocTimestampDecider.Init(option);
    ASSERT_EQ(1, kvDocTimestampDecider.GetTimestamp(&doc));
}
}} // namespace indexlib::index
