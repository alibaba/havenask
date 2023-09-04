#include "indexlib/index/kv/test/kv_ttl_decider_unittest.h"

#include "indexlib/test/region_schema_maker.h"
#include "indexlib/test/schema_maker.h"

using namespace std;
using namespace indexlib::test;
using namespace indexlib::config;

namespace indexlib { namespace index {
IE_LOG_SETUP(index, KvTtlDeciderTest);

KvTtlDeciderTest::KvTtlDeciderTest() {}

KvTtlDeciderTest::~KvTtlDeciderTest() {}

void KvTtlDeciderTest::CaseSetUp() {}

void KvTtlDeciderTest::CaseTearDown() {}

void KvTtlDeciderTest::TestSimpleProcess()
{
    string field = "key1:string;key2:int32;iValue:uint32;"
                   "sValue:string;miValue:int32:true";
    RegionSchemaMaker maker(field, "kv");
    maker.AddRegion("region1", "key1", "key2;iValue;sValue;miValue", "");
    maker.AddRegion("region2", "key2", "iValue", "", 100);
    maker.AddRegion("region3", "iValue", "sValue", "", 50);
    maker.AddRegion("region4", "sValue", "iValue", "");
    IndexPartitionSchemaPtr schema = maker.GetSchema();
    KVTTLDeciderPtr decider;
    decider.reset(new KVTTLDecider());
    decider->Init(schema);
    // invalid regionId
    ASSERT_FALSE(decider->IsExpiredItem(INVALID_REGIONID, 0, 0));
    ASSERT_FALSE(decider->IsExpiredItem(schema->GetRegionCount() + 1, 0, 0));

    // region without ttl
    ASSERT_FALSE(decider->IsExpiredItem(schema->GetRegionId("region1"), 0, 0));
    ASSERT_FALSE(decider->IsExpiredItem(schema->GetRegionId("region4"), 0, 0));

    // region with ttl
    ASSERT_TRUE(decider->IsExpiredItem(schema->GetRegionId("region2"), 30, 30 + 100 + 1));
    ASSERT_FALSE(decider->IsExpiredItem(schema->GetRegionId("region2"), 30, 30 + 100 - 1));
    ASSERT_TRUE(decider->IsExpiredItem(schema->GetRegionId("region3"), 20, 20 + 50 + 1));
    ASSERT_FALSE(decider->IsExpiredItem(schema->GetRegionId("region3"), 20, 20 + 50 - 1));
}
}} // namespace indexlib::index
