#include "kv_key_extractor_unittest.h"

#include "autil/ConstString.h"
#include "indexlib/config/test/region_schema_maker.h"
#include "indexlib/config/test/schema_maker.h"
#include "indexlib/index/common/KeyHasherWrapper.h"

using namespace std;
using namespace autil;
using namespace indexlib::config;
using namespace indexlib::util;
using namespace indexlib::test;

namespace indexlib { namespace document {
IE_LOG_SETUP(document, KvKeyExtractorTest);

KvKeyExtractorTest::KvKeyExtractorTest() {}

KvKeyExtractorTest::~KvKeyExtractorTest() {}

void KvKeyExtractorTest::CaseSetUp() {}

void KvKeyExtractorTest::CaseTearDown() {}

void KvKeyExtractorTest::TestHashKey()
{
    string field = "key1:string;key2:int32;iValue:uint32;"
                   "sValue:string;miValue:int32:true";
    RegionSchemaMaker maker(field, "kv");
    maker.AddRegion("region1", "key1", "key2;iValue;sValue;miValue", "");
    maker.AddRegion("region2", "key2", "iValue", "", 100);
    maker.AddRegion("region3", "iValue", "sValue", "", 50);
    maker.AddRegion("region4", "sValue", "iValue", "");
    IndexPartitionSchemaPtr schema = maker.GetSchema();

    MultiRegionKVKeyExtractorPtr extractor;
    extractor.reset(new MultiRegionKVKeyExtractor(schema));
    {
        // invalid region id
        string key = "dhfhdf";
        uint64_t keyHash = 0;
        ASSERT_FALSE(extractor->HashKey(key, keyHash, INVALID_REGIONID));
        ASSERT_FALSE(extractor->HashKey(key, keyHash, schema->GetRegionCount()));
    }
    auto checkHash = [&extractor](const string& key, regionid_t regionId, FieldType ft) {
        StringView constkey(key);
        uint64_t keyHash = 0;
        uint64_t expectedHash = -1;

        ASSERT_TRUE(extractor->HashKey(key, keyHash, regionId));
        index::KeyHasherWrapper::GetHashKey(ft, /*useNumberHash=*/true, constkey.data(), constkey.size(), expectedHash);
        ASSERT_EQ(expectedHash, keyHash);
    };
    checkHash("testKey", 0, ft_string);
    checkHash("12", 1, ft_int32);
    checkHash("-112", 1, ft_int32);
    checkHash("661", 2, ft_uint32);
    checkHash("testKey2", 3, ft_string);
}

void KvKeyExtractorTest::TestHashKeyWithMurmurHash()
{
    string field = "key1:string;key2:int32;iValue:uint32;"
                   "sValue:string;miValue:int32:true";
    RegionSchemaMaker maker(field, "kv");
    maker.AddRegion("region1", "key1", "key2;iValue;sValue;miValue", "");
    maker.AddRegion("region2", "key2", "iValue", "", 100);
    maker.AddRegion("region3", "iValue", "sValue", "", 50);
    maker.AddRegion("region4", "sValue", "iValue", "");
    IndexPartitionSchemaPtr schema = maker.GetSchema();
    for (size_t regionId = 0; regionId < schema->GetRegionCount(); regionId++) {
        // use number hasher
        const IndexSchemaPtr& indexSchema = schema->GetIndexSchema(regionId);
        KVIndexConfigPtr kvConfig =
            DYNAMIC_POINTER_CAST(config::KVIndexConfig, indexSchema->GetPrimaryKeyIndexConfig());
        kvConfig->SetUseNumberHash(false);
    }

    MultiRegionKVKeyExtractorPtr extractor;
    extractor.reset(new MultiRegionKVKeyExtractor(schema));
    auto checkHash = [&extractor](const string& key, regionid_t regionId, FieldType ft) {
        StringView constkey(key);
        uint64_t keyHash = 0;
        uint64_t expectedHash = -1;

        ASSERT_TRUE(extractor->HashKey(key, keyHash, regionId));
        index::KeyHasherWrapper::GetHashKey(ft, /*useNumberHash=*/false, constkey.data(), constkey.size(),
                                            expectedHash);
        ASSERT_EQ(expectedHash, keyHash);
    };
    checkHash("testKey", 0, ft_string);
    checkHash("12", 1, ft_int32);
    checkHash("-112", 1, ft_int32);
    checkHash("661", 2, ft_uint32);
    checkHash("testKey2", 3, ft_string);
}
}} // namespace indexlib::document
