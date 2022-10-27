#include "indexlib/document/document_parser/kv_parser/test/kv_key_extractor_unittest.h"
#include "indexlib/test/schema_maker.h"
#include "indexlib/test/region_schema_maker.h"
#include "indexlib/config/impl/kv_index_config_impl.h"
#include <autil/ConstString.h>

using namespace std;
using namespace autil;
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(util);
IE_NAMESPACE_USE(test);

IE_NAMESPACE_BEGIN(document);
IE_LOG_SETUP(document, KvKeyExtractorTest);

KvKeyExtractorTest::KvKeyExtractorTest()
{
}

KvKeyExtractorTest::~KvKeyExtractorTest()
{
}

void KvKeyExtractorTest::CaseSetUp()
{
}

void KvKeyExtractorTest::CaseTearDown()
{
}

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
        //invalid region id
        string key = "dhfhdf";
        uint64_t keyHash = 0;
        ASSERT_FALSE(extractor->HashKey(key, keyHash, INVALID_REGIONID));
        ASSERT_FALSE(extractor->HashKey(key, keyHash, schema->GetRegionCount()));
    }
    auto checkHash = [&extractor](const string& key, regionid_t regionId,
                                  FieldType ft)
    {
        ConstString constkey(key);
        uint64_t keyHash = 0;
        uint64_t expectedHash = -1;
        
        ASSERT_TRUE(extractor->HashKey(key, keyHash, regionId));
        KeyHasher* hasher = KeyHasherFactory::Create(ft, true);
        hasher->GetHashKey(constkey, expectedHash);
        ASSERT_EQ(expectedHash, keyHash);
        delete hasher;
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
    for (size_t regionId = 0; regionId < schema->GetRegionCount(); regionId++)
    {
        //use number hasher
        const IndexSchemaPtr& indexSchema = schema->GetIndexSchema(regionId);
        KVIndexConfigPtr kvConfig = DYNAMIC_POINTER_CAST(config::KVIndexConfig,
                indexSchema->GetPrimaryKeyIndexConfig());
        kvConfig->mImpl->mUseNumberHash = false;
    }
    
    MultiRegionKVKeyExtractorPtr extractor;
    extractor.reset(new MultiRegionKVKeyExtractor(schema));
    auto checkHash = [&extractor](const string& key, regionid_t regionId, FieldType ft)
    {
        ConstString constkey(key);
        uint64_t keyHash = 0;
        uint64_t expectedHash = -1;
        
        ASSERT_TRUE(extractor->HashKey(key, keyHash, regionId));
        KeyHasher* hasher = KeyHasherFactory::Create(ft, false);
        hasher->GetHashKey(constkey, expectedHash);
        ASSERT_EQ(expectedHash, keyHash);
        delete hasher;
    };
    checkHash("testKey", 0, ft_string);
    checkHash("12", 1, ft_int32);
    checkHash("-112", 1, ft_int32);
    checkHash("661", 2, ft_uint32);
    checkHash("testKey2", 3, ft_string);
}

IE_NAMESPACE_END(document);

