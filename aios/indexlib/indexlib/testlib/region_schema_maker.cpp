#include "indexlib/testlib/region_schema_maker.h"
#include "indexlib/config/kv_index_config.h"

using namespace std;

IE_NAMESPACE_USE(config);

IE_NAMESPACE_BEGIN(testlib);
IE_LOG_SETUP(testlib, RegionSchemaMaker);

RegionSchemaMaker::RegionSchemaMaker(
        const std::string& fieldNames, const std::string& tableType)
{
    assert(tableType == "kv" || tableType == "kkv");
    mSchema.reset(new IndexPartitionSchema);
    mSchema->SetTableType(tableType);
    SchemaMaker::MakeFieldConfigForSchema(mSchema, fieldNames);
}

RegionSchemaMaker::~RegionSchemaMaker() 
{
}

void RegionSchemaMaker::AddRegion(
        const string& regionName, const string& pkeyName,
        const string& skeyName, const string& valueNames,
        const string& fieldNames, int64_t ttl)
{
    assert(mSchema->GetTableType() == tt_kkv);
    regionid_t regionId = (regionid_t)mSchema->GetRegionCount();
    RegionSchemaPtr region(new RegionSchema(mSchema->GetImpl().get()));
    region->SetRegionName(regionName);
    if (ttl != INVALID_TTL)
    {
        region->SetDefaultTTL(ttl);
        region->SetEnableTTL(true);
    }
    if (!fieldNames.empty())
    {
        FieldSchemaPtr fieldSchema =
            SchemaMaker::MakeFieldSchema(fieldNames);
        region->SetFieldSchema(fieldSchema);
    }
    
    mSchema->AddRegionSchema(region);
    SchemaMaker::MakeAttributeConfigForSchema(mSchema, valueNames, regionId);
    SchemaMaker::MakeKKVIndex(mSchema, pkeyName, skeyName, regionId, ttl);
}

void RegionSchemaMaker::AddRegion(
        const string& regionName, const string& pkeyName,
        const string& valueNames, const string& fieldNames,
        int64_t ttl)
{
    assert(mSchema->GetTableType() == tt_kv);
    regionid_t regionId = (regionid_t)mSchema->GetRegionCount();
    RegionSchemaPtr region(new RegionSchema(mSchema->GetImpl().get()));
    region->SetRegionName(regionName);
    if (ttl != INVALID_TTL)
    {
        region->SetDefaultTTL(ttl);
        region->SetEnableTTL(true);
    }
    if (!fieldNames.empty())
    {
        FieldSchemaPtr fieldSchema =
            SchemaMaker::MakeFieldSchema(fieldNames);
        region->SetFieldSchema(fieldSchema);
    }
    
    mSchema->AddRegionSchema(region);
    SchemaMaker::MakeAttributeConfigForSchema(mSchema, valueNames, regionId);
    SchemaMaker::MakeKVIndex(mSchema, pkeyName, regionId, ttl);
}

const IndexPartitionSchemaPtr& RegionSchemaMaker::GetSchema() const
{
    assert (mSchema->GetTableType() == tt_kv || mSchema->GetTableType() == tt_kkv);
    for (regionid_t i = 0; i < (regionid_t)mSchema->GetRegionCount(); i++)
    {
        const IndexSchemaPtr& indexSchema = mSchema->GetIndexSchema(i);
        assert(indexSchema);
        const SingleFieldIndexConfigPtr& pkConfig = indexSchema->GetPrimaryKeyIndexConfig();
        KVIndexConfigPtr kvConfig = DYNAMIC_POINTER_CAST(KVIndexConfig, pkConfig);
        assert(kvConfig);
        kvConfig->SetRegionInfo(i, mSchema->GetRegionCount());
    }
    return mSchema;
}

IE_NAMESPACE_END(testlib);

