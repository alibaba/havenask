/*
 * Copyright 2014-present Alibaba Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#include "indexlib/test/region_schema_maker.h"

#include "indexlib/config/kv_index_config.h"
#include "indexlib/test/schema_maker.h"

using namespace std;

using namespace indexlib::config;

namespace indexlib { namespace test {
IE_LOG_SETUP(test, RegionSchemaMaker);

RegionSchemaMaker::RegionSchemaMaker(const std::string& fieldNames, const std::string& tableType)
{
    assert(tableType == "kv" || tableType == "kkv");
    mSchema.reset(new IndexPartitionSchema);
    mSchema->SetTableType(tableType);
    SchemaMaker::MakeFieldConfigForSchema(mSchema, fieldNames);
}

RegionSchemaMaker::~RegionSchemaMaker() {}

void RegionSchemaMaker::AddRegion(const string& regionName, const string& pkeyName, const string& skeyName,
                                  const string& valueNames, const string& fieldNames, int64_t ttl, bool impactValue)
{
    assert(mSchema->GetTableType() == tt_kkv);
    regionid_t regionId = (regionid_t)mSchema->GetRegionCount();
    RegionSchemaPtr region(new RegionSchema(mSchema->GetImpl().get()));
    region->SetRegionName(regionName);
    if (!fieldNames.empty()) {
        FieldSchemaPtr fieldSchema = SchemaMaker::MakeFieldSchema(fieldNames);
        region->SetFieldSchema(fieldSchema);
    }

    mSchema->AddRegionSchema(region);
    SchemaMaker::MakeAttributeConfigForSchema(mSchema, valueNames, regionId);
    if (impactValue) {
        SchemaMaker::MakeKKVIndex(mSchema, pkeyName, skeyName, regionId, ttl, "impact");
    } else {
        SchemaMaker::MakeKKVIndex(mSchema, pkeyName, skeyName, regionId, ttl, "");
    }
    if (ttl != INVALID_TTL) {
        region->SetDefaultTTL(ttl);
        region->SetEnableTTL(true);
    }
}

void RegionSchemaMaker::AddRegion(const string& regionName, const string& pkeyName, const string& valueNames,
                                  const string& fieldNames, int64_t ttl, bool impactValue)
{
    assert(mSchema->GetTableType() == tt_kv);
    regionid_t regionId = (regionid_t)mSchema->GetRegionCount();
    RegionSchemaPtr region(new RegionSchema(mSchema->GetImpl().get()));
    region->SetRegionName(regionName);

    if (!fieldNames.empty()) {
        FieldSchemaPtr fieldSchema = SchemaMaker::MakeFieldSchema(fieldNames);
        region->SetFieldSchema(fieldSchema);
    }

    mSchema->AddRegionSchema(region);
    SchemaMaker::MakeAttributeConfigForSchema(mSchema, valueNames, regionId);
    if (impactValue) {
        SchemaMaker::MakeKVIndex(mSchema, pkeyName, regionId, ttl, "impact");
    } else {
        SchemaMaker::MakeKVIndex(mSchema, pkeyName, regionId, ttl, "");
    }
    if (ttl != INVALID_TTL) {
        region->SetDefaultTTL(ttl);
        region->SetEnableTTL(true);
    }
}

const IndexPartitionSchemaPtr& RegionSchemaMaker::GetSchema() const
{
    assert(mSchema->GetTableType() == tt_kv || mSchema->GetTableType() == tt_kkv);
    for (regionid_t i = 0; i < (regionid_t)mSchema->GetRegionCount(); i++) {
        const IndexSchemaPtr& indexSchema = mSchema->GetIndexSchema(i);
        assert(indexSchema);
        const SingleFieldIndexConfigPtr& pkConfig = indexSchema->GetPrimaryKeyIndexConfig();
        KVIndexConfigPtr kvConfig = DYNAMIC_POINTER_CAST(KVIndexConfig, pkConfig);
        assert(kvConfig);
        kvConfig->SetRegionInfo(i, mSchema->GetRegionCount());
    }
    return mSchema;
}
}} // namespace indexlib::test
