#include "indexlib/testlib/schema_maker.h"
#include "indexlib/config/schema_configurator.h"
#include <autil/StringTokenizer.h>
#include <autil/StringUtil.h>
#include "indexlib/config/configurator_define.h"
#include "indexlib/config/index_config_creator.h"
#include "indexlib/config/primary_key_index_config.h"
#include "indexlib/config/impl/primary_key_index_config_impl.h"
#include "indexlib/config/kkv_index_config.h"
#include "indexlib/config/impl/kkv_index_config_impl.h"
#include "indexlib/config/number_index_type_transformor.h"

using namespace std;
using namespace autil;
IE_NAMESPACE_USE(config);

IE_NAMESPACE_BEGIN(testlib);
IE_LOG_SETUP(testlib, SchemaMaker);

IndexPartitionSchemaPtr SchemaMaker::MakeSchema(
        const string& fieldNames, const string& indexNames, 
        const string& attributeNames, const string& summaryNames,
        const string& truncateProfileStr)
{
    IndexPartitionSchemaPtr schema(new IndexPartitionSchema);
    if (fieldNames.size() > 0)
    {
        MakeFieldConfigSchema(schema, SplitToStringVector(fieldNames));
    }
    if (indexNames.size() > 0)
    {
        MakeIndexConfigSchema(schema, indexNames);
    }

    if (attributeNames.size() > 0)
    {
        MakeAttributeConfigSchema(schema, SplitToStringVector(attributeNames));
    }
    if (summaryNames.size() > 0)
    {
        MakeSummaryConfigSchema(schema, SplitToStringVector(summaryNames));
    }
    schema->GetRegionSchema(DEFAULT_REGIONID)->SetNeedStoreSummary();
    
    if (truncateProfileStr.size() > 0)
    {
        MakeTruncateProfiles(schema, truncateProfileStr);
    }

    schema->GetRegionSchema(DEFAULT_REGIONID)->EnsureSpatialIndexWithAttribute();
    return schema;
}

IndexPartitionSchemaPtr SchemaMaker::MakeKKVSchema(
        const string& fieldNames, const string& pkeyName,
        const string& skeyName, const string& valueNames)
{
    IndexPartitionSchemaPtr schema(new IndexPartitionSchema);
    schema->SetTableType("kkv");
    MakeFieldConfigSchema(schema, SplitToStringVector(fieldNames));
    MakeAttributeConfigSchema(schema, SplitToStringVector(valueNames));
    MakeKKVIndex(schema, pkeyName, skeyName);
    return schema;
}

config::IndexPartitionSchemaPtr SchemaMaker::MakeKVSchema(
        const std::string& fieldNames,
        const std::string& keyName,
        const std::string& valueNames,
        int64_t ttl)
{
    IndexPartitionSchemaPtr schema(new IndexPartitionSchema);
    schema->SetTableType("kv");
    MakeFieldConfigSchema(schema, SplitToStringVector(fieldNames));
    MakeAttributeConfigSchema(schema, SplitToStringVector(valueNames));
    MakeKVIndex(schema, keyName, DEFAULT_REGIONID, ttl);
    return schema;
}

SchemaMaker::StringVector 
SchemaMaker::SplitToStringVector(const string& names)
{
    StringVector sv;
    autil::StringTokenizer st(names, ";", autil::StringTokenizer::TOKEN_TRIM 
                       | autil::StringTokenizer::TOKEN_IGNORE_EMPTY);
    for (autil::StringTokenizer::Iterator it = st.begin(); it != st.end(); ++it)
    {
        sv.push_back(*it);
    }
    return sv;
}

void SchemaMaker::MakeFieldConfigSchema(
        IndexPartitionSchemaPtr& schema,
        const StringVector& fieldNames)
{
    // make fieldSchema
    size_t i;
    for (i = 0; i < fieldNames.size(); ++i)
    {

        autil::StringTokenizer st(fieldNames[i], ":", autil::StringTokenizer::TOKEN_TRIM);

        assert(st.getNumTokens() >= 2 && st.getNumTokens() <= 6);
        
        bool isMultiValue = false;
        if (st.getNumTokens() >= 3)
        {
            isMultiValue = (st[2] == "true");
        }
        FieldType fieldType = FieldConfig::StrToFieldType(st[1]);
        schema->AddFieldConfig(st[0], fieldType, isMultiValue);
        
        if (st.getNumTokens() >= 4)
        {
            if (st[3] == "true")
            {
                FieldConfigPtr fieldConfig = schema->GetFieldSchema()->GetFieldConfig(st[0]);
                fieldConfig->SetUpdatableMultiValue(true);
            }
        }

        if (st.getNumTokens() >= 5 && !st[4].empty())
        {
            FieldConfigPtr fieldConfig = schema->GetFieldSchema()->GetFieldConfig(st[0]);
            fieldConfig->SetCompressType(st[4]);
        }

        if (st.getNumTokens() >= 6)
        {
            FieldConfigPtr fieldConfig = schema->GetFieldSchema()->GetFieldConfig(st[0]);
            fieldConfig->SetFixedMultiValueCount(StringUtil::fromString<int32_t>(st[5]));
        }
                
        if (fieldType == ft_text)
        {   
            FieldConfigPtr fieldConfig = schema->GetFieldSchema()->GetFieldConfig(st[0]);
            fieldConfig->SetAnalyzerName("taobao_analyzer");
        }
    }
}

FieldSchemaPtr SchemaMaker::MakeFieldSchema(
        const string& fieldNames)
{
    StringVector fields = SplitToStringVector(fieldNames);
    FieldSchemaPtr fieldSchema(new FieldSchema(fields.size()));
    // make fieldSchema
    size_t i;
    for (i = 0; i < fields.size(); ++i)
    {
        // fieldName:fieldType:[isMultiValue]:[isUpdatable]:[compressType]:[fixCount]        
        autil::StringTokenizer st(fields[i], ":", autil::StringTokenizer::TOKEN_TRIM);

        assert(st.getNumTokens() >= 2 && st.getNumTokens() <= 6);
        
        bool isMultiValue = false;
        if (st.getNumTokens() >= 3)
        {
            isMultiValue = (st[2] == "true");
        }
        FieldType fieldType = FieldConfig::StrToFieldType(st[1]);
        FieldConfigPtr fieldConfig(
                new FieldConfig(st[0], fieldType, isMultiValue));
        
        if (st.getNumTokens() >= 4)
        {
            if (st[3] == "true")
            {
                fieldConfig->SetUpdatableMultiValue(true);
            }
        }

        if (st.getNumTokens() >= 5 && !st[4].empty())
        {
            fieldConfig->SetCompressType(st[4]);
        }

        if (st.getNumTokens() >= 6)
        {
            fieldConfig->SetFixedMultiValueCount(
                    StringUtil::fromString<int32_t>(st[5]));
        }
                
        if (fieldType == ft_text)
        {   
            fieldConfig->SetAnalyzerName("taobao_analyzer");
        }
        fieldSchema->AddFieldConfig(fieldConfig);
    }
    return fieldSchema;
}

void SchemaMaker::SetAdaptiveHighFrequenceDictionary(const string& indexName,
        const std::string& adaptiveRuleStr,
        HighFrequencyTermPostingType type,
        const config::IndexPartitionSchemaPtr& schema){
    IndexSchemaPtr indexSchema = schema->GetIndexSchema();
    assert(indexSchema != NULL);
    IndexConfigPtr indexConfig = indexSchema->GetIndexConfig(indexName);
    assert(indexConfig != NULL);

    vector<string> adaptiveParams = StringUtil::split(adaptiveRuleStr, "#");
    assert(adaptiveParams.size() == 2);

    AdaptiveDictionaryConfigPtr dictConfig(new AdaptiveDictionaryConfig("adaptive_rule",
                    adaptiveParams[0], StringUtil::fromString<int32_t>(adaptiveParams[1])));
    AdaptiveDictionarySchemaPtr adaptiveDictSchema(new AdaptiveDictionarySchema);
    adaptiveDictSchema->AddAdaptiveDictionaryConfig(dictConfig);
    schema->SetAdaptiveDictSchema(adaptiveDictSchema);
    indexConfig->SetAdaptiveDictConfig(dictConfig);
    indexConfig->SetHighFreqencyTermPostingType(type);
}

vector<IndexConfigPtr> SchemaMaker::MakeIndexConfigs(
        const string& indexNames, const IndexPartitionSchemaPtr& schema)
{
    StringVector fieldNames = SplitToStringVector(indexNames);
    vector<IndexConfigPtr> ret;
    for (size_t i = 0; i < fieldNames.size(); ++i)
    {
        autil::StringTokenizer st(fieldNames[i], ":", autil::StringTokenizer::TOKEN_TRIM 
                | autil::StringTokenizer::TOKEN_IGNORE_EMPTY);
        assert(st.getNumTokens() >= 3);
        string indexName = st[0];
        IndexType indexType = IndexConfig::StrToIndexType(st[1], tt_index);
        string fieldName = st[2];
        bool hasTruncate = false;
        if ((st.getNumTokens() >= 4) && (st[3] == string("true")))
        {
            hasTruncate = true;
        }
        size_t shardingCount = 0;
        if (st.getNumTokens() >= 5)
        {
            shardingCount = StringUtil::fromString<size_t>(st[4]);
        }
        assert(shardingCount != 1);

        bool useHashDict = false;
        if (st.getNumTokens() == 6)
        {
            useHashDict = StringUtil::fromString<bool>(st[5]);
        }
        
        autil::StringTokenizer st2(fieldName, ",", autil::StringTokenizer::TOKEN_TRIM 
                | autil::StringTokenizer::TOKEN_IGNORE_EMPTY);

        if (st2.getNumTokens() == 1 && indexType != it_pack 
            && indexType != it_expack && indexType != it_customized)
        {
            SingleFieldIndexConfigPtr indexConfig = 
                IndexConfigCreator::CreateSingleFieldIndexConfig(
                        indexName, indexType, st2[0], "", false,
                        schema->GetFieldSchema());

            indexConfig->SetHasTruncateFlag(hasTruncate);

            if (indexType == it_primarykey64 
                || indexType == it_primarykey128)
            {
                indexConfig->SetPrimaryKeyAttributeFlag(true);
                indexConfig->SetOptionFlag(of_none);
                if (st.getNumTokens() > 3) {
                    bool numberHash = st[3] == "number_hash";
                    if (numberHash) {
                        PrimaryKeyIndexConfig *pkIndexConfig =
                            dynamic_cast<PrimaryKeyIndexConfig *>(indexConfig.get());
                        pkIndexConfig->SetPrimaryKeyHashType(pk_number_hash);
                    }
                }
            }

            if (indexType == it_string || indexType == it_number)
            {
                indexConfig->SetOptionFlag(of_none | of_term_frequency);
            }

            if (indexType == it_number)
            {
                NumberIndexTypeTransformor::Transform(indexConfig, indexType);
                indexConfig->SetIndexType(indexType);
            }

            if (useHashDict)
            {
                assert(indexType != it_range && indexType != it_date);
                indexConfig->SetHashTypedDictionary(true);
            }
            
            if (shardingCount >= 2)
            {
                IndexConfigCreator::CreateShardingIndexConfigs(
                        indexConfig, shardingCount);
            }
            ret.push_back(indexConfig);
        }
        else 
        {
            vector<int32_t> boostVec;
            boostVec.resize(st2.getTokenVector().size(), 0);
            PackageIndexConfigPtr indexConfig = 
                IndexConfigCreator::CreatePackageIndexConfig(
                        indexName, indexType, st2.getTokenVector(), boostVec, "",
                        false, schema->GetFieldSchema());
            indexConfig->SetHasTruncateFlag(hasTruncate);
   
            if (indexType == it_number)
            {
                NumberIndexTypeTransformor::Transform(indexConfig, indexType);
                indexConfig->SetIndexType(indexType);
            }
            if (useHashDict)
            {
                indexConfig->SetHashTypedDictionary(true);
            }
            if (shardingCount >= 2)
            {
                IndexConfigCreator::CreateShardingIndexConfigs(
                        indexConfig, shardingCount);
            }
            ret.push_back(indexConfig);            
        }
    }
    return ret;
}

void SchemaMaker::MakeIndexConfigSchema(
        IndexPartitionSchemaPtr& schema, const string& indexNames)
{
    vector<IndexConfigPtr> indexs = MakeIndexConfigs(indexNames, schema);
    for (auto& index : indexs)
    {
        schema->AddIndexConfig(index);
    }
}

void SchemaMaker::MakeAttributeConfigSchema(
        IndexPartitionSchemaPtr& schema,
        const StringVector& fieldNames, regionid_t regionId)
{
    for (size_t i = 0; i < fieldNames.size(); ++i)
    {
        StringTokenizer st(fieldNames[i], ":", StringTokenizer::TOKEN_IGNORE_EMPTY
                           | StringTokenizer::TOKEN_TRIM);
        assert(st.getNumTokens() > 0 && st.getNumTokens() <= 3);
        if (st.getNumTokens() == 1)
        {
            schema->AddAttributeConfig(st[0], regionId);
            continue;
        }

        vector<string> subAttrNames;
        StringUtil::fromString(st[1], subAttrNames, ",");
        assert(subAttrNames.size() >= 1);
        string compressStr = (st.getNumTokens() == 3) ? st[2] : string("");
        schema->AddPackAttributeConfig(st[0], subAttrNames, compressStr, regionId);
    }
}

void SchemaMaker::MakeSummaryConfigSchema(
        IndexPartitionSchemaPtr& schema,
        const StringVector& fieldNames)
{
    // make SummarySchema
    for (size_t i = 0; i < fieldNames.size(); ++i)
    {
        schema->AddSummaryConfig(fieldNames[i]);
    }
}

// truncateName0:sortDescription0#truncateName1:sortDescription1...
void SchemaMaker::MakeTruncateProfiles(
    IndexPartitionSchemaPtr& schema,
    const string& truncateProfileStr)
{
    vector<vector<string> > truncateProfileVec;
    StringUtil::fromString(truncateProfileStr, truncateProfileVec, ":", "#");
    TruncateProfileSchemaPtr truncateProfileSchema(new TruncateProfileSchema());
    for (size_t i = 0 ; i < truncateProfileVec.size(); ++i)
    {
	string profileName = truncateProfileVec[i][0];
	string sortDesp =
	    (truncateProfileVec[i].size() > 1) ? truncateProfileVec[i][1] : "";
	TruncateProfileConfigPtr truncateProfile(
	    new TruncateProfileConfig(profileName, sortDesp));
	truncateProfileSchema->AddTruncateProfileConfig(truncateProfile);
    }
    schema->SetTruncateProfileSchema(truncateProfileSchema);
}

void SchemaMaker::MakeKKVIndex(IndexPartitionSchemaPtr& schema,
                               const string& pkeyName, const string& skeyName,
                               regionid_t id, int64_t ttl)
{
    KKVIndexConfigPtr kkvIndexConfig(new KKVIndexConfig(pkeyName, it_kkv));
    FieldSchemaPtr fieldSchema = schema->GetFieldSchema(id);
    FieldConfigPtr prefixFieldConfig = fieldSchema->GetFieldConfig(pkeyName);
    if (!prefixFieldConfig)
    {
        INDEXLIB_FATAL_ERROR(Schema, "kkv field %s not exist in field schema",
                             pkeyName.c_str());
    }
    kkvIndexConfig->SetPrefixFieldConfig(prefixFieldConfig);
    FieldConfigPtr suffixFieldConfig = fieldSchema->GetFieldConfig(skeyName);
    if (!suffixFieldConfig)
    {
        INDEXLIB_FATAL_ERROR(Schema, "kkv field %s not exist in field schema",
                             skeyName.c_str());
    }
    kkvIndexConfig->SetSuffixFieldConfig(suffixFieldConfig);
    if (ttl != INVALID_TTL)
    {
        kkvIndexConfig->SetTTL(ttl);   
    }
    KKVIndexFieldInfo pkeyFieldInfo;
    pkeyFieldInfo.fieldName = pkeyName;
    pkeyFieldInfo.keyType = kkv_prefix;
    kkvIndexConfig->SetPrefixFieldInfo(pkeyFieldInfo);
    KKVIndexFieldInfo skeyFieldInfo;
    skeyFieldInfo.fieldName = skeyName;
    skeyFieldInfo.keyType = kkv_suffix;
    kkvIndexConfig->SetSuffixFieldInfo(skeyFieldInfo);
    schema->AddIndexConfig(kkvIndexConfig, id);
    kkvIndexConfig->SetRegionInfo(DEFAULT_REGIONID, 1);
    schema->LoadValueConfig(id);
}

void SchemaMaker::MakeKVIndex(IndexPartitionSchemaPtr& schema,
                              const string& keyName, regionid_t id,
                              int64_t ttl)
{
    KVIndexConfigPtr kvIndexConfig(new KVIndexConfig(keyName, it_kv));
    FieldSchemaPtr fieldSchema = schema->GetFieldSchema(id);
    FieldConfigPtr fieldConfig = fieldSchema->GetFieldConfig(keyName);
    if (!fieldConfig)
    {
        INDEXLIB_FATAL_ERROR(Schema, "kv field %s not exist in field schema",
                             keyName.c_str());
    }
    kvIndexConfig->SetFieldConfig(fieldConfig);
    if (ttl != INVALID_TTL)
    {
        kvIndexConfig->SetTTL(ttl);
    }
    schema->AddIndexConfig(kvIndexConfig, id);
    kvIndexConfig->SetRegionInfo(DEFAULT_REGIONID, 1);
    schema->LoadValueConfig(id);
}

IE_NAMESPACE_END(testlib);
