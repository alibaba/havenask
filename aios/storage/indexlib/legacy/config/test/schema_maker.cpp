#include "indexlib/config/test/schema_maker.h"

#include "autil/EnvUtil.h"
#include "autil/StringTokenizer.h"
#include "autil/StringUtil.h"
#include "autil/legacy/json.h"
#include "indexlib/config/FileCompressConfig.h"
#include "indexlib/config/configurator_define.h"
#include "indexlib/config/customized_index_config.h"
#include "indexlib/config/index_config_creator.h"
#include "indexlib/config/kkv_index_config.h"
#include "indexlib/config/number_index_type_transformor.h"
#include "indexlib/config/primary_key_index_config.h"
#include "indexlib/config/schema_configurator.h"
#include "indexlib/config/source_group_config.h"
#include "indexlib/config/source_schema.h"
#include "indexlib/framework/TabletSchemaLoader.h"

using namespace std;
using namespace autil;
using namespace indexlib::config;

namespace indexlib { namespace test {
AUTIL_LOG_SETUP(indexlib.config, SchemaMaker);

IndexPartitionSchemaPtr SchemaMaker::MakeSchema(const string& fieldNames, const string& indexNames,
                                                const string& attributeNames, const string& summaryNames,
                                                const string& truncateProfileStr, const string& sourceFields)
{
    IndexPartitionSchemaPtr schema(new IndexPartitionSchema);
    if (fieldNames.size() > 0) {
        MakeFieldConfigSchema(schema, SplitToStringVector(fieldNames));
    }
    if (indexNames.size() > 0) {
        MakeIndexConfigSchema(schema, indexNames);
    }

    if (attributeNames.size() > 0) {
        MakeAttributeConfigSchema(schema, SplitToStringVector(attributeNames));
    }
    if (summaryNames.size() > 0) {
        MakeSummaryConfigSchema(schema, SplitToStringVector(summaryNames));
    }
    schema->GetRegionSchema(DEFAULT_REGIONID)->SetNeedStoreSummary();

    if (truncateProfileStr.size() > 0) {
        MakeTruncateProfiles(schema, truncateProfileStr);
        ResolveEmptyProfileNamesForTruncateIndex(schema->GetTruncateProfileSchema(), schema->GetIndexSchema());
    }
    if (!sourceFields.empty()) {
        MakeSourceSchema(schema, sourceFields);
    }
    schema->GetRegionSchema(DEFAULT_REGIONID)->EnsureSpatialIndexWithAttribute();
    return schema;
}

void SchemaMaker::ResolveEmptyProfileNamesForTruncateIndex(const TruncateProfileSchemaPtr& truncateProfileSchema,
                                                           const IndexSchemaPtr& indexSchema)
{
    std::vector<std::string> usingTruncateNames;
    for (auto iter = truncateProfileSchema->Begin(); iter != truncateProfileSchema->End(); iter++) {
        if (iter->second->GetPayloadConfig().IsInitialized()) {
            continue;
        }
        usingTruncateNames.push_back(iter->first);
    }
    for (auto it = indexSchema->Begin(); it != indexSchema->End(); ++it) {
        const IndexConfigPtr& indexConfig = *it;
        if (indexConfig->GetShardingType() == IndexConfig::IST_IS_SHARDING || !indexConfig->HasTruncate()) {
            continue;
        }
        auto useTruncateProfiles = indexConfig->GetUseTruncateProfiles();
        if (useTruncateProfiles.size() == 0) {
            indexConfig->SetUseTruncateProfiles(usingTruncateNames);
        }
    }
}

IndexPartitionSchemaPtr SchemaMaker::MakeKKVSchema(const string& fieldNames, const string& pkeyName,
                                                   const string& skeyName, const string& valueNames, int64_t ttl,
                                                   const std::string& valueFormat)
{
    IndexPartitionSchemaPtr schema(new IndexPartitionSchema);
    schema->SetTableType("kkv");
    MakeFieldConfigSchema(schema, SplitToStringVector(fieldNames));
    MakeAttributeConfigSchema(schema, SplitToStringVector(valueNames));
    MakeKKVIndex(schema, pkeyName, skeyName, DEFAULT_REGIONID, ttl, valueFormat);
    schema->SetDefaultTTL(ttl);
    return schema;
}

config::IndexPartitionSchemaPtr SchemaMaker::MakeKVSchema(const std::string& fieldNames, const std::string& keyName,
                                                          const std::string& valueNames, int64_t ttl,
                                                          const std::string& valueFormat, bool useNumberHash)
{
    IndexPartitionSchemaPtr schema(new IndexPartitionSchema);
    schema->SetTableType("kv");
    MakeFieldConfigSchema(schema, SplitToStringVector(fieldNames));
    MakeAttributeConfigSchema(schema, SplitToStringVector(valueNames));
    MakeKVIndex(schema, keyName, DEFAULT_REGIONID, ttl, valueFormat, useNumberHash);
    schema->SetDefaultTTL(ttl);
    return schema;
}

shared_ptr<indexlibv2::config::TabletSchema> SchemaMaker::MakeKVTabletSchema(const std::string& fieldNames,
                                                                             const std::string& keyName,
                                                                             const std::string& valueNames, int64_t ttl,
                                                                             const std::string& valueFormat,
                                                                             bool useNumberHash)
{
    auto partitionSchema = MakeKVSchema(fieldNames, keyName, valueNames, ttl, valueFormat, useNumberHash);
    if (!partitionSchema) {
        return nullptr;
    }
    auto jsonStr = autil::legacy::ToJsonString(*partitionSchema);
    return indexlibv2::framework::TabletSchemaLoader::LoadSchema(jsonStr);
}

shared_ptr<indexlibv2::config::TabletSchema>
SchemaMaker::MakeKKVTabletSchema(const string& fieldNames, const string& pkeyName, const string& skeyName,
                                 const string& valueNames, int64_t ttl, const std::string& valueFormat)
{
    auto partitionSchema = MakeKKVSchema(fieldNames, pkeyName, skeyName, valueNames, ttl, valueFormat);
    if (!partitionSchema) {
        return nullptr;
    }
    auto jsonStr = autil::legacy::ToJsonString(*partitionSchema);
    return indexlibv2::framework::TabletSchemaLoader::LoadSchema(jsonStr);
}

SchemaMaker::StringVector SchemaMaker::SplitToStringVector(const string& names)
{
    StringVector sv;
    autil::StringTokenizer st(names, ";",
                              autil::StringTokenizer::TOKEN_TRIM | autil::StringTokenizer::TOKEN_IGNORE_EMPTY);
    for (autil::StringTokenizer::Iterator it = st.begin(); it != st.end(); ++it) {
        sv.push_back(*it);
    }
    return sv;
}

void SchemaMaker::MakeFieldConfigSchema(IndexPartitionSchemaPtr& schema, const StringVector& fieldNames)
{
    // make fieldSchema
    size_t i;
    for (i = 0; i < fieldNames.size(); ++i) {
        autil::StringTokenizer st(fieldNames[i], ":", autil::StringTokenizer::TOKEN_TRIM);

        assert(st.getNumTokens() >= 2 && st.getNumTokens() <= 7);

        bool isMultiValue = false;
        if (st.getNumTokens() >= 3) {
            isMultiValue = (st[2] == "true");
        }
        FieldType fieldType = FieldConfig::StrToFieldType(st[1]);
        schema->AddFieldConfig(st[0], fieldType, isMultiValue);

        if (st.getNumTokens() >= 4) {
            if (st[3] == "true") {
                FieldConfigPtr fieldConfig = schema->GetFieldSchema()->GetFieldConfig(st[0]);
                fieldConfig->SetUpdatableMultiValue(true);
            }
        }

        if (st.getNumTokens() >= 5 && !st[4].empty()) {
            FieldConfigPtr fieldConfig = schema->GetFieldSchema()->GetFieldConfig(st[0]);
            fieldConfig->SetCompressType(st[4]);
        }

        if (st.getNumTokens() >= 6) {
            FieldConfigPtr fieldConfig = schema->GetFieldSchema()->GetFieldConfig(st[0]);
            fieldConfig->SetFixedMultiValueCount(StringUtil::fromString<int32_t>(st[5]));
        }

        if (st.getNumTokens() >= 7 && st[6] == "true") {
            FieldConfigPtr fieldConfig = schema->GetFieldSchema()->GetFieldConfig(st[0]);
            fieldConfig->SetEnableNullField(true);
        }

        if (fieldType == ft_text) {
            FieldConfigPtr fieldConfig = schema->GetFieldSchema()->GetFieldConfig(st[0]);
            fieldConfig->SetAnalyzerName("taobao_analyzer");
        }
    }
}

FieldSchemaPtr SchemaMaker::MakeFieldSchema(const string& fieldNames)
{
    StringVector fields = SplitToStringVector(fieldNames);
    FieldSchemaPtr fieldSchema(new FieldSchema(fields.size()));
    // make fieldSchema
    size_t i;
    for (i = 0; i < fields.size(); ++i) {
        // fieldName:fieldType:[isMultiValue]:[isUpdatable]:[compressType]:[fixCount]
        autil::StringTokenizer st(fields[i], ":", autil::StringTokenizer::TOKEN_TRIM);

        assert(st.getNumTokens() >= 2 && st.getNumTokens() <= 6);

        bool isMultiValue = false;
        if (st.getNumTokens() >= 3) {
            isMultiValue = (st[2] == "true");
        }
        FieldType fieldType = FieldConfig::StrToFieldType(st[1]);
        FieldConfigPtr fieldConfig(new FieldConfig(st[0], fieldType, isMultiValue));

        if (st.getNumTokens() >= 4) {
            if (st[3] == "true") {
                fieldConfig->SetUpdatableMultiValue(true);
            }
        }

        if (st.getNumTokens() >= 5 && !st[4].empty()) {
            fieldConfig->SetCompressType(st[4]);
        }

        if (st.getNumTokens() >= 6) {
            fieldConfig->SetFixedMultiValueCount(StringUtil::fromString<int32_t>(st[5]));
        }

        if (fieldType == ft_text) {
            fieldConfig->SetAnalyzerName("taobao_analyzer");
        }
        fieldSchema->AddFieldConfig(fieldConfig);
    }
    return fieldSchema;
}

void SchemaMaker::SetAdaptiveHighFrequenceDictionary(const string& indexName, const std::string& adaptiveRuleStr,
                                                     index::HighFrequencyTermPostingType type,
                                                     const config::IndexPartitionSchemaPtr& schema)
{
    IndexSchemaPtr indexSchema = schema->GetIndexSchema();
    assert(indexSchema != NULL);
    IndexConfigPtr indexConfig = indexSchema->GetIndexConfig(indexName);
    assert(indexConfig != NULL);

    vector<string> adaptiveParams = StringUtil::split(adaptiveRuleStr, "#");
    assert(adaptiveParams.size() == 2);

    std::shared_ptr<AdaptiveDictionaryConfig> dictConfig(new AdaptiveDictionaryConfig(
        "adaptive_rule", adaptiveParams[0], StringUtil::fromString<int32_t>(adaptiveParams[1])));
    AdaptiveDictionarySchemaPtr adaptiveDictSchema(new AdaptiveDictionarySchema);
    adaptiveDictSchema->AddAdaptiveDictionaryConfig(dictConfig);
    schema->SetAdaptiveDictSchema(adaptiveDictSchema);
    indexConfig->SetAdaptiveDictConfig(dictConfig);
    indexConfig->SetHighFreqencyTermPostingType(type);
}

vector<IndexConfigPtr> SchemaMaker::MakeIndexConfigs(const string& indexNames, const IndexPartitionSchemaPtr& schema)
{
    StringVector fieldNames = SplitToStringVector(indexNames);
    vector<IndexConfigPtr> ret;
    for (size_t i = 0; i < fieldNames.size(); ++i) {
        autil::StringTokenizer st(fieldNames[i], ":",
                                  autil::StringTokenizer::TOKEN_TRIM | autil::StringTokenizer::TOKEN_IGNORE_EMPTY);
        assert(st.getNumTokens() >= 3);
        string indexName = st[0];
        InvertedIndexType indexType = IndexConfig::StrToIndexType(st[1], tt_index);
        string fieldName = st[2];
        bool hasTruncate = false;
        if ((st.getNumTokens() >= 4) && (st[3] == string("true"))) {
            hasTruncate = true;
        }
        size_t shardingCount = 0;
        if (st.getNumTokens() >= 5) {
            shardingCount = StringUtil::fromString<size_t>(st[4]);
        }
        assert(shardingCount != 1);

        bool useHashDict = false;
        if (st.getNumTokens() == 6) {
            useHashDict = StringUtil::fromString<bool>(st[5]);
        }

        autil::StringTokenizer st2(fieldName, ",",
                                   autil::StringTokenizer::TOKEN_TRIM | autil::StringTokenizer::TOKEN_IGNORE_EMPTY);

        if (st2.getNumTokens() == 1 && indexType != it_pack && indexType != it_expack && indexType != it_customized) {
            SingleFieldIndexConfigPtr indexConfig = IndexConfigCreator::CreateSingleFieldIndexConfig(
                indexName, indexType, st2[0], "", false, schema->GetFieldSchema());

            indexConfig->SetHasTruncateFlag(hasTruncate);

            if (indexType == it_primarykey64 || indexType == it_primarykey128) {
                indexConfig->SetPrimaryKeyAttributeFlag(true);
                indexConfig->SetOptionFlag(of_none);
                if (st.getNumTokens() > 3) {
                    bool numberHash = st[3] == "number_hash";
                    if (numberHash) {
                        PrimaryKeyIndexConfig* pkIndexConfig = dynamic_cast<PrimaryKeyIndexConfig*>(indexConfig.get());
                        pkIndexConfig->SetPrimaryKeyHashType(index::pk_number_hash);
                    }
                }
            }

            if (indexType == it_string || indexType == it_number) {
                indexConfig->SetOptionFlag(of_none | of_term_frequency);
            }

            if (indexType == it_number) {
                NumberIndexTypeTransformor::Transform(indexConfig, indexType);
                indexConfig->SetInvertedIndexType(indexType);
            }

            if (useHashDict) {
                assert(indexType != it_range && indexType != it_datetime);
                indexConfig->SetHashTypedDictionary(true);
            }

            if (shardingCount >= 2) {
                IndexConfigCreator::CreateShardingIndexConfigs(indexConfig, shardingCount);
            }
            ret.push_back(indexConfig);
        } else {
            vector<int32_t> boostVec;
            boostVec.resize(st2.getTokenVector().size(), 0);
            PackageIndexConfigPtr indexConfig = IndexConfigCreator::CreatePackageIndexConfig(
                indexName, indexType, st2.getTokenVector(), boostVec, "", false, schema->GetFieldSchema());
            indexConfig->SetHasTruncateFlag(hasTruncate);

            if (indexType == it_number) {
                NumberIndexTypeTransformor::Transform(indexConfig, indexType);
                indexConfig->SetInvertedIndexType(indexType);
            }
            if (useHashDict) {
                indexConfig->SetHashTypedDictionary(true);
            }
            if (shardingCount >= 2) {
                IndexConfigCreator::CreateShardingIndexConfigs(indexConfig, shardingCount);
            }
            if (indexType == it_customized) {
                auto customizedIndexConfig = std::dynamic_pointer_cast<CustomizedIndexConfig>(indexConfig);
                customizedIndexConfig->SetIndexer(indexName); // need st[7]?
            }
            ret.push_back(indexConfig);
        }
    }
    return ret;
}

void SchemaMaker::MakeIndexConfigSchema(IndexPartitionSchemaPtr& schema, const string& indexNames)
{
    vector<IndexConfigPtr> indexs = MakeIndexConfigs(indexNames, schema);
    for (auto& index : indexs) {
        schema->AddIndexConfig(index);
    }
}

void SchemaMaker::MakeAttributeConfigSchema(IndexPartitionSchemaPtr& schema, const StringVector& fieldNames,
                                            regionid_t regionId)
{
    for (size_t i = 0; i < fieldNames.size(); ++i) {
        StringTokenizer st(fieldNames[i], ":", StringTokenizer::TOKEN_IGNORE_EMPTY | StringTokenizer::TOKEN_TRIM);
        assert(st.getNumTokens() > 0 && st.getNumTokens() <= 3);
        if (st.getNumTokens() == 1) {
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

void SchemaMaker::MakeSummaryConfigSchema(IndexPartitionSchemaPtr& schema, const StringVector& fieldNames)
{
    // make SummarySchema
    for (size_t i = 0; i < fieldNames.size(); ++i) {
        schema->AddSummaryConfig(fieldNames[i]);
    }
}

// truncateName0:sortDescription0#truncateName1:sortDescription1...
void SchemaMaker::MakeTruncateProfiles(IndexPartitionSchemaPtr& schema, const string& truncateProfileStr)
{
    vector<vector<string>> truncateProfileVec;
    StringUtil::fromString(truncateProfileStr, truncateProfileVec, ":", "#");
    TruncateProfileSchemaPtr truncateProfileSchema(new TruncateProfileSchema());
    for (size_t i = 0; i < truncateProfileVec.size(); ++i) {
        string profileName = truncateProfileVec[i][0];
        string sortDesp = (truncateProfileVec[i].size() > 1) ? truncateProfileVec[i][1] : "";
        TruncateProfileConfigPtr truncateProfile(new TruncateProfileConfig(profileName, sortDesp));
        truncateProfileSchema->AddTruncateProfileConfig(truncateProfile);
    }
    schema->SetTruncateProfileSchema(truncateProfileSchema);
}

void SchemaMaker::MakeSourceSchema(IndexPartitionSchemaPtr& schema, const string& sourceFields)
{
    // 2 group
    // f1:f2:f3#f4:f5
    vector<vector<string>> fields;
    StringUtil::fromString(sourceFields, fields, ":", "#");
    SourceSchemaPtr sourceSchema(new SourceSchema);
    uint32_t groupCount = 0;
    for (auto f : fields) {
        SourceGroupConfigPtr groupConfig(new SourceGroupConfig);
        groupConfig->SetSpecifiedFields(f);
        groupConfig->SetFieldMode(SourceGroupConfig::SourceFieldMode::SPECIFIED_FIELD);
        groupConfig->SetGroupId(groupCount);
        sourceSchema->AddGroupConfig(groupConfig);
        groupCount++;
    }
    AUTIL_LOG(INFO, "make schema with source[%u]", groupCount);
    schema->TEST_SetSourceSchema(sourceSchema);
}

void SchemaMaker::MakeKKVIndex(IndexPartitionSchemaPtr& schema, const string& pkeyName, const string& skeyName,
                               regionid_t id, int64_t ttl, const std::string& valueFormat)
{
    KKVIndexConfigPtr kkvIndexConfig(new KKVIndexConfig(pkeyName, it_kkv));
    FieldSchemaPtr fieldSchema = schema->GetFieldSchema(id);
    FieldConfigPtr prefixFieldConfig = fieldSchema->GetFieldConfig(pkeyName);
    if (!prefixFieldConfig) {
        INDEXLIB_FATAL_ERROR(Schema, "kkv field %s not exist in field schema", pkeyName.c_str());
    }
    kkvIndexConfig->SetPrefixFieldConfig(prefixFieldConfig);
    FieldConfigPtr suffixFieldConfig = fieldSchema->GetFieldConfig(skeyName);
    if (!suffixFieldConfig) {
        INDEXLIB_FATAL_ERROR(Schema, "kkv field %s not exist in field schema", skeyName.c_str());
    }
    kkvIndexConfig->SetSuffixFieldConfig(suffixFieldConfig);
    if (ttl != INVALID_TTL) {
        kkvIndexConfig->SetTTL(ttl);
    }
    KKVIndexFieldInfo pkeyFieldInfo;
    pkeyFieldInfo.fieldName = pkeyName;
    pkeyFieldInfo.keyType = KKVKeyType::PREFIX;
    kkvIndexConfig->SetPrefixFieldInfo(pkeyFieldInfo);
    KKVIndexFieldInfo skeyFieldInfo;
    skeyFieldInfo.fieldName = skeyName;
    skeyFieldInfo.keyType = KKVKeyType::SUFFIX;
    uint32_t skipListThreshold =
        EnvUtil::getEnv("indexlib_kkv_default_skiplist_threshold", indexlibv2::index::KKV_DEFAULT_SKIPLIST_THRESHOLD);
    skeyFieldInfo.skipListThreshold = skipListThreshold;
    kkvIndexConfig->SetSuffixFieldInfo(skeyFieldInfo);
    schema->AddIndexConfig(kkvIndexConfig, id);
    kkvIndexConfig->SetRegionInfo(DEFAULT_REGIONID, 1);

    auto preference = kkvIndexConfig->GetIndexPreference();
    auto valueParam = preference.GetValueParam();
    if (valueFormat == "plain") {
        valueParam.EnablePlainFormat(true);
    } else if (valueFormat == "impact") {
        valueParam.EnableValueImpact(true);
    } else {
        valueParam.EnablePlainFormat(false);
        valueParam.EnableValueImpact(false);
    }
    preference.SetValueParam(valueParam);
    kkvIndexConfig->SetIndexPreference(preference);

    schema->LoadValueConfig(id);
}

void SchemaMaker::MakeKVIndex(IndexPartitionSchemaPtr& schema, const string& keyName, regionid_t id, int64_t ttl,
                              const std::string& valueFormat, bool useNumberHash)
{
    KVIndexConfigPtr kvIndexConfig(new KVIndexConfig(keyName, it_kv));
    FieldSchemaPtr fieldSchema = schema->GetFieldSchema(id);
    FieldConfigPtr fieldConfig = fieldSchema->GetFieldConfig(keyName);
    if (!fieldConfig) {
        INDEXLIB_FATAL_ERROR(Schema, "kv field %s not exist in field schema", keyName.c_str());
    }
    kvIndexConfig->SetFieldConfig(fieldConfig);
    if (ttl != INVALID_TTL) {
        kvIndexConfig->SetTTL(ttl);
    }
    schema->AddIndexConfig(kvIndexConfig, id);
    kvIndexConfig->SetRegionInfo(DEFAULT_REGIONID, 1);
    kvIndexConfig->SetUseNumberHash(useNumberHash);
    auto preference = kvIndexConfig->GetIndexPreference();
    auto valueParam = preference.GetValueParam();
    if (valueFormat == "plain") {
        valueParam.EnablePlainFormat(true);
    } else if (valueFormat == "impact") {
        valueParam.EnableValueImpact(true);
    } else if (valueFormat == "autoInline") {
        valueParam.EnableFixLenAutoInline();
    } else {
        valueParam.EnablePlainFormat(false);
        valueParam.EnableValueImpact(false);
    }
    preference.SetValueParam(valueParam);
    kvIndexConfig->SetIndexPreference(preference);

    schema->LoadValueConfig(id);
}

void SchemaMaker::EnableNullFields(const IndexPartitionSchemaPtr& schema, const string& fieldInfos)
{
    FieldSchemaPtr fieldSchema = schema->GetFieldSchema();
    StringVector fieldInfoString = SplitToStringVector(fieldInfos);
    for (size_t i = 0; i < fieldInfoString.size(); i++) {
        StringTokenizer st(fieldInfoString[i], ":", StringTokenizer::TOKEN_IGNORE_EMPTY | StringTokenizer::TOKEN_TRIM);
        assert(st.getNumTokens() > 0 && st.getNumTokens() <= 3);
        FieldConfigPtr fieldConfig = fieldSchema->GetFieldConfig(st[0]);
        assert(fieldConfig);
        fieldConfig->SetEnableNullField(true);
        if (st.getNumTokens() > 1) {
            fieldConfig->SetNullFieldLiteralString(st[1]);
        }

        if (st.getNumTokens() > 2) {
            fieldConfig->SetUserDefineAttributeNullValue(st[2]);
        }
        fieldConfig->Check();
    }
}

// fileCompressInfoStr : compressorName:compresorType:[HOT#compressorName|WARM#compressorName];....
// indexCompressorStr : indexName:compressorName;indexName1:compressorName1
// attributeCompressorStr : attrName:compressorName;attrName1:compressorName1
config::IndexPartitionSchemaPtr
SchemaMaker::EnableFileCompressSchema(const config::IndexPartitionSchemaPtr& schema, const string& fileCompressInfoStr,
                                      const string& indexCompressorStr, const string& attrCompressorStr,
                                      const string& summaryCompressStr, const string& sourceCompressStr)
{
    auto CreateFileCompressConfig = [&](string fileCompressType) -> std::shared_ptr<FileCompressConfig> {
        const string REPLACE_TYPE = "REPLACED_TYPE";
        const string REPLACE_NO = "REPLACED_NO";
        string content = R"({
            "name" : "REPLACED_NO",
            "type" : "REPLACED_TYPE")";
        string tempStr = R"("TEMP_KEY" : "TEMP_VALUE")";
        vector<string> compressorInfoVec;
        StringUtil::fromString(fileCompressType, compressorInfoVec, ":");
        assert(compressorInfoVec.size() >= 2);
        StringUtil::replaceAll(content, REPLACE_NO, compressorInfoVec[0]);
        StringUtil::replaceAll(content, REPLACE_TYPE, compressorInfoVec[1] == "null" ? "" : compressorInfoVec[1]);

        if (compressorInfoVec.size() == 2) {
            content += "}";
        } else {
            assert(compressorInfoVec.size() == 3);
            content += ",\n";
            content += R"("temperature_compressor" : {)";
            vector<vector<string>> kvpairs;
            StringUtil::fromString(compressorInfoVec[2], kvpairs, "#", "|");
            for (size_t i = 0; i < kvpairs.size(); i++) {
                assert(kvpairs[i].size() == 2);
                string kvStr = tempStr;
                StringUtil::replaceAll(kvStr, "TEMP_KEY", kvpairs[i][0]);
                StringUtil::replaceAll(kvStr, "TEMP_VALUE", kvpairs[i][1] == "null" ? "" : kvpairs[i][1]);
                content += kvStr;
                if (i != (kvpairs.size() - 1)) {
                    content += ",";
                }
            }
            content += "}\n}";
        }
        std::shared_ptr<FileCompressConfig> fileCompressConfig(new FileCompressConfig);
        FromJsonString(*fileCompressConfig, content);
        return fileCompressConfig;
    };

    std::shared_ptr<FileCompressSchema> fileCompressSchema(new FileCompressSchema);
    StringVector infoString = SplitToStringVector(fileCompressInfoStr);
    for (auto& info : infoString) {
        std::shared_ptr<FileCompressConfig> fileCompressConfig = CreateFileCompressConfig(info);
        fileCompressSchema->AddFileCompressConfig(fileCompressConfig);
    }
    schema->TEST_SetFileCompressSchema(fileCompressSchema);

    StringVector indexString = SplitToStringVector(indexCompressorStr);
    for (auto& info : indexString) {
        vector<string> tempVec;
        StringUtil::fromString(info, tempVec, ":");
        std::shared_ptr<FileCompressConfig> compressConfig = fileCompressSchema->GetFileCompressConfig(tempVec[1]);
        assert(compressConfig);
        IndexConfigPtr indexConfig = schema->GetIndexSchema()->GetIndexConfig(tempVec[0]);
        assert(indexConfig);
        indexConfig->SetFileCompressConfig(compressConfig);
    }

    StringVector attrString = SplitToStringVector(attrCompressorStr);
    for (auto& info : attrString) {
        vector<string> tempVec;
        StringUtil::fromString(info, tempVec, ":");
        std::shared_ptr<FileCompressConfig> compressConfig = fileCompressSchema->GetFileCompressConfig(tempVec[1]);
        assert(compressConfig);
        AttributeConfigPtr attrConfig = schema->GetAttributeSchema()->GetAttributeConfig(tempVec[0]);
        assert(attrConfig);
        attrConfig->SetFileCompressConfig(compressConfig);
    }

    StringVector summaryString = SplitToStringVector(summaryCompressStr);
    for (auto& info : summaryString) {
        vector<string> tempVec;
        StringUtil::fromString(info, tempVec, ":");
        std::shared_ptr<FileCompressConfig> compressConfig = fileCompressSchema->GetFileCompressConfig(tempVec[1]);
        assert(compressConfig);
        SummaryGroupConfigPtr summaryGroupConfig =
            schema->GetSummarySchema()->GetSummaryGroupConfig(StringUtil::fromString<int32_t>(tempVec[0]));
        auto param = summaryGroupConfig->GetSummaryGroupDataParam();
        param.SetFileCompressConfig(compressConfig);
        summaryGroupConfig->SetSummaryGroupDataParam(param);
    }

    StringVector sourceString = SplitToStringVector(sourceCompressStr);
    for (auto& info : sourceString) {
        vector<string> tempVec;
        StringUtil::fromString(info, tempVec, ":");
        std::shared_ptr<FileCompressConfig> compressConfig = fileCompressSchema->GetFileCompressConfig(tempVec[1]);
        assert(compressConfig);
        SourceGroupConfigPtr sourceGroupConfig =
            schema->GetSourceSchema()->GetGroupConfig(StringUtil::fromString<int32_t>(tempVec[0]));
        auto param = sourceGroupConfig->GetParameter();
        param.SetFileCompressConfig(compressConfig);
        sourceGroupConfig->SetParameter(param);
    }
    return config::IndexPartitionSchemaPtr(schema->Clone());
}

// temperatureConfigPtr: HOT#[conditionStr&conditionStr]|WARM#[conditionStr;...]
// conditionStr: key1=value1;key2=value2
void SchemaMaker::EnableTemperatureLayer(const IndexPartitionSchemaPtr& schema, const string& temperatureConfigStr,
                                         const string& defaultTemperatureLayer)
{
    auto CreateTemperatureLayerConfig = [](string temperatureConfigStr,
                                           string defaultTemperatureLayer) -> TemperatureLayerConfigPtr {
        string content = R"({
           "default_property" : "REPLACED_DEFAULT_PROPERTY",
           "conditions" : [
               REPLACED_CONDITION_STRING
           ]
        })";

        string conditionTemplateStr = R"({
            "property" : "REPLACED_PROPERTY",
            "filters" : [
                REPLACED_PARAM_STRING
            ]
        })";

        vector<string> temperatureConditionStrVec;
        vector<vector<string>> temperatureLayerStrVec;
        StringUtil::fromString(temperatureConfigStr, temperatureLayerStrVec, "#", "|");
        for (size_t i = 0; i < temperatureLayerStrVec.size(); i++) {
            assert(temperatureLayerStrVec[i].size() == 2);
            string singleConditionStr = conditionTemplateStr;
            StringUtil::replaceAll(singleConditionStr, "REPLACED_PROPERTY", temperatureLayerStrVec[i][0]);

            vector<string> tmpStrs;
            StringUtil::fromString(temperatureLayerStrVec[i][1], tmpStrs, "&");
            for (size_t j = 0; j < tmpStrs.size(); j++) {
                vector<vector<string>> kvPairs;
                StringUtil::fromString(tmpStrs[j], kvPairs, "=", ";");
                vector<string> retVec;
                for (auto kv : kvPairs) {
                    assert(kv.size() == 2);
                    string kvTempStr = R"("_KEY_":"_VALUE_")";
                    StringUtil::replaceAll(kvTempStr, "_KEY_", kv[0]);
                    StringUtil::replaceAll(kvTempStr, "_VALUE_", kv[1]);
                    retVec.push_back(kvTempStr);
                }

                string tmpStr = "{";
                tmpStr += StringUtil::toString(retVec, ",");
                tmpStr += "}";
                tmpStrs[j] = tmpStr;
            }
            StringUtil::replaceAll(singleConditionStr, "REPLACED_PARAM_STRING", StringUtil::toString(tmpStrs, ","));
            temperatureConditionStrVec.push_back(singleConditionStr);
        }

        StringUtil::replaceAll(content, "REPLACED_DEFAULT_PROPERTY", defaultTemperatureLayer);
        StringUtil::replaceAll(content, "REPLACED_CONDITION_STRING",
                               StringUtil::toString(temperatureConditionStrVec, ","));

        cout << "############" << endl;
        cout << content << endl;
        TemperatureLayerConfigPtr config(new TemperatureLayerConfig);
        FromJsonString(*config, content);
        return config;
    };

    TemperatureLayerConfigPtr config = CreateTemperatureLayerConfig(temperatureConfigStr, defaultTemperatureLayer);
    RegionSchemaPtr regionSchema = schema->GetRegionSchema(schema->GetDefaultRegionId());
    regionSchema->TEST_SetTemperatureLayerConfig(config);
}

}} // namespace indexlib::test
