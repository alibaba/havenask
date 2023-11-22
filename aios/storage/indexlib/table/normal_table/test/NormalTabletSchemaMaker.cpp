#include "indexlib/table/normal_table/test/NormalTabletSchemaMaker.h"

#include "autil/legacy/json.h"
#include "indexlib/config/UnresolvedSchema.h"
#include "indexlib/config/test/FieldConfigMaker.h"
#include "indexlib/index/ann/ANNIndexConfig.h"
#include "indexlib/index/attribute/Common.h"
#include "indexlib/index/attribute/config/AttributeConfig.h"
#include "indexlib/index/common/Constant.h"
#include "indexlib/index/field_meta/Common.h"
#include "indexlib/index/field_meta/config/FieldMetaConfig.h"
#include "indexlib/index/inverted_index/config/DateIndexConfig.h"
#include "indexlib/index/inverted_index/config/PackageIndexConfig.h"
#include "indexlib/index/inverted_index/config/RangeIndexConfig.h"
#include "indexlib/index/inverted_index/config/SpatialIndexConfig.h"
#include "indexlib/index/inverted_index/config/TruncateProfileConfig.h"
#include "indexlib/index/pack_attribute/PackAttributeConfig.h"
#include "indexlib/index/primary_key/config/PrimaryKeyIndexConfig.h"
#include "indexlib/index/source/config/SourceGroupConfig.h"
#include "indexlib/index/source/config/SourceIndexConfig.h"
#include "indexlib/index/summary/Common.h"
#include "indexlib/index/summary/Constant.h"
#include "indexlib/index/summary/config/SummaryConfig.h"
#include "indexlib/index/summary/config/SummaryIndexConfig.h"
#include "indexlib/util/Algorithm.h"

using namespace std;
using namespace indexlibv2::config;

namespace indexlibv2::table {
AUTIL_LOG_SETUP(indexlib.table, NormalTabletSchemaMaker);

std::shared_ptr<TabletSchema>
NormalTabletSchemaMaker::Make(const std::string& fieldNames, const std::string& indexNames,
                              const std::string& attributeNames, const std::string& summaryNames,
                              const std::string& truncateProfiles, const std::string& sourceNames,
                              const std::string& fieldMetaNames)
{
    auto schema = std::make_shared<TabletSchema>();
    auto unresolvedSchema = schema->TEST_GetImpl();
    unresolvedSchema->TEST_SetTableName("noname");
    unresolvedSchema->TEST_SetTableType("normal");
    if (fieldNames.size() > 0 && !MakeFieldConfig(unresolvedSchema, fieldNames).IsOK()) {
        AUTIL_LOG(ERROR, "make field config failed");
        return nullptr;
    }
    if (indexNames.size() > 0 && !MakeIndexConfig(unresolvedSchema, indexNames).IsOK()) {
        AUTIL_LOG(ERROR, "make index config failed");
        return nullptr;
    }

    if (attributeNames.size() > 0 &&
        !MakeAttributeConfig(unresolvedSchema, FieldConfigMaker::SplitToStringVector(attributeNames)).IsOK()) {
        AUTIL_LOG(ERROR, "make attribute config failed");
        return nullptr;
    }
    if (summaryNames.size() > 0 &&
        !MakeSummaryConfig(unresolvedSchema, FieldConfigMaker::SplitToStringVector(summaryNames)).IsOK()) {
        AUTIL_LOG(ERROR, "make summary config failed");
        return nullptr;
    }
    if (sourceNames.size() > 0 && !MakeSourceConfig(unresolvedSchema, sourceNames).IsOK()) {
        AUTIL_LOG(ERROR, "make source config failed");
        return nullptr;
    }

    if (fieldMetaNames.size() > 0 &&
        !MakeFieldMetaConfig(unresolvedSchema, FieldConfigMaker::SplitToStringVector(fieldMetaNames)).IsOK()) {
        AUTIL_LOG(ERROR, "make field meta config failed");
        return nullptr;
    }

    SetNeedStoreSummary(unresolvedSchema);

    if (truncateProfiles.size() > 0 && !MakeTruncateProfiles(unresolvedSchema, truncateProfiles).IsOK()) {
        AUTIL_LOG(ERROR, "make truncate profiles failed");
        return nullptr;
    }

    if (!EnsureSpatialIndexWithAttribute(unresolvedSchema).IsOK()) {
        AUTIL_LOG(ERROR, "add attribute config for spatial index failed");
        return nullptr;
    }
    return schema;
}

Status NormalTabletSchemaMaker::MakeFieldConfig(UnresolvedSchema* schema, const string& fieldNames)
{
    // fieldName:fieldType:isMultiValue:FixedMultiValueCount:EnableNullField:AnalyzerName
    const auto& fieldConfigs = FieldConfigMaker::Make(fieldNames);
    for (const auto& fieldConfig : fieldConfigs) {
        RETURN_IF_STATUS_ERROR(schema->AddFieldConfig(fieldConfig), "add field config [%s] failed",
                               fieldConfig->GetFieldName().c_str());
    }
    return Status::OK();
}

Status NormalTabletSchemaMaker::MakeIndexConfig(UnresolvedSchema* schema, const string& indexNames)
{
    auto fieldNames = FieldConfigMaker::SplitToStringVector(indexNames);
    for (size_t i = 0; i < fieldNames.size(); ++i) {
        autil::StringTokenizer st(fieldNames[i], ":",
                                  autil::StringTokenizer::TOKEN_TRIM | autil::StringTokenizer::TOKEN_IGNORE_EMPTY);
        assert(st.getNumTokens() >= 3);
        string indexName = st[0];
        auto [status, indexType] = InvertedIndexConfig::StrToIndexType(st[1]);
        RETURN_IF_STATUS_ERROR(status, "parse index type [%s] failed", st[1].c_str());
        string fieldName = st[2];
        bool hasTruncate = false;
        if ((st.getNumTokens() >= 4) && (st[3] == string("true"))) {
            hasTruncate = true;
        }
        size_t shardingCount = 0;
        if (st.getNumTokens() >= 5) {
            shardingCount = autil::StringUtil::fromString<size_t>(st[4]);
        }
        assert(shardingCount != 1);

        bool useHashDict = false;
        if (st.getNumTokens() == 6) {
            useHashDict = autil::StringUtil::fromString<bool>(st[5]);
        }

        autil::StringTokenizer st2(fieldName, ",",
                                   autil::StringTokenizer::TOKEN_TRIM | autil::StringTokenizer::TOKEN_IGNORE_EMPTY);

        if (st2.getNumTokens() == 1 && indexType != it_pack && indexType != it_expack && indexType != it_customized) {
            auto indexConfig =
                CreateSingleFieldIndexConfig(indexName, indexType, st2[0], "", false, schema->GetFieldConfig(st2[0]));
            if (!indexConfig) {
                RETURN_IF_STATUS_ERROR(Status::InternalError(), "create single field index config failed, [%s]",
                                       fieldNames[i].c_str());
            }
            indexConfig->SetHasTruncateFlag(hasTruncate);
            if (indexType == it_primarykey64 || indexType == it_primarykey128) {
                auto pkIndexConfig = dynamic_cast<index::PrimaryKeyIndexConfig*>(indexConfig.get());
                assert(pkIndexConfig);
                pkIndexConfig->SetPrimaryKeyAttributeFlag(true);
                pkIndexConfig->SetOptionFlag(of_none);
                if (st.getNumTokens() > 3) {
                    bool numberHash = st[3] == "number_hash";
                    if (numberHash) {
                        pkIndexConfig->SetPrimaryKeyHashType(index::pk_number_hash);
                    }
                }
            }

            if (indexType == it_string || indexType == it_number) {
                indexConfig->SetOptionFlag(of_none | of_term_frequency);
            }

            if (indexType == it_number) {
                auto fieldType = indexConfig->GetFieldConfig()->GetFieldType();
                indexType = InvertedIndexConfig::FieldTypeToInvertedIndexType(fieldType);
                indexConfig->SetInvertedIndexType(indexType);
            }

            if (useHashDict) {
                assert(indexType != it_range && indexType != it_datetime);
                indexConfig->SetHashTypedDictionary(true);
            }

            if (shardingCount >= 2) {
                CreateShardingIndexConfigs(indexConfig, shardingCount);
            }
            indexConfig->SetIndexId(i);
            RETURN_IF_STATUS_ERROR(schema->DoAddIndexConfig(indexConfig->GetIndexType(), indexConfig, true),
                                   "add index config failed");
            RETURN_IF_STATUS_ERROR(
                schema->AddIndexConfig(indexlib::index::GENERAL_INVERTED_INDEX_TYPE_STR, indexConfig),
                "add general_inverted failed");
        } else {
            vector<int32_t> boostVec;
            boostVec.resize(st2.getTokenVector().size(), 0);
            auto indexConfig =
                CreatePackageIndexConfig(indexName, indexType, st2.getTokenVector(), boostVec, "", false, schema);
            indexConfig->SetHasTruncateFlag(hasTruncate);

            if (indexType == it_number) {
                auto fieldConfigs = indexConfig->GetFieldConfigs();
                assert(fieldConfigs.size() == 1);
                indexType = InvertedIndexConfig::FieldTypeToInvertedIndexType(fieldConfigs[0]->GetFieldType());
                indexConfig->SetInvertedIndexType(indexType);
            }
            if (useHashDict) {
                indexConfig->SetHashTypedDictionary(true);
            }
            if (shardingCount >= 2) {
                CreateShardingIndexConfigs(indexConfig, shardingCount);
            }
            indexConfig->SetIndexId(i);
            RETURN_IF_STATUS_ERROR(schema->DoAddIndexConfig(indexConfig->GetIndexType(), indexConfig, true),
                                   "add index config failed");
            RETURN_IF_STATUS_ERROR(
                schema->AddIndexConfig(indexlib::index::GENERAL_INVERTED_INDEX_TYPE_STR, indexConfig),
                "add general_inverted failed");
        }
    }
    return Status::OK();
}

Status NormalTabletSchemaMaker::MakeSourceConfig(UnresolvedSchema* schema, const std::string& sourceStr)
{
    auto groupFields = autil::StringUtil::split(sourceStr, "|");
    auto sourceIndexConfig = make_shared<SourceIndexConfig>();
    std::vector<std::string> indexFieldNames;
    for (size_t i = 0; i < groupFields.size(); ++i) {
        auto sourceGroupConfig = make_shared<indexlib::config::SourceGroupConfig>();
        sourceGroupConfig->SetGroupId(i);
        if (groupFields[i] == "__UDF__") {
            for (auto fieldConfig : schema->GetFieldConfigs()) {
                indexFieldNames.push_back(fieldConfig->GetFieldName());
            }
            sourceGroupConfig->SetFieldMode(indexlib::config::SourceGroupConfig::SourceFieldMode::USER_DEFINE);
        } else if (groupFields[i] == "__ALL__") {
            for (auto fieldConfig : schema->GetFieldConfigs()) {
                indexFieldNames.push_back(fieldConfig->GetFieldName());
            }
            sourceGroupConfig->SetFieldMode(indexlib::config::SourceGroupConfig::SourceFieldMode::ALL_FIELD);
        } else {
            auto fieldNames = FieldConfigMaker::SplitToStringVector(groupFields[i]);
            for (auto& fieldName : fieldNames) {
                assert(schema->GetFieldConfig(fieldName));
                indexFieldNames.push_back(fieldName);
            }
            sourceGroupConfig->SetSpecifiedFields(fieldNames);
            sourceGroupConfig->SetFieldMode(indexlib::config::SourceGroupConfig::SourceFieldMode::SPECIFIED_FIELD);
        }
        sourceIndexConfig->AddGroupConfig(sourceGroupConfig);
    }

    indexlib::util::Algorithm::SortUniqueAndErase(indexFieldNames);
    std::vector<std::shared_ptr<config::FieldConfig>> fieldConfigs;
    for (auto& fieldName : indexFieldNames) {
        fieldConfigs.push_back(schema->GetFieldConfig(fieldName));
    }
    sourceIndexConfig->SetFieldConfigs(fieldConfigs);

    RETURN_IF_STATUS_ERROR(schema->DoAddIndexConfig(sourceIndexConfig->GetIndexType(), sourceIndexConfig, true),
                           "add source index config failed");
    return Status::OK();
}

shared_ptr<SingleFieldIndexConfig>
NormalTabletSchemaMaker::CreateSingleFieldIndexConfig(const string& indexName, InvertedIndexType indexType,
                                                      const string& fieldName, const string& analyzerName,
                                                      bool isVirtual, const shared_ptr<FieldConfig>& fieldConfig)
{
    if (!fieldConfig) {
        stringstream ss;
        ss << "No such field defined:" << fieldName;
        AUTIL_LOG(ERROR, "%s", ss.str().c_str());
        return nullptr;
    }
    FieldType fieldType = fieldConfig->GetFieldType();
    if (fieldType != ft_text && !analyzerName.empty()) {
        stringstream ss;
        ss << "index field :" << fieldName << " can not set index analyzer";
        AUTIL_LOG(ERROR, "%s", ss.str().c_str());
        return nullptr;
    }

    if (fieldConfig->IsBinary()) {
        stringstream ss;
        ss << "index field :" << fieldName << " can not set isBinary = true";
        AUTIL_LOG(ERROR, "%s", ss.str().c_str());
        return nullptr;
    }

    shared_ptr<SingleFieldIndexConfig> indexConfig;
    if (indexType == it_spatial) {
        indexConfig.reset(new SpatialIndexConfig(indexName, indexType));
    } else if (indexType == it_datetime) {
        string defaultGranularity = (fieldType == ft_date) ? "day" : "second";
        indexConfig.reset(new DateIndexConfig(indexName, indexType, defaultGranularity));
    } else if (indexType == it_range) {
        indexConfig.reset(new RangeIndexConfig(indexName, indexType));
    } else if (indexType == it_primarykey64 || indexType == it_primarykey128) {
        indexConfig.reset(new index::PrimaryKeyIndexConfig(indexName, indexType));
    } else if (indexType == it_kv) {
        assert(false);
        // indexConfig.reset(new KVIndexConfig(indexName, indexType));
    } else if (indexType == it_kkv) {
        assert(false);
    } else {
        indexConfig.reset(new SingleFieldIndexConfig(indexName, indexType));
    }

    if (!indexConfig->SetFieldConfig(fieldConfig).IsOK()) {
        AUTIL_LOG(ERROR, "set field config failed");
        return nullptr;
    }
    indexConfig->SetVirtual(isVirtual);
    if (!analyzerName.empty()) {
        indexConfig->SetAnalyzer(analyzerName);
    }
    return indexConfig;
}

void NormalTabletSchemaMaker::CreateShardingIndexConfigs(const std::shared_ptr<InvertedIndexConfig>& indexConfig,
                                                         size_t shardingCount)
{
    const std::string& indexName = indexConfig->GetIndexName();
    indexConfig->SetShardingType(InvertedIndexConfig::IST_NEED_SHARDING);
    for (size_t i = 0; i < shardingCount; ++i) {
        std::shared_ptr<InvertedIndexConfig> shardingIndexConfig(indexConfig->Clone());
        shardingIndexConfig->SetShardingType(InvertedIndexConfig::IST_IS_SHARDING);
        shardingIndexConfig->SetIndexName(InvertedIndexConfig::GetShardingIndexName(indexName, i));
        shardingIndexConfig->SetDictConfig(indexConfig->GetDictConfig());
        shardingIndexConfig->SetAdaptiveDictConfig(indexConfig->GetAdaptiveDictionaryConfig());
        shardingIndexConfig->SetFileCompressConfig(indexConfig->GetFileCompressConfig());
        indexConfig->AppendShardingIndexConfig(shardingIndexConfig);
    }
}

shared_ptr<PackageIndexConfig> NormalTabletSchemaMaker::CreatePackageIndexConfig(
    const string& indexName, InvertedIndexType indexType, const vector<string>& fieldNames,
    const vector<int32_t> boosts, const string& analyzerName, bool isVirtual, const UnresolvedSchema* schema)
{
    shared_ptr<PackageIndexConfig> packageConfig;
    if (indexType == it_customized) {
        packageConfig.reset(new ANNIndexConfig(indexName, indexType));
    } else {
        packageConfig.reset(new PackageIndexConfig(indexName, indexType));
    }
    packageConfig->SetVirtual(isVirtual);
    for (size_t i = 0; i < fieldNames.size(); ++i) {
        auto fieldConfig = schema->GetFieldConfig(fieldNames[i]);
        if (!fieldConfig) {
            AUTIL_LOG(ERROR, "get field config [%s] failed", fieldNames[i].c_str());
            return nullptr;
        }
        if (!packageConfig->AddFieldConfig(fieldConfig, boosts[i]).IsOK()) {
            AUTIL_LOG(ERROR, "add field config failed");
            return nullptr;
        }
    }

    if (!analyzerName.empty()) {
        packageConfig->SetAnalyzer(analyzerName);
    } else {
        if (!packageConfig->SetDefaultAnalyzer().IsOK()) {
            AUTIL_LOG(ERROR, "set default analyzer failed");
            return nullptr;
        }
    }
    return packageConfig;
}

Status NormalTabletSchemaMaker::MakeAttributeConfig(config::UnresolvedSchema* schema,
                                                    const std::vector<std::string>& attributeNames)
{
    attrid_t attrId = 0;
    packattrid_t packAttrId = 0;
    for (size_t i = 0; i < attributeNames.size(); ++i) {
        autil::StringTokenizer st(attributeNames[i], ":", autil::StringTokenizer::TOKEN_TRIM);
        // name:updatable:compressStr:sub1,sub2
        assert(st.getNumTokens() > 0 && st.getNumTokens() <= 4);
        string compressStr = (st.getNumTokens() >= 3) ? st[2] : "";
        bool updatable = (st.getNumTokens() >= 2 && st[1] == "true");
        if (st.getNumTokens() < 4) {
            // AttributeConfig
            auto fieldConfig = schema->GetFieldConfig(st[0]);
            if (!fieldConfig) {
                RETURN_IF_STATUS_ERROR(Status::InternalError(), "get field config [%s] failed", st[0].c_str());
            }
            auto attrConfig = make_shared<index::AttributeConfig>();
            auto status = attrConfig->Init(fieldConfig);
            RETURN_IF_STATUS_ERROR(status, "attr config init failed, field [%s]", st[0].c_str());
            attrConfig->SetAttrId(attrId++);
            if (!compressStr.empty()) {
                RETURN_IF_STATUS_ERROR(attrConfig->SetCompressType(compressStr), "set compress [%s] failed",
                                       compressStr.c_str());
            }
            if (updatable) {
                attrConfig->SetUpdatable(updatable);
            }
            RETURN_IF_STATUS_ERROR(schema->DoAddIndexConfig(attrConfig->GetIndexType(), attrConfig, true),
                                   "add index config failed");
            if (!schema->GetIndexConfig(indexlib::index::GENERAL_VALUE_INDEX_TYPE_STR, attrConfig->GetIndexName())) {
                RETURN_IF_STATUS_ERROR(
                    schema->AddIndexConfig(indexlib::index::GENERAL_VALUE_INDEX_TYPE_STR, attrConfig),
                    "add index config failed");
            }
        } else {
            // PackAttributeConfig
            vector<string> subAttrConfigStrs;
            autil::StringUtil::fromString(st[3], subAttrConfigStrs, ",");
            assert(subAttrConfigStrs.size() >= 1);
            auto packAttrConfig = make_shared<index::PackAttributeConfig>();
            packAttrConfig->SetPackName(st[0]);
            packAttrConfig->SetPackAttrId(packAttrId++);
            if (updatable) {
                packAttrConfig->SetUpdatable(updatable);
            }
            if (!compressStr.empty()) {
                RETURN_IF_STATUS_ERROR(packAttrConfig->SetCompressType(compressStr), "set compress [%s] failed",
                                       compressStr.c_str());
            }
            for (const auto& subAttrConfigStr : subAttrConfigStrs) {
                std::vector<std::string> params;
                autil::StringUtil::fromString(subAttrConfigStr, params, "|");
                assert(params.size() >= 1);
                auto subAttrName = params[0];
                auto fieldConfig = schema->GetFieldConfig(subAttrName);
                if (!fieldConfig) {
                    RETURN_IF_STATUS_ERROR(Status::InternalError(), "pack attribute [%s] get field config [%s] failed",
                                           st[0].c_str(), subAttrName.c_str());
                }
                auto attrConfig = std::make_shared<index::AttributeConfig>();
                if (params.size() >= 2) {
                    RETURN_IF_STATUS_ERROR(attrConfig->SetCompressType(params[1]),
                                           "attr config init compress type [%s] failed, subAttrName [%s]",
                                           params[1].c_str(), subAttrName.c_str());
                }
                RETURN_IF_STATUS_ERROR(attrConfig->Init(fieldConfig), "attr config init failed, field [%s]",
                                       subAttrName.c_str());
                if (!schema->GetIndexConfig(indexlib::index::GENERAL_VALUE_INDEX_TYPE_STR,
                                            attrConfig->GetIndexName())) {
                    RETURN_IF_STATUS_ERROR(
                        schema->AddIndexConfig(indexlib::index::GENERAL_VALUE_INDEX_TYPE_STR, attrConfig),
                        "add index config failed");
                }
                RETURN_IF_STATUS_ERROR(packAttrConfig->AddAttributeConfig(attrConfig),
                                       "pack attribute [%s] add attribute [%s] failed", st[0].c_str(),
                                       subAttrName.c_str());
            }
            RETURN_IF_STATUS_ERROR(schema->DoAddIndexConfig(packAttrConfig->GetIndexType(), packAttrConfig, true),
                                   "add pack attribute [%s] failed", st[0].c_str());
        }
    }
    return Status::OK();
}

Status NormalTabletSchemaMaker::MakeSummaryConfig(config::UnresolvedSchema* schema,
                                                  const std::vector<std::string>& fieldNames)
{
    auto summaryIndexConfig = make_shared<SummaryIndexConfig>();
    for (size_t i = 0; i < fieldNames.size(); ++i) {
        const auto& fieldName = fieldNames[i];
        auto fieldConfig = schema->GetFieldConfig(fieldName);
        if (!fieldConfig) {
            RETURN_IF_STATUS_ERROR(Status::InternalError(), "get field config [%s] failed", fieldName.c_str());
        }
        auto summaryConfig = make_shared<indexlib::config::SummaryConfig>();
        summaryConfig->SetFieldConfig(fieldConfig);
        auto status = summaryIndexConfig->AddSummaryConfig(summaryConfig, indexlib::index::DEFAULT_SUMMARYGROUPID);
        RETURN_IF_STATUS_ERROR(status, "add summary config failed");
        if (fieldConfig->GetFieldType() == ft_timestamp &&
            !schema->GetIndexConfig(index::ATTRIBUTE_INDEX_TYPE_STR, fieldName)) {
            auto attrConfig = make_shared<index::AttributeConfig>();
            auto status = attrConfig->Init(fieldConfig);
            RETURN_IF_STATUS_ERROR(status, "attr config init failed, field [%s]", fieldName.c_str());
            attrConfig->SetAttrId(schema->GetIndexConfigs(attrConfig->GetIndexType()).size());
            RETURN_IF_STATUS_ERROR(schema->AddIndexConfig(attrConfig), "add attribute config [%s] failed",
                                   fieldName.c_str());
        }
    }
    RETURN_IF_STATUS_ERROR(schema->DoAddIndexConfig(summaryIndexConfig->GetIndexType(), summaryIndexConfig, true),
                           "add summary index config failed");
    return Status::OK();
}

void NormalTabletSchemaMaker::SetNeedStoreSummary(config::UnresolvedSchema* schema)
{
    const auto& config =
        schema->GetIndexConfig(indexlib::index::SUMMARY_INDEX_TYPE_STR, indexlib::index::SUMMARY_INDEX_NAME);
    if (!config) {
        return;
    }
    const auto& summaryConfig = std::dynamic_pointer_cast<config::SummaryIndexConfig>(config);
    assert(summaryConfig);
    const auto& attrFields = schema->GetIndexFieldConfigs(indexlib::index::ATTRIBUTE_INDEX_TYPE_STR);
    if (attrFields.empty()) {
        summaryConfig->SetNeedStoreSummary(true);
        return;
    }
    std::set<fieldid_t> attrFieldIds;
    for (const auto& attrField : attrFields) {
        attrFieldIds.insert(attrField->GetFieldId());
    }
    summaryConfig->SetNeedStoreSummary(false);
    const auto& summaryFields = summaryConfig->GetFieldConfigs();
    for (const auto& summaryField : summaryFields) {
        auto fieldId = summaryField->GetFieldId();
        if (attrFieldIds.find(fieldId) == attrFieldIds.end()) {
            summaryConfig->SetNeedStoreSummary(fieldId);
        }
    }
}

Status NormalTabletSchemaMaker::EnsureSpatialIndexWithAttribute(config::UnresolvedSchema* schema)
{
    for (auto indexConfig : schema->GetIndexConfigs(indexlib::index::INVERTED_INDEX_TYPE_STR)) {
        if (auto spatialIndexConfig = std::dynamic_pointer_cast<config::SpatialIndexConfig>(indexConfig)) {
            if (spatialIndexConfig->IsDeleted()) {
                continue;
            }
            auto fieldConfig = spatialIndexConfig->GetFieldConfig();
            if (fieldConfig->GetFieldType() != FieldType::ft_location) {
                continue;
            }
            auto fieldName = fieldConfig->GetFieldName();
            auto attributeConfig = std::dynamic_pointer_cast<index::AttributeConfig>(
                schema->GetIndexConfig(index::ATTRIBUTE_INDEX_TYPE_STR, fieldName));
            if (!attributeConfig) {
                AUTIL_LOG(INFO, "add attribute [%s] to ensure spatial index precision", fieldName.c_str());
                auto attributeConfig = std::make_shared<index::AttributeConfig>();
                auto status = attributeConfig->Init(fieldConfig);
                RETURN_IF_STATUS_ERROR(status, "attr config init failed, field [%s]", fieldName.c_str());
                attributeConfig->SetFileCompressConfigV2(spatialIndexConfig->GetFileCompressConfigV2());
                attributeConfig->SetAttrId(schema->GetIndexConfigs(attributeConfig->GetIndexType()).size());
                RETURN_IF_STATUS_ERROR(schema->AddIndexConfig(attributeConfig), "add spatial attribute [%s] failed",
                                       fieldName.c_str());
            }
        }
    }
    return Status::OK();
}

Status NormalTabletSchemaMaker::MakeFieldMetaConfig(config::UnresolvedSchema* schema,
                                                    const std::vector<std::string>& fieldNames)
{
    for (size_t i = 0; i < fieldNames.size(); ++i) {
        autil::StringTokenizer st(fieldNames[i], ":",
                                  autil::StringTokenizer::TOKEN_TRIM | autil::StringTokenizer::TOKEN_IGNORE_EMPTY);
        assert(st.getNumTokens() >= 2);
        const auto& fieldName = st[0];
        auto fieldConfig = schema->GetFieldConfig(fieldName);
        if (!fieldConfig) {
            RETURN_IF_STATUS_ERROR(Status::InternalError(), "get field config [%s] failed", fieldName.c_str());
        }
        auto fieldMetaConfig = make_shared<indexlib::index::FieldMetaConfig>();
        RETURN_IF_STATUS_ERROR(fieldMetaConfig->Init(fieldConfig, fieldName), "field meta config init failed");
        auto& fieldMetaInfos = fieldMetaConfig->TEST_GetFieldMetaInfos();
        fieldMetaInfos.clear();
        for (size_t i = 1; i < st.getNumTokens(); ++i) {
            indexlib::index::FieldMetaConfig::FieldMetaInfo info;
            info.metaName = st[i];
            fieldMetaInfos.push_back(info);
        }
        if (fieldMetaConfig->NeedFieldSource()) {
            fieldMetaConfig->TEST_SetStoreMetaSourceType(indexlib::index::FieldMetaConfig::MetaSourceType::MST_FIELD);
        } else {
            fieldMetaConfig->TEST_SetStoreMetaSourceType(
                indexlib::index::FieldMetaConfig::MetaSourceType::MST_TOKEN_COUNT);
        }

        auto status = schema->DoAddIndexConfig(fieldMetaConfig->GetIndexType(), fieldMetaConfig, true);
        if (!status.IsOK()) {
            return status;
        }
    }
    return Status::OK();
}

// truncateName0:sortDescription0#truncateName1:sortDescription1...
Status NormalTabletSchemaMaker::MakeTruncateProfiles(config::UnresolvedSchema* schema,
                                                     const std::string& truncateProfileStr)
{
    vector<vector<string>> truncateProfileVec;
    autil::StringUtil::fromString(truncateProfileStr, truncateProfileVec, ":", "#");
    vector<shared_ptr<TruncateProfileConfig>> truncateConfigs;
    for (size_t i = 0; i < truncateProfileVec.size(); ++i) {
        string profileName = truncateProfileVec[i][0];
        string sortDesp = (truncateProfileVec[i].size() > 1) ? truncateProfileVec[i][1] : "";
        auto truncateProfile = make_shared<TruncateProfileConfig>();
        truncateProfile->TEST_SetTruncateProfileName(profileName);
        truncateProfile->TEST_SetSortParams(sortDesp);
        truncateConfigs.push_back(truncateProfile);
    }

    if (!schema->TEST_SetSetting("truncate_profiles", truncateConfigs, false)) {
        RETURN_IF_STATUS_ERROR(Status::InternalError(), "set truncate_profiles failed");
    }
    return Status::OK();
}

} // namespace indexlibv2::table
