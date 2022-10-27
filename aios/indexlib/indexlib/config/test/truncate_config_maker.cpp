#include "indexlib/config/test/truncate_config_maker.h"
#include <autil/StringUtil.h>
#include "indexlib/config/configurator_define.h"
#include "indexlib/config/sort_param.h"
#include "indexlib/config/merge_config.h"

using namespace autil;
using namespace std;

IE_NAMESPACE_BEGIN(config);
IE_LOG_SETUP(config, TruncateConfigMaker);

TruncateConfigMaker::TruncateConfigMaker() 
{
}

TruncateConfigMaker::~TruncateConfigMaker() 
{
}

void TruncateConfigMaker::ParseCommon(const string& commonStr, 
                                      TruncateStrategyPtr &strategy)
{
    vector<string> strVec;
    StringUtil::fromString(commonStr, strVec, ":");
    if (2 == strVec.size())
    {
        strategy->SetThreshold(StringUtil::fromString<uint64_t>(strVec[0]));
        strategy->SetLimit(StringUtil::fromString<uint64_t>(strVec[1]));
    }
}

void TruncateConfigMaker::ParseDistinct(const string& str, 
                                        DiversityConstrain &constrain)
{
    vector<string> strVec;
    StringUtil::fromString(str, strVec, ":");
    if (strVec.size() == 3)
    {
        constrain.SetDistField(strVec[0]);
        constrain.SetDistCount(StringUtil::fromString<uint64_t>(strVec[1]));
        constrain.SetDistExpandLimit(
                StringUtil::fromString<uint64_t>(strVec[2]));
    }
}

void TruncateConfigMaker::ParseIndexConfigs(
        const string& str, 
        TruncateOptionConfigPtr &truncOptionConfig, 
        TruncateStrategyPtr &truncateStrategy, 
        DiversityConstrain &diverConstrain,
        const string& strategyType)
{
    vector<string> truncIndexConfigStrVec;
    StringUtil::fromString(str, truncIndexConfigStrVec, ";");
    assert(truncIndexConfigStrVec.size() > 0);

    for (size_t i = 0; i < truncIndexConfigStrVec.size(); i++)
    {
        vector<string> indexConfigStrVec = 
            StringUtil::split(truncIndexConfigStrVec[i], "=");
        assert(indexConfigStrVec.size() == 2);
        vector<string> indexPropertiesStrVec;
        StringUtil::fromString(indexConfigStrVec[1], 
                               indexPropertiesStrVec, ",");
        assert(indexPropertiesStrVec.size() > 0);
        for (size_t j = 0; j < indexPropertiesStrVec.size(); j++)
        {
            vector<string> indexPropertyStrVec;
            indexPropertyStrVec = 
                StringUtil::split(indexPropertiesStrVec[j], ":", false);
            assert(indexPropertyStrVec.size() == 5);

            const string& profileName = indexPropertyStrVec[0];
            //assert(!truncOptionConfig->GetTruncateProfile(profileName));
            TruncateProfilePtr truncateProfile(new TruncateProfile);

            truncateProfile->mIndexNames.push_back(indexConfigStrVec[0]);
            truncateProfile->mTruncateProfileName = profileName;
            if (!indexPropertyStrVec[1].empty())
            {
                SortParam sortParam;
                vector<string> sortParamVec;
                sortParamVec = StringUtil::split(indexPropertyStrVec[1], "|");
                for(size_t p = 0; p < sortParamVec.size(); ++p)
                {
                    vector<string> sortItems;
                    sortItems = StringUtil::split(sortParamVec[p], "#"); 
                    assert(sortItems.size() == 2);
                    sortParam.SetSortField(sortItems[0]);
                    sortParam.SetSortPatternString(sortItems[1]);
                    truncateProfile->mSortParams.push_back(sortParam);
                }
            }
            diverConstrain.SetFilterField(indexPropertyStrVec[2]);
            diverConstrain.SetFilterMinValue(
                    StringUtil::strToUInt64WithDefault(
                            indexPropertyStrVec[3].c_str(), LONG_MIN));
            diverConstrain.SetFilterMaxValue(
                    StringUtil::strToUInt64WithDefault(
                            indexPropertyStrVec[4].c_str(), LONG_MAX));
            if (strategyType == TRUNCATE_META_STRATEGY_TYPE)
            {
                diverConstrain.SetFilterByMeta(true);
            }
            truncateStrategy->SetDiversityConstrain(diverConstrain);
            std::string strategyName = "strategy_" 
                                       + StringUtil::toString<int32_t>(i) 
                                       +  "_"
                                       + StringUtil::toString<int32_t>(j);
            truncateStrategy->SetStrategyName(strategyName);
            truncateStrategy->SetStrategyType(strategyType);
            truncateProfile->mTruncateStrategyName = strategyName;
            truncOptionConfig->AddTruncateProfile(truncateProfile);
            truncOptionConfig->AddTruncateStrategy(truncateStrategy);
            truncateStrategy.reset(new TruncateStrategy(*truncateStrategy));
        }
    }
}

TruncateOptionConfigPtr TruncateConfigMaker::MakeConfig(
        const string& truncCommonStr,
        const string& distinctStr,
        const string& truncIndexConfigStr,
        const string& strategyType)
{
    TruncateOptionConfigPtr truncOptionConfig(new TruncateOptionConfig);
    TruncateStrategyPtr truncateStrategy(new TruncateStrategy);
    DiversityConstrain diverConstrain;

    ParseCommon(truncCommonStr, truncateStrategy);
    ParseDistinct(distinctStr, diverConstrain);
    ParseIndexConfigs(truncIndexConfigStr, truncOptionConfig, 
                      truncateStrategy, diverConstrain, strategyType);
    truncOptionConfig->Init(IndexPartitionSchemaPtr());
    truncOptionConfig->Check();
    return truncOptionConfig;
}

TruncateOptionConfigPtr TruncateConfigMaker::MakeConfig(
        const TruncateParams& params)
{
    return MakeConfig(params.truncCommonStr, params.distinctStr, 
                      params.truncIndexConfigStr, params.strategyType);
}

TruncateOptionConfigPtr TruncateConfigMaker::MakeConfigFromJson(
    const string& jsonStr)
{
    MergeConfig mergeConfig;
    FromJsonString(mergeConfig, jsonStr);
    return mergeConfig.truncateOptionConfig;
}

void TruncateConfigMaker::RewriteSchema(
    const IndexPartitionSchemaPtr& schema, const TruncateParams& params)
{
    TruncateOptionConfigPtr truncOptionConfig = MakeConfig(params);
    const IndexSchemaPtr& indexSchema = schema->GetIndexSchema();
    const TruncateIndexConfigMap& truncIndexConfigs =
        truncOptionConfig->GetTruncateIndexConfigs();

    TruncateProfileSchemaPtr truncateProfileSchema(new TruncateProfileSchema);

    TruncateIndexConfigMap::const_iterator iter;
    for (iter = truncIndexConfigs.begin();
         iter != truncIndexConfigs.end(); ++iter)
    {
        const TruncateIndexConfig& truncIndexConfig = iter->second;
        const string& indexName = truncIndexConfig.GetIndexName();
        const IndexConfigPtr& parentIndexConfig =
            indexSchema->GetIndexConfig(indexName);
        if (!parentIndexConfig)
        {
            INDEXLIB_FATAL_ERROR(Schema,
                                 "indexName[%s] is not exist, check your config!",
                                 indexName.c_str());
        }

        const TruncateIndexPropertyVector& truncIndexProperties =
            truncIndexConfig.GetTruncateIndexProperties();
        parentIndexConfig->SetHasTruncateFlag(true);
        
        string profileNames = "";
        for (size_t j = 0; j < truncIndexProperties.size(); j++)
        {
            const TruncateIndexProperty& property = truncIndexProperties[j];
            if (indexSchema->GetIndexConfig(property.mTruncateIndexName))
            {
                continue;
            }

            const string& profileName = property.mTruncateProfile->mTruncateProfileName;
            if (profileName.empty())
            {
                profileNames = profileName;
            }
            else
            {
                profileNames = profileNames + ";" + profileName;
            }

            IndexConfigPtr indexConfig(parentIndexConfig->Clone());
            indexConfig->SetIndexName(property.mTruncateIndexName);
            indexConfig->SetVirtual(true);
            indexConfig->SetNonTruncateIndexName(indexName);
            indexSchema->AddIndexConfig(indexConfig);

            SortParams sortParams = property.mTruncateProfile->mSortParams;
            TruncateProfileConfigPtr truncProfileConfig(
                new TruncateProfileConfig(profileName, SortParamsToString(sortParams)));

            truncateProfileSchema->AddTruncateProfileConfig(truncProfileConfig);
        }
        parentIndexConfig->SetUseTruncateProfiles(profileNames);
    }
    schema->GetRegionSchema(0)->SetTruncateProfileSchema(truncateProfileSchema);
}





IE_NAMESPACE_END(config);

