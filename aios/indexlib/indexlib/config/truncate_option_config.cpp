#include "indexlib/config/truncate_option_config.h"
#include "indexlib/misc/exception.h"
#include "indexlib/config/sort_pattern_transformer.h"
#include "indexlib/config/configurator_define.h"
#include "indexlib/config/index_config.h"
using namespace std;

IE_NAMESPACE_USE(misc);

IE_NAMESPACE_BEGIN(config);
IE_LOG_SETUP(config, TruncateOptionConfig);

TruncateOptionConfig::TruncateOptionConfig(
    const vector<TruncateStrategy>& truncateStrategyVec)
    : mOriTruncateStrategyVec(truncateStrategyVec)
{
}

TruncateOptionConfig::~TruncateOptionConfig() 
{
}

const TruncateProfilePtr& TruncateOptionConfig::GetTruncateProfile(
        const std::string& profileName) const
{
    for (size_t i = 0; i < mTruncateProfileVec.size(); ++i)
    {
        if (mTruncateProfileVec[i]->mTruncateProfileName == profileName)
        {
            return mTruncateProfileVec[i];
        }
    }
    return mDefaultTruncateProfilePtr;
}

const TruncateStrategyPtr& TruncateOptionConfig::GetTruncateStrategy(
        const std::string& strategyName) const
{
    for (size_t i = 0; i < mTruncateStrategyVec.size(); ++i)
    {
        if (mTruncateStrategyVec[i]->GetStrategyName() == strategyName)
        {
            return mTruncateStrategyVec[i];
        }
    }
    return mDefaultTruncateStrategyPtr;
}

void TruncateOptionConfig::InitProfileToStrategyNameMap(
    ProfileToStrategyNameMap &profileToStrategyMap)
{
    profileToStrategyMap.clear();
    for (size_t i = 0; i < mOriTruncateStrategyVec.size(); ++i)
    {
	const vector<string>& profiles = mOriTruncateStrategyVec[i].GetProfileNames();
	const string& strategyName = mOriTruncateStrategyVec[i].GetStrategyName();
	for (size_t j = 0; j < profiles.size(); ++j)
	{
	    const string& profileName = profiles[j];
	    if (profileToStrategyMap.find(profileName) !=
		profileToStrategyMap.end())
	    {
		INDEXLIB_FATAL_ERROR(
		    BadParameter,
		    "profile name [%s] is duplicate in truncate_strategy[%s, %s]",
		    profileName.c_str(),
		    profileToStrategyMap[profileName].c_str(),
		    strategyName.c_str());
	    }
	    profileToStrategyMap[profileName] = strategyName;
	}
    }
}

void TruncateOptionConfig::CompleteTruncateStrategy(
        TruncateProfilePtr &profilePtr, const TruncateStrategy &strategy)
{
    TruncateStrategyPtr strategyPtr(new TruncateStrategy(strategy));
    if (TRUNCATE_META_STRATEGY_TYPE == strategyPtr->GetStrategyType()
        && strategyPtr->GetDiversityConstrain().GetFilterField().empty()
        && profilePtr->mSortParams.size() >= 1)
    {
        // generate new stategy completed with FilterField by SortParams[0]
        const string &firstSortField = profilePtr->mSortParams[0].GetSortField();
        strategyPtr->GetDiversityConstrain().SetFilterField(firstSortField);
        strategyPtr->SetStrategyName(
                firstSortField + "_" + strategyPtr->GetStrategyName());
        profilePtr->mTruncateStrategyName = strategyPtr->GetStrategyName();
    }
    strategyPtr->GetDiversityConstrain().Init();

    // skip duplicate strategy
    for (size_t i = 0; i < mTruncateStrategyVec.size(); ++i)
    {
        if (mTruncateStrategyVec[i]->GetStrategyName()
                == strategyPtr->GetStrategyName())
        {
            return;
        }
    }
    mTruncateStrategyVec.push_back(strategyPtr);
}

void TruncateOptionConfig::CompleteTruncateConfig(
    ProfileToStrategyNameMap &profileToStrategyMap,
    const IndexPartitionSchemaPtr& schema)
{
    const TruncateProfileSchemaPtr& truncateProfileSchema 
            = schema->GetTruncateProfileSchema();
    for (TruncateProfileSchema::Iterator iter = truncateProfileSchema->Begin();
            iter != truncateProfileSchema->End(); ++iter) 
    {
        TruncateProfilePtr profilePtr(new TruncateProfile(*(iter->second)));
        
        const IndexSchemaPtr& indexSchema = schema->GetIndexSchema();
        // complete IndexNames
        auto indexConfigs = indexSchema->CreateIterator(false);
        for (auto indexIt = indexConfigs->Begin(); indexIt != indexConfigs->End(); indexIt++)
        {
            if((*indexIt)->HasTruncate()
               && (*indexIt)->HasTruncateProfile(profilePtr->mTruncateProfileName))
            {
                profilePtr->mIndexNames.push_back((*indexIt)->GetIndexName());
            }    
        }
        // complete with ProfileToStrategyNameMap
        ProfileToStrategyNameMap::const_iterator it = 
            profileToStrategyMap.find(profilePtr->mTruncateProfileName);
        if (it != profileToStrategyMap.end())
        {
            profilePtr->mTruncateStrategyName = it->second;
        }
        // complete TruncateStrategy
        for (size_t i = 0; i < mOriTruncateStrategyVec.size(); ++i)
        {
            if (profilePtr->mTruncateStrategyName ==
                    mOriTruncateStrategyVec[i].GetStrategyName())
            {
                CompleteTruncateStrategy(profilePtr, mOriTruncateStrategyVec[i]);
            }
        }
        mTruncateProfileVec.push_back(profilePtr);
        IE_LOG(INFO, "Generated a truncate profile[%s] with strategy[%s], has %lu indexes", 
                    profilePtr->mTruncateProfileName.c_str(),
                    profilePtr->mTruncateStrategyName.c_str(),
                    profilePtr->mIndexNames.size());
    }

}

void TruncateOptionConfig::Init(const IndexPartitionSchemaPtr& schema)
{
    if (mTruncateIndexConfig.size() > 0)
    {
        // already completed
        return;
    }

    if (schema && schema->GetTruncateProfileSchema())
    {
        IE_LOG(INFO, "Combine truncate config with schema");
	ProfileToStrategyNameMap profileToStrategyMap;
	InitProfileToStrategyNameMap(profileToStrategyMap);
        CompleteTruncateConfig(profileToStrategyMap, schema);
    }
    CreateTruncateIndexConfigs();
    Check();
}

void TruncateOptionConfig::Check() const
{
    for (size_t i = 0; i < mTruncateProfileVec.size(); ++i)
    {
        mTruncateProfileVec[i]->Check();
        const std::string& strategyName = 
            mTruncateProfileVec[i]->mTruncateStrategyName;
        bool valid = false;
        for (size_t j = 0; j < mTruncateStrategyVec.size(); ++j)
        {
            if (mTruncateStrategyVec[j]->GetStrategyName() == strategyName)
            {
                valid = true;
                break;
            }
        }
        IE_CONFIG_ASSERT_PARAMETER_VALID(valid, 
                "no truncate strategy for name");
    }
    for (size_t i = 0; i < mTruncateStrategyVec.size(); ++i)
    {
        mTruncateStrategyVec[i]->Check();
    }
}

bool TruncateOptionConfig::operator==(const TruncateOptionConfig& other) const
{
    if (mOriTruncateStrategyVec.size() != other.mOriTruncateStrategyVec.size())
    {
        return false;
    }

    for (size_t i = 0; i < mOriTruncateStrategyVec.size(); ++i)
    {
        if (!(mOriTruncateStrategyVec[i] == other.mOriTruncateStrategyVec[i]))
        {
            return false;
        }
    }
    return true;
}

bool TruncateOptionConfig::IsTruncateIndex(const std::string indexName) const
{
    return (mTruncateIndexConfig.find(indexName) != 
            mTruncateIndexConfig.end());
}

void TruncateOptionConfig::CreateTruncateIndexConfigs()
{
    for (size_t i = 0; i < mTruncateProfileVec.size(); ++i)
    {
        const TruncateProfilePtr& truncateProfile = mTruncateProfileVec[i];
        TruncateIndexProperty property;

        property.mTruncateProfile = truncateProfile;
        property.mTruncateStrategy = 
            GetTruncateStrategy(truncateProfile->mTruncateStrategyName);
        if (!property.mTruncateStrategy)
        {
            IE_CONFIG_ASSERT_PARAMETER_VALID(false, "invalid strategy name");
        }

        for (size_t j = 0; j < truncateProfile->mIndexNames.size(); ++j)
        {
            const std::string& indexName = truncateProfile->mIndexNames[j];
            property.mTruncateIndexName = IndexConfig::CreateTruncateIndexName(
                indexName, truncateProfile->mTruncateProfileName);
            TruncateIndexConfigMap::iterator iter = 
                mTruncateIndexConfig.find(indexName);
            if (iter != mTruncateIndexConfig.end())
            {
                iter->second.AddTruncateIndexProperty(property);
            }
            else
            {
                mTruncateIndexConfig[indexName].SetIndexName(indexName);
                mTruncateIndexConfig[indexName].AddTruncateIndexProperty(property);
            }
        }
    }
}

void TruncateOptionConfig::AddTruncateProfile(const TruncateProfilePtr& truncateProfile)
{
    mTruncateProfileVec.push_back(truncateProfile);
}

void TruncateOptionConfig::AddTruncateStrategy(
        const TruncateStrategyPtr& truncateStategy)
{
    mTruncateStrategyVec.push_back(truncateStategy);
}

const TruncateIndexConfig& TruncateOptionConfig::GetTruncateIndexConfig(
        const std::string& indexName) const
{
    TruncateIndexConfigMap::const_iterator iter = 
        mTruncateIndexConfig.find(indexName);
    if (mTruncateIndexConfig.end() == iter)
    {
        IE_CONFIG_ASSERT_PARAMETER_VALID(false, "invalid index name");    
    }
    return iter->second;
}

void TruncateOptionConfig::UpdateTruncateIndexConfig(
        int64_t beginTime, int64_t baseTime)
{
    for (size_t i = 0; i < mTruncateStrategyVec.size(); ++i)
    {
        mTruncateStrategyVec[i]->GetDiversityConstrain()
            .ParseTimeStampFilter(beginTime, baseTime);
    }
}

IE_NAMESPACE_END(config);

