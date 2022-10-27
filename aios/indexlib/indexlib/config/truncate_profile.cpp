#include "indexlib/config/truncate_profile.h"
#include "indexlib/config/configurator_define.h"

using namespace std;

IE_NAMESPACE_BEGIN(config);
IE_LOG_SETUP(config, TruncateProfile);

TruncateProfile::TruncateProfile() 
{
}

TruncateProfile::TruncateProfile(TruncateProfileConfig& truncateProfileConfig)
{
    mTruncateProfileName = truncateProfileConfig.GetTruncateProfileName();
    mSortParams = truncateProfileConfig.GetTruncateSortParams();
}

TruncateProfile::~TruncateProfile() 
{
}

void TruncateProfile::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json)
{
    json.Jsonize("TruncateProfileName", mTruncateProfileName,
                 mTruncateProfileName);
    json.Jsonize("IndexNames", mIndexNames, mIndexNames);
    json.Jsonize("TruncateStrategyName", mTruncateStrategyName, 
                 mTruncateStrategyName);

    if (json.GetMode() == TO_JSON)
    {
        if (mSortParams.size() > 1)
        {
            json.Jsonize("SortParams", mSortParams, mSortParams);
        }
        else if (mSortParams.size() == 1)
        {
            std::string strSort = mSortParams[0].GetSortField();
            json.Jsonize("SortField", strSort, strSort);
            strSort = mSortParams[0].GetSortPatternString();
            json.Jsonize("SortType", strSort, strSort);
        }
    }
    else
    {
        string sortFieldName;
        string sortPattern = DESC_SORT_PATTERN;
        json.Jsonize("SortField", sortFieldName, sortFieldName);
        json.Jsonize("SortType", sortPattern, sortPattern);
        
        if (!sortFieldName.empty())
        {
            SortParam tmpSortParam;
            tmpSortParam.SetSortField(sortFieldName);
            tmpSortParam.SetSortPatternString(sortPattern);
            mSortParams.push_back(tmpSortParam);
        }
        else
        {
            SortParams tmpSortParams;
            json.Jsonize("SortParams", tmpSortParams, tmpSortParams);
            for(size_t i = 0; i < tmpSortParams.size(); ++i)
            {
                if (tmpSortParams[i].GetSortField().empty())
                {
                    continue;
                }
                mSortParams.push_back(tmpSortParams[i]);
            }
        }
    }
}

void TruncateProfile::Check() const
{
    bool valid  = !mTruncateProfileName.empty();
    IE_CONFIG_ASSERT_PARAMETER_VALID(valid, "truncate profile name is invalid");

    for (size_t i = 0; i < mSortParams.size(); ++i)
    {
        if (!mSortParams[i].GetSortField().empty())
        {
            std::string sortPattern = mSortParams[i].GetSortPatternString();        
            valid = sortPattern == DESC_SORT_PATTERN 
                    || sortPattern == ASC_SORT_PATTERN;
            IE_CONFIG_ASSERT_PARAMETER_VALID(valid, "sort pattern is invalid");
        }
    }
}

void TruncateProfile::AppendIndexName(const std::string& indexName)
{
    for (size_t i = 0; i < mIndexNames.size(); ++i)
    {
        if (mIndexNames[i] == indexName)
        {
            return;
        }
    }
    mIndexNames.push_back(indexName);
}

bool TruncateProfile::operator==(const TruncateProfile& other) const
{
    if (mIndexNames.size() != other.mIndexNames.size())
    {
        return false;
    }

    if (mSortParams.size() != other.mSortParams.size())
    {
        return false;
    }

    for (size_t i = 0; i < mIndexNames.size(); ++i)
    {
        if (mIndexNames[i] != other.mIndexNames[i])
        {
            return false;
        }
    }

    for (size_t i = 0; i < mSortParams.size(); ++i)
    {
        if (mSortParams[i].GetSortField() != other.mSortParams[i].GetSortField())
        {
            return false;
        }

        if (mSortParams[i].GetSortPattern() != other.mSortParams[i].GetSortPattern())
        {
            return false;
        }
    }

    return mTruncateProfileName == other.mTruncateProfileName
        && mTruncateStrategyName == other.mTruncateStrategyName;
}

IE_NAMESPACE_END(config);

