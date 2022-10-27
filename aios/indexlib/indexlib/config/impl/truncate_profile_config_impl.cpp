#include "indexlib/config/impl/truncate_profile_config_impl.h"
#include "indexlib/config/configurator_define.h"
#include "indexlib/config/config_define.h"
#include <autil/StringTokenizer.h>

using namespace std;
using namespace autil;
using namespace autil::legacy;

IE_NAMESPACE_BEGIN(config);
IE_LOG_SETUP(config, TruncateProfileConfigImpl);

TruncateProfileConfigImpl::TruncateProfileConfigImpl()
{
}

// for test
TruncateProfileConfigImpl::TruncateProfileConfigImpl(
    const std::string& profileName, const std::string& sortDesp)
    : mTruncateProfileName(profileName)
    , mOriSortDescriptions(sortDesp)
{
    mSortParams = StringToSortParams(sortDesp);
}

TruncateProfileConfigImpl::~TruncateProfileConfigImpl()
{
}

void TruncateProfileConfigImpl::Jsonize(Jsonizable::JsonWrapper& json)
{
    json.Jsonize(TRUNCATE_PROFILE_NAME, mTruncateProfileName, mTruncateProfileName);
    if (json.GetMode() == TO_JSON)
    {
        if (!mSortParams.empty())
        {
            string sortDespStr = SortParamsToString(mSortParams);
            json.Jsonize(TRUNCATE_SORT_DESCRIPTIONS, sortDespStr);
        }
    }
    else
    {
        json.Jsonize(TRUNCATE_SORT_DESCRIPTIONS, mOriSortDescriptions, mOriSortDescriptions);
        mSortParams = StringToSortParams(mOriSortDescriptions);
    }
}

void TruncateProfileConfigImpl::Check() const
{
    if (mTruncateProfileName.empty())
    {
        INDEXLIB_FATAL_ERROR(Schema, "mTruncateProfileName is empty");
    }
}

void TruncateProfileConfigImpl::AssertEqual(const TruncateProfileConfigImpl& other) const
{
    IE_CONFIG_ASSERT_EQUAL(mTruncateProfileName, other.mTruncateProfileName,
                           "mTruncateProfileName doesn't match");

    IE_CONFIG_ASSERT_EQUAL(mSortParams.size(), other.mSortParams.size(),
            "mSortParams's size not equal");
    for (size_t i = 0; i < mSortParams.size(); ++i)
    {
        mSortParams[i].AssertEqual(other.mSortParams[i]);
    }
}

IE_NAMESPACE_END(config);

