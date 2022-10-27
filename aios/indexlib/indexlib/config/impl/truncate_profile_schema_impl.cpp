#include "indexlib/config/impl/truncate_profile_schema_impl.h"
#include "indexlib/misc/exception.h"
#include "indexlib/config/configurator_define.h"
#include "indexlib/config/config_define.h"

using namespace std;
using namespace autil::legacy;
IE_NAMESPACE_USE(misc);

IE_NAMESPACE_BEGIN(config);
IE_LOG_SETUP(config, TruncateProfileSchemaImpl);

TruncateProfileSchemaImpl::TruncateProfileSchemaImpl()
{
}

TruncateProfileSchemaImpl::~TruncateProfileSchemaImpl()
{
}

TruncateProfileConfigPtr TruncateProfileSchemaImpl::GetTruncateProfileConfig(
    const string& truncateProfileName) const
{
    Iterator it = mTruncateProfileConfigs.find(truncateProfileName);
    if (it != mTruncateProfileConfigs.end())
    {
        return it->second;
    }
    return TruncateProfileConfigPtr();
}

void TruncateProfileSchemaImpl::Jsonize(Jsonizable::JsonWrapper& json)
{
    if (json.GetMode() == TO_JSON)
    {
        Iterator it = mTruncateProfileConfigs.begin();
        vector<Any> anyVec;
        for (; it != mTruncateProfileConfigs.end(); ++it)
        {
            anyVec.push_back(ToJson(*(it->second)));
        }
        json.Jsonize(TRUNCATE_PROFILES, anyVec);
    }
    else
    {
        std::vector<TruncateProfileConfig> truncateProfileConfigs;
        json.Jsonize(TRUNCATE_PROFILES, truncateProfileConfigs, truncateProfileConfigs);
        std::vector<TruncateProfileConfig>::const_iterator it =
            truncateProfileConfigs.begin();
        for (; it != truncateProfileConfigs.end(); ++it)
        {
            mTruncateProfileConfigs[it->GetTruncateProfileName()] =
                TruncateProfileConfigPtr(new TruncateProfileConfig(*it));
        }
    }
}

void TruncateProfileSchemaImpl::AssertEqual(const TruncateProfileSchemaImpl& other) const
{
    IE_CONFIG_ASSERT_EQUAL(mTruncateProfileConfigs.size(), other.mTruncateProfileConfigs.size(),
                           "mTruncateProfileConfigs' size not equal");

    Iterator it = mTruncateProfileConfigs.begin();
    for (; it != mTruncateProfileConfigs.end(); ++it)
    {
        it->second->AssertEqual(*(other.GetTruncateProfileConfig(it->first)));
    }
}

void TruncateProfileSchemaImpl::AddTruncateProfileConfig(
    const TruncateProfileConfigPtr& truncateProfileConfig)
{
    Iterator it = mTruncateProfileConfigs.find(
	truncateProfileConfig->GetTruncateProfileName());
    if (it != mTruncateProfileConfigs.end())
    {
        // already exist
        return;
    }
    mTruncateProfileConfigs[truncateProfileConfig->GetTruncateProfileName()]
	= truncateProfileConfig;
}

IE_NAMESPACE_END(config);

