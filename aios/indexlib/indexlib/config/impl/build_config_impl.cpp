#include <autil/legacy/any.h>
#include <autil/legacy/json.h>
#include <autil/StringUtil.h>
#include "indexlib/config/impl/build_config_impl.h"
#include "indexlib/config/configurator_define.h"
#include "indexlib/misc/exception.h"

using namespace std;
using namespace autil::legacy;
using namespace autil::legacy::json;
using namespace autil;

IE_NAMESPACE_BEGIN(config);
IE_LOG_SETUP(config, BuildConfigImpl);

BuildConfigImpl::BuildConfigImpl()
    : raidConfig(new storage::RaidConfig())
{}

BuildConfigImpl::BuildConfigImpl(const BuildConfigImpl& other)
    : segAttrUpdaterConfig(other.segAttrUpdaterConfig)
    , raidConfig(other.raidConfig)
    , customizedParams(other.customizedParams)
{
}

BuildConfigImpl::~BuildConfigImpl() {}

void BuildConfigImpl::Jsonize(Jsonizable::JsonWrapper& json)
{
    json.Jsonize("segment_customize_metrics_updater", segAttrUpdaterConfig, segAttrUpdaterConfig);
    json.Jsonize("raid_config", *raidConfig, storage::RaidConfig());
    
    if (json.GetMode() == TO_JSON)
    {
        json.Jsonize(CUSTOMIZED_PARAMETERS, customizedParams);
    }
    else
    {
        map<string ,Any> data = json.GetMap();
        auto iter = data.find(CUSTOMIZED_PARAMETERS);
        if (iter != data.end())
        {
            map<string, Any> paramMap = AnyCast<JsonMap>(iter->second);
            map<string, Any>::iterator paramIter = paramMap.begin();
            for (; paramIter != paramMap.end(); paramIter++)
            {
                string value = AnyCast<string>(paramIter->second);
                customizedParams.insert(
                    make_pair(paramIter->first, value));
            }
        }
    }
}

const map<string, string>& BuildConfigImpl::GetCustomizedParams() const
{
    return customizedParams;
}

bool BuildConfigImpl::SetCustomizedParams(
    const std::string& key, const std::string& value)
{
    return customizedParams.insert(make_pair(key, value)).second;
}

void BuildConfigImpl::Check() const
{
}

bool BuildConfigImpl::operator == (const BuildConfigImpl& other) const
{
    return segAttrUpdaterConfig == other.segAttrUpdaterConfig
        && *raidConfig == *other.raidConfig
        && customizedParams == other.customizedParams;
}

void BuildConfigImpl::operator = (const BuildConfigImpl& other)
{
    segAttrUpdaterConfig = other.segAttrUpdaterConfig;
    raidConfig = other.raidConfig;
    customizedParams = other.customizedParams;
}

IE_NAMESPACE_END(config);

