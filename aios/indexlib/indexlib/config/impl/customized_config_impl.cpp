#include <autil/legacy/any.h>
#include <autil/legacy/json.h>
#include "indexlib/config/configurator_define.h"
#include "indexlib/config/config_define.h"
#include "indexlib/misc/exception.h"
#include "indexlib/config/impl/customized_config_impl.h"

using namespace std;
using namespace autil;
using namespace autil::legacy;
using namespace autil::legacy::json;
IE_NAMESPACE_USE(misc);

IE_NAMESPACE_BEGIN(config);
IE_LOG_SETUP(config, CustomizedConfigImpl);

CustomizedConfigImpl::CustomizedConfigImpl() 
{
}

CustomizedConfigImpl::CustomizedConfigImpl(const CustomizedConfigImpl& other)
    : mId(other.mId)
    , mPluginName(other.mPluginName)
    , mParameters(other.mParameters)
{
}

CustomizedConfigImpl::~CustomizedConfigImpl() 
{
}

void CustomizedConfigImpl::Jsonize(legacy::Jsonizable::JsonWrapper& json)
{
    if (json.GetMode() == TO_JSON)
    {
        json.Jsonize(CUSTOMIZED_IDENTIFIER, mId);
        json.Jsonize(CUSTOMIZED_PLUGIN_NAME, mPluginName);
        json.Jsonize(CUSTOMIZED_PARAMETERS, mParameters);
    }
    else
    {
        map<string, Any> data = json.GetMap();
        auto iter = data.find(CUSTOMIZED_IDENTIFIER);
        if (iter == data.end())
        {
            INDEXLIB_FATAL_ERROR(Schema, "id is defined");
        }
        mId = AnyCast<string>(iter->second);
        if (mId.empty())
        {
            INDEXLIB_FATAL_ERROR(Schema, "id can not be empty");
        }
        iter = data.find(CUSTOMIZED_PLUGIN_NAME);
        if (iter == data.end())
        {
            INDEXLIB_FATAL_ERROR(Schema, "no plugin name is defined");
        }
        mPluginName = AnyCast<string>(iter->second);
        if (mPluginName.empty())
        {
            INDEXLIB_FATAL_ERROR(Schema, "plugin name can not be empty");
        }

        iter = data.find(CUSTOMIZED_PARAMETERS);
        if (iter != data.end()) {
            map<string, Any> paramMap = AnyCast<JsonMap>(iter->second);
            map<string, Any>::iterator paramIter = paramMap.begin();
            for (; paramIter != paramMap.end(); paramIter++)
            {
                string value = AnyCast<string>(paramIter->second);
                mParameters.insert(
                    make_pair(paramIter->first, value));
            }
        }
    }
}

void CustomizedConfigImpl::AssertEqual(const CustomizedConfigImpl& other) const
{
    IE_CONFIG_ASSERT_EQUAL(mId, other.mId, "mId not equal");
    IE_CONFIG_ASSERT_EQUAL(mPluginName, other.mPluginName, "mPluginName not equal");
    IE_CONFIG_ASSERT_EQUAL(mParameters, other.mParameters, "mParameters not equal");
}

const bool CustomizedConfigImpl::GetParameters(const string& key,
                                                    string& value) const
{
    auto iter = mParameters.find(key);
    if (iter == mParameters.end())
    {
        return false;
    }
    value = iter->second;
    return true;
}

void CustomizedConfigImpl::SetId(const string& id)
{
    mId = id;
}

void CustomizedConfigImpl::SetPluginName(const string& pluginName)
{
    mPluginName = pluginName;
}

IE_NAMESPACE_END(config);

