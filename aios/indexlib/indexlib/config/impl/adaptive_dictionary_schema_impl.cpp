#include "indexlib/config/impl/adaptive_dictionary_schema_impl.h"
#include "indexlib/misc/exception.h"
#include "indexlib/config/configurator_define.h"
#include "indexlib/config/config_define.h"

using namespace std;
using namespace autil::legacy;

IE_NAMESPACE_USE(misc);
IE_NAMESPACE_BEGIN(config);
IE_LOG_SETUP(config, AdaptiveDictionarySchemaImpl);

AdaptiveDictionarySchemaImpl::AdaptiveDictionarySchemaImpl() 
{
}

AdaptiveDictionarySchemaImpl::~AdaptiveDictionarySchemaImpl() 
{
}

AdaptiveDictionaryConfigPtr AdaptiveDictionarySchemaImpl::GetAdaptiveDictionaryConfig(
        const string& ruleName) const
{
    Iterator it = mDictRules.find(ruleName);
    if (it == mDictRules.end())
    {
        return AdaptiveDictionaryConfigPtr();
    }
    
    size_t idx = it->second;
    assert(idx < mAdaptiveDictConfigs.size());
    return mAdaptiveDictConfigs[idx];
}

void AdaptiveDictionarySchemaImpl::Jsonize(Jsonizable::JsonWrapper& json)
{
    json.Jsonize(ADAPTIVE_DICTIONARIES, mAdaptiveDictConfigs, 
                 mAdaptiveDictConfigs);
    if (json.GetMode() == Jsonizable::FROM_JSON)
    {
        mDictRules.clear();
        for (size_t i = 0; i < mAdaptiveDictConfigs.size(); ++i)
        {
            AdaptiveDictionaryConfigPtr dictConf = mAdaptiveDictConfigs[i];
            string ruleName = dictConf->GetRuleName();
            Iterator it = mDictRules.find(ruleName);
            if (it != mDictRules.end())
            {
                stringstream ss;
                ss << "Duplicated adaptive dictionary name:" << ruleName;
                INDEXLIB_FATAL_ERROR(Schema, "%s", ss.str().c_str());
            }
            mDictRules.insert(make_pair(ruleName, i));
        }
    }
}

void AdaptiveDictionarySchemaImpl::AssertEqual(const AdaptiveDictionarySchemaImpl& other) const
{
    IE_CONFIG_ASSERT_EQUAL(mDictRules.size(), other.mDictRules.size(),
                           "mDictRules' size not equal");

    IE_CONFIG_ASSERT_EQUAL(mAdaptiveDictConfigs.size(), other.mAdaptiveDictConfigs.size(),
                           "mAdaptiveDictConfigs' size not equal");

    Iterator it = mDictRules.begin();
    for (; it != mDictRules.end(); ++it)
    {
        AdaptiveDictionaryConfigPtr dictConfig = 
            other.GetAdaptiveDictionaryConfig(it->first);
        IE_CONFIG_ASSERT(dictConfig, "adaptive dictionary configs do not match");
        mAdaptiveDictConfigs[it->second]->AssertEqual(*dictConfig);
    }

}

void AdaptiveDictionarySchemaImpl::AssertCompatible(const AdaptiveDictionarySchemaImpl& other) const
{
    IE_CONFIG_ASSERT(mDictRules.size() <= other.mDictRules.size(),
                     "adaptive dictionary schema is not compatible");

    IE_CONFIG_ASSERT(mAdaptiveDictConfigs.size() <= other.mAdaptiveDictConfigs.size(),
                     "adaptive dictionary schema is not compatible");

    Iterator it = mDictRules.begin();
    for (; it != mDictRules.end(); ++it)
    {
        AdaptiveDictionaryConfigPtr dictConfig = 
            other.GetAdaptiveDictionaryConfig(it->first);
        IE_CONFIG_ASSERT(dictConfig, "adaptive dictionary configs do not match");
        mAdaptiveDictConfigs[it->second]->AssertEqual(*dictConfig);
    }
}

void AdaptiveDictionarySchemaImpl::AddAdaptiveDictionaryConfig(
        const AdaptiveDictionaryConfigPtr& config)
{
    string ruleName = config->GetRuleName();
    Iterator it = mDictRules.find(ruleName);
    if (it != mDictRules.end())
    {
        stringstream ss;
        ss << "Duplicated adaptive dictionary name:" << ruleName;
        INDEXLIB_FATAL_ERROR(Schema, "%s", ss.str().c_str());
    }

    size_t currentDictSize = mAdaptiveDictConfigs.size();
    mDictRules.insert(make_pair(ruleName, currentDictSize));
    mAdaptiveDictConfigs.push_back(config);
}

IE_NAMESPACE_END(config);

