#include "indexlib/config/impl/dictionary_schema_impl.h"
#include "indexlib/misc/exception.h"
#include "indexlib/config/configurator_define.h"
#include "indexlib/config/config_define.h"

using namespace std;
using namespace autil::legacy;
IE_NAMESPACE_USE(misc);

IE_NAMESPACE_BEGIN(config);
IE_LOG_SETUP(config, DictionarySchemaImpl);

DictionarySchemaImpl::DictionarySchemaImpl() 
{
}

DictionarySchemaImpl::~DictionarySchemaImpl() 
{
}

void DictionarySchemaImpl::AddDictionaryConfig(const DictionaryConfigPtr& dictConfig)
{
    DictionarySchema::Iterator it = mDictConfigs.find(dictConfig->GetDictName());
    if (it != mDictConfigs.end())
    {
        stringstream ss;
        ss << "Duplicated dictionary name:" << dictConfig->GetDictName();
        INDEXLIB_FATAL_ERROR(Schema, "%s", ss.str().c_str());
    }

    mDictConfigs.insert(make_pair(dictConfig->GetDictName(), dictConfig));
}

DictionaryConfigPtr DictionarySchemaImpl::GetDictionaryConfig(
        const string& dictName) const
{
    DictionarySchema::Iterator it = mDictConfigs.find(dictName);
    if (it != mDictConfigs.end())
    {
        return it->second;
    }
    return DictionaryConfigPtr();
}

void DictionarySchemaImpl::Jsonize(Jsonizable::JsonWrapper& json)
{
    if (json.GetMode() == TO_JSON) 
    {
        DictionarySchema::Iterator it = mDictConfigs.begin();
        vector<Any> anyVec;
        for (; it != mDictConfigs.end(); ++it)
        {
            anyVec.push_back(ToJson(*(it->second)));
        }

        json.Jsonize(DICTIONARIES, anyVec);
    }
}

void DictionarySchemaImpl::AssertEqual(const DictionarySchemaImpl& other) const
{
    IE_CONFIG_ASSERT_EQUAL(mDictConfigs.size(), other.mDictConfigs.size(),
                           "mDictConfigs' size not equal");

    DictionarySchema::Iterator it = mDictConfigs.begin();
    for (; it != mDictConfigs.end(); ++it)
    {
        std::map<std::string, DictionaryConfigPtr>::const_iterator iter = 
            other.mDictConfigs.find(it->first);
        DictionaryConfigPtr dictConfig;
        if (iter != other.mDictConfigs.end())
        {
            dictConfig = iter->second;
        }

        IE_CONFIG_ASSERT(dictConfig, "dictionary configs do not match");
        it->second->AssertEqual(*dictConfig);
    }
}

void DictionarySchemaImpl::AssertCompatible(const DictionarySchemaImpl& other) const
{
    IE_CONFIG_ASSERT(mDictConfigs.size() <= other.mDictConfigs.size(),
                     "dictionary schema is not compatible");

    DictionarySchema::Iterator it = mDictConfigs.begin();
    for (; it != mDictConfigs.end(); ++it)
    {
        std::map<std::string, DictionaryConfigPtr>::const_iterator iter = 
            other.mDictConfigs.find(it->first);
        DictionaryConfigPtr dictConfig;
        if (iter != other.mDictConfigs.end())
        {
            dictConfig = iter->second;
        }
        IE_CONFIG_ASSERT(dictConfig, "dictionary configs do not match");
        it->second->AssertEqual(*dictConfig);
    }
}

IE_NAMESPACE_END(config);

