#include <autil/StringTokenizer.h>
#include "indexlib/config/impl/dictionary_config_impl.h"
#include "indexlib/config/configurator_define.h"
#include "indexlib/config/config_define.h"

using namespace std;
using namespace autil;
using namespace autil::legacy;

IE_NAMESPACE_BEGIN(config);
IE_LOG_SETUP(config, DictionaryConfigImpl);

DictionaryConfigImpl::DictionaryConfigImpl(const string& dictName, 
                                   const string& content)
    : mDictName(dictName)
    , mContent(content)
{
}

DictionaryConfigImpl::~DictionaryConfigImpl() 
{
}

void DictionaryConfigImpl::Jsonize(Jsonizable::JsonWrapper& json)
{
    if (json.GetMode() == Jsonizable::TO_JSON)
    {
        json.Jsonize(DICTIONARY_NAME, mDictName);
        json.Jsonize(DICTIONARY_CONTENT, mContent);
    }
}

void DictionaryConfigImpl::AssertEqual(const DictionaryConfigImpl& other) const
{
    IE_CONFIG_ASSERT_EQUAL(mDictName, other.GetDictName(),
                           "Dictionary name doesn't match");

    IE_CONFIG_ASSERT_EQUAL(mContent, other.GetContent(),
                           "Dictionary content doesn't match");
}

IE_NAMESPACE_END(config);

