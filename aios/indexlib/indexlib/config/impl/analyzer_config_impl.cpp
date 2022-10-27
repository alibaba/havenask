#include "indexlib/config/impl/analyzer_config_impl.h"
#include "autil/legacy/any.h"
#include "autil/legacy/jsonizable.h"
#include "autil/legacy/json.h"
#include "indexlib/config/configurator_define.h"
#include "indexlib/config/config_define.h"
#include "indexlib/config/schema_configurator.h"

using namespace std;
using namespace autil::legacy;
using namespace autil::legacy::json;

IE_NAMESPACE_BEGIN(config);
IE_LOG_SETUP(config, AnalyzerConfigImpl);

AnalyzerConfigImpl::AnalyzerConfigImpl(const string& name) 
    : mAnalyzerName(name)
{
}

AnalyzerConfigImpl::~AnalyzerConfigImpl() 
{
}

string AnalyzerConfigImpl::GetTokenizerConfig(const string& key) const
{
    map<string, string>::const_iterator it = mTokenizerConfigs.find(key);
    if (it != mTokenizerConfigs.end())
    {
        return it->second;
    }
    return "";
}

void AnalyzerConfigImpl::SetNormalizeOption(const string& key, const string& value)
{
    mNormalizeOptions[key] = value;
}

string AnalyzerConfigImpl::GetNormalizeOption(const string& key) const
{
    map<string, string>::const_iterator it = mNormalizeOptions.find(key);
    if (it != mNormalizeOptions.end())
    {
        return it->second;
    }
    return "";
}

void AnalyzerConfigImpl::SetTokenizerConfig(const string& key, const string& value)
{
    mTokenizerConfigs[key] = value;
}

void AnalyzerConfigImpl::AddStopWord(const std::string& word)
{
    mStopWords.push_back(word);
}

void AnalyzerConfigImpl::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json)
{
    if (json.GetMode() == TO_JSON) 
    {
        json.Jsonize(ANALYZER_NAME, mAnalyzerName);
        json.Jsonize(TOKENIZER_CONFIGS, mTokenizerConfigs);
        json.Jsonize(STOPWORDS, mStopWords);
        json.Jsonize(NORMALIZE_OPTIONS, mNormalizeOptions);
    }
}

void AnalyzerConfigImpl::AssertEqual(const AnalyzerConfigImpl& other) const
{
    IE_CONFIG_ASSERT_EQUAL(mAnalyzerName, other.mAnalyzerName, "analyzer_name not equal");
    IE_CONFIG_ASSERT_EQUAL(mTokenizerConfigs, other.mTokenizerConfigs, "tokenizer_configs not equal");
    IE_CONFIG_ASSERT_EQUAL(mStopWords, other.mStopWords, "stop words not equal");
    IE_CONFIG_ASSERT_EQUAL(mNormalizeOptions, other.mNormalizeOptions, "normalize_options not equal");
}

IE_NAMESPACE_END(config);

