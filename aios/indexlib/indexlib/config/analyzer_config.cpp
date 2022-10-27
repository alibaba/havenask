#include "indexlib/config/analyzer_config.h"
#include "indexlib/config/impl/analyzer_config_impl.h"

using namespace std;
using namespace autil::legacy;
using namespace autil::legacy::json;

IE_NAMESPACE_BEGIN(config);
IE_LOG_SETUP(config, AnalyzerConfig);

AnalyzerConfig::AnalyzerConfig(const string& name) 
    : mImpl(new AnalyzerConfigImpl(name))
{
}

AnalyzerConfig::~AnalyzerConfig() 
{
}

string AnalyzerConfig::GetTokenizerConfig(const string& key) const
{
    return mImpl->GetTokenizerConfig(key);
}

void AnalyzerConfig::SetNormalizeOption(const string& key, const string& value)
{
    mImpl->SetNormalizeOption(key, value);
}

string AnalyzerConfig::GetNormalizeOption(const string& key) const
{
    return mImpl->GetNormalizeOption(key);
}

void AnalyzerConfig::SetTokenizerConfig(const string& key, const string& value)
{
    mImpl->SetTokenizerConfig(key, value);
}

void AnalyzerConfig::AddStopWord(const std::string& word)
{
    mImpl->AddStopWord(word);
}

void AnalyzerConfig::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json)
{
    mImpl->Jsonize(json);
}

void AnalyzerConfig::AssertEqual(const AnalyzerConfig& other) const
{
    mImpl->AssertEqual(*(other.mImpl.get()));
}

const string& AnalyzerConfig::GetAnalyzerName() const
{
    return mImpl->GetAnalyzerName();
}

const vector<string>& AnalyzerConfig::GetStopWords() const
{
    return mImpl->GetStopWords();
}

IE_NAMESPACE_END(config);

