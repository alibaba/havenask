#ifndef __INDEXLIB_ANALYZER_CONFIG_IMPL_H
#define __INDEXLIB_ANALYZER_CONFIG_IMPL_H

#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"

#include <tr1/memory>
#include "indexlib/misc/exception.h"
#include "autil/legacy/jsonizable.h"

IE_NAMESPACE_BEGIN(config);

class AnalyzerConfigImpl : public autil::legacy::Jsonizable
{
public:
    AnalyzerConfigImpl(const std::string& name);
    ~AnalyzerConfigImpl();

public:
    const std::string& GetAnalyzerName() const
    { return mAnalyzerName; }

    std::string GetTokenizerConfig(const std::string& key) const;
    void SetTokenizerConfig(const std::string& key, const std::string& value);

    std::string GetNormalizeOption(const std::string& key) const;
    void SetNormalizeOption(const std::string& key, const std::string& value);

    void AddStopWord(const std::string& word);
    const std::vector<std::string>& GetStopWords() const
    { return mStopWords; }

    virtual void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json);
    virtual void AssertEqual(const AnalyzerConfigImpl& other) const;

protected:
    std::string mAnalyzerName;
    std::map<std::string, std::string> mTokenizerConfigs;
    std::vector<std::string> mStopWords;
    std::map<std::string, std::string> mNormalizeOptions;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(AnalyzerConfigImpl);

IE_NAMESPACE_END(config);

#endif //__INDEXLIB_ANALYZER_CONFIG_IMPL_H
