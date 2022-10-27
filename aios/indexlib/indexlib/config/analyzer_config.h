#ifndef __INDEXLIB_ANALYZER_CONFIG_H
#define __INDEXLIB_ANALYZER_CONFIG_H

#include <tr1/memory>
#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"
#include "autil/legacy/jsonizable.h"

DECLARE_REFERENCE_CLASS(config, AnalyzerConfigImpl);

IE_NAMESPACE_BEGIN(config);

class AnalyzerConfig : public autil::legacy::Jsonizable
{
public:
    AnalyzerConfig(const std::string& name);
    ~AnalyzerConfig();

public:
    const std::string& GetAnalyzerName() const;

    std::string GetTokenizerConfig(const std::string& key) const;
    void SetTokenizerConfig(const std::string& key, const std::string& value);

    std::string GetNormalizeOption(const std::string& key) const;
    void SetNormalizeOption(const std::string& key, const std::string& value);

    void AddStopWord(const std::string& word);
    const std::vector<std::string>& GetStopWords() const;

    virtual void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json);
    virtual void AssertEqual(const AnalyzerConfig& other) const;

protected:
    AnalyzerConfigImplPtr mImpl;
    
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(AnalyzerConfig);

IE_NAMESPACE_END(config);

#endif //__INDEXLIB_ANALYZER_CONFIG_H
