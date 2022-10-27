#ifndef ISEARCH_BS_ANALYZER_CONFIG_H
#define ISEARCH_BS_ANALYZER_CONFIG_H


#include "build_service/util/Log.h"
#include <tr1/memory>
#include <autil/legacy/jsonizable.h>
#include "build_service/analyzer/AnalyzerDefine.h"

namespace build_service {
namespace analyzer {

struct NormalizeOptions : public autil::legacy::Jsonizable
{
    NormalizeOptions(bool f1 = true, bool f2 = true, bool f3 = true)
        : caseSensitive(f1)
        , traditionalSensitive(f2)
        , widthSensitive(f3)
    {
    }

    bool caseSensitive;
    bool traditionalSensitive;
    bool widthSensitive;
    std::string tableName;

public:
    virtual void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) {
        json.Jsonize(CASE_SENSITIVE, caseSensitive, true);
        json.Jsonize(TRADITIONAL_SENSITIVE, traditionalSensitive, true);
        json.Jsonize(WIDTH_SENSITIVE, widthSensitive, true);
        json.Jsonize(TRADITIONAL_TABLE_NAME,tableName, tableName);
    }
};

class AnalyzerInfo : public autil::legacy::Jsonizable
{
public:
    std::string getTokenizerConfig(const std::string& key) const;
    const std::map<std::string, std::string>& getTokenizerConfigs() const;
    void setTokenizerConfig(const std::string& key, const std::string& value);
    const std::string& getTokenizerName() const;
    void setTokenizerName(const std::string &tokenizerName);
    const NormalizeOptions& getNormalizeOptions() const { return _normalizeOptions; }
    void setNormalizeOptions(const NormalizeOptions &options) { _normalizeOptions = options; }

    void addStopWord(const std::string& word);
    const std::set<std::string>& getStopWords() const
    { return _stopwords; }
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json);

    //only for make Compatible config
    void constructTokenizerName(std::string &tokenizerName) const;
private:
    std::map<std::string, std::string> _tokenizerConfigs;
    std::string _tokenizerName;
    std::set<std::string> _stopwords;
    NormalizeOptions _normalizeOptions;
private:
    BS_LOG_DECLARE();
};

typedef std::tr1::shared_ptr<AnalyzerInfo> AnalyzerInfoPtr;

}
}

#endif //ISEARCH_BS_ANALYZER_CONFIG_H
