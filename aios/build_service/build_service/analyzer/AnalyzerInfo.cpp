#include "build_service/analyzer/AnalyzerInfo.h"
#include <autil/legacy/any.h>
#include <autil/legacy/jsonizable.h>
#include <autil/legacy/json.h>
#include "build_service/util/Log.h"

using namespace std;
using namespace autil::legacy;

namespace build_service {
namespace analyzer {
 BS_LOG_SETUP(analyzer, AnalyzerInfo);

string AnalyzerInfo::getTokenizerConfig(const string& key) const
{
    map<string, string>::const_iterator it = _tokenizerConfigs.find(key);
    if (it != _tokenizerConfigs.end())
    {
        return it->second;
    }
    return "";
}
void AnalyzerInfo::setTokenizerConfig(const string& key, const string& value)
{
    _tokenizerConfigs[key] = value;
}
const string& AnalyzerInfo::getTokenizerName() const {
    return _tokenizerName;
}

const map<string, string>& AnalyzerInfo::getTokenizerConfigs() const {
    return _tokenizerConfigs;
}

void AnalyzerInfo::setTokenizerName(const string &tokenizerName) {
    _tokenizerName = tokenizerName;
}
void AnalyzerInfo::addStopWord(const string& word)
{
    _stopwords.insert(word);
}

void AnalyzerInfo::constructTokenizerName(string& tokenizerName) const {
    map<string, string>::const_iterator it = _tokenizerConfigs.begin();
    for (;it != _tokenizerConfigs.end(); ++it)
    {
        tokenizerName.append(it->second);
        tokenizerName.append("_");
    }
}

void AnalyzerInfo::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json)
{
    json.Jsonize(TOKENIZER_CONFIGS, _tokenizerConfigs, map<string, string>());
    json.Jsonize(TOKENIZER_NAME, _tokenizerName, _tokenizerName);
    json.Jsonize(STOPWORDS, _stopwords, _stopwords);
    json.Jsonize(NORMALIZE_OPTIONS, _normalizeOptions, _normalizeOptions);
}

}
}
