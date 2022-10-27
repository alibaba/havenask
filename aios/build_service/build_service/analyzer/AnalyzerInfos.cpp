#include "build_service/analyzer/AnalyzerInfos.h"
#include "build_service/analyzer/AnalyzerDefine.h"
#include <autil/legacy/any.h>
#include <autil/legacy/jsonizable.h>
#include <autil/legacy/json.h>
#include <set>

namespace build_service {
namespace analyzer {

using namespace std;
using namespace autil::legacy;
using namespace std::tr1;

BS_LOG_SETUP(analyzer, AnalyzerInfos);

static bool isBuildInSupportedTokenizerType(const string& tokenizerType) {
    if (tokenizerType == ALIWS_ANALYZER
        || tokenizerType == MULTILEVEL_ALIWS_ANALYZER
        || tokenizerType == SEMANTIC_ALIWS_ANALYZER
        || tokenizerType == SIMPLE_ANALYZER
        || tokenizerType == SINGLEWS_ANALYZER) {
        return true;
    }
    return false;
}

void AnalyzerInfos::addAnalyzerInfo(const string &name,
                                    const AnalyzerInfo &analyzerInfo)
{
    _analyzerInfos[name] = analyzerInfo;
}

const AnalyzerInfo* AnalyzerInfos::getAnalyzerInfo(const string& analyzerName) const
{
    AnalyzerInfoMap::const_iterator it = _analyzerInfos.find(analyzerName);
    if (it == _analyzerInfos.end())
    {
        return NULL;
    }
    return &(it->second);
}

void AnalyzerInfos::merge(const AnalyzerInfos& other) {
    _analyzerInfos.insert(other._analyzerInfos.begin(), other._analyzerInfos.end());
}

void AnalyzerInfos::makeCompatibleWithOldConfig() {
    AnalyzerInfoMap::iterator iter = _analyzerInfos.begin();
    for (; iter != _analyzerInfos.end(); ++iter) {
        AnalyzerInfo &analyzerInfo = iter->second;
        string tokenizerType = analyzerInfo.getTokenizerConfig("tokenizer_type");
        if (tokenizerType.empty()
            || !isBuildInSupportedTokenizerType(tokenizerType)) {
            continue;
        }
        string tokenizerName;
        analyzerInfo.constructTokenizerName(tokenizerName);
        analyzerInfo.setTokenizerName(tokenizerName);
        if (_tokenizerConfig.tokenizerNameExist(tokenizerName)) {
            continue;
        }
        TokenizerInfo info;
        info.setTokenizerName(tokenizerName);
        info.setTokenizerType(tokenizerType);
        info.setParameters(analyzerInfo.getTokenizerConfigs());
        _tokenizerConfig.addTokenizerInfo(info);
    }
}

void AnalyzerInfos::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json)
{
    json.Jsonize("analyzers", _analyzerInfos, _analyzerInfos);
    json.Jsonize("tokenizer_config", _tokenizerConfig, _tokenizerConfig);
    _traditionalTables.Jsonize(json);
}

bool AnalyzerInfos::validate() const {
    return true;
}


}
}
