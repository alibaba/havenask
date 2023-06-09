/*
 * Copyright 2014-present Alibaba Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#include "build_service/analyzer/AnalyzerInfos.h"

#include <set>

#include "autil/legacy/any.h"
#include "autil/legacy/json.h"
#include "autil/legacy/jsonizable.h"
#include "indexlib/analyzer/AnalyzerDefine.h"

namespace build_service { namespace analyzer {

using namespace std;
using namespace autil::legacy;
using namespace indexlibv2::analyzer;

BS_LOG_SETUP(analyzer, AnalyzerInfos);

static bool isBuildInSupportedTokenizerType(const string& tokenizerType)
{
    if (tokenizerType == ALIWS_ANALYZER || tokenizerType == MULTILEVEL_ALIWS_ANALYZER ||
        tokenizerType == SEMANTIC_ALIWS_ANALYZER || tokenizerType == SIMPLE_ANALYZER ||
        tokenizerType == SINGLEWS_ANALYZER) {
        return true;
    }
    return false;
}

void AnalyzerInfos::addAnalyzerInfo(const string& name, const AnalyzerInfo& analyzerInfo)
{
    _analyzerInfos[name] = analyzerInfo;
}

const AnalyzerInfo* AnalyzerInfos::getAnalyzerInfo(const string& analyzerName) const
{
    AnalyzerInfoMap::const_iterator it = _analyzerInfos.find(analyzerName);
    if (it == _analyzerInfos.end()) {
        return NULL;
    }
    return &(it->second);
}

void AnalyzerInfos::merge(const AnalyzerInfos& other)
{
    _analyzerInfos.insert(other._analyzerInfos.begin(), other._analyzerInfos.end());
}

void AnalyzerInfos::makeCompatibleWithOldConfig()
{
    AnalyzerInfoMap::iterator iter = _analyzerInfos.begin();
    for (; iter != _analyzerInfos.end(); ++iter) {
        AnalyzerInfo& analyzerInfo = iter->second;
        string tokenizerType = analyzerInfo.getTokenizerConfig("tokenizer_type");
        if (tokenizerType.empty() || !isBuildInSupportedTokenizerType(tokenizerType)) {
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

bool AnalyzerInfos::validate() const { return true; }

}} // namespace build_service::analyzer
