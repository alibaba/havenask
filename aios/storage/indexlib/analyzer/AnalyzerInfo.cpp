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
#include "indexlib/analyzer/AnalyzerInfo.h"

#include <utility>

#include "autil/legacy/jsonizable.h"
#include "indexlib/analyzer/AnalyzerDefine.h"

namespace indexlibv2 { namespace analyzer {
AUTIL_LOG_SETUP(indexlib.analyzer, AnalyzerInfo);

std::string AnalyzerInfo::getTokenizerConfig(const std::string& key) const
{
    std::map<std::string, std::string>::const_iterator it = _tokenizerConfigs.find(key);
    if (it != _tokenizerConfigs.end()) {
        return it->second;
    }
    return "";
}

void AnalyzerInfo::setTokenizerConfig(const std::string& key, const std::string& value)
{
    _tokenizerConfigs[key] = value;
}
const std::string& AnalyzerInfo::getTokenizerName() const { return _tokenizerName; }

const std::map<std::string, std::string>& AnalyzerInfo::getTokenizerConfigs() const { return _tokenizerConfigs; }

void AnalyzerInfo::setTokenizerName(const std::string& tokenizerName) { _tokenizerName = tokenizerName; }
void AnalyzerInfo::addStopWord(const std::string& word) { _stopwords.insert(word); }

void AnalyzerInfo::constructTokenizerName(std::string& tokenizerName) const
{
    std::map<std::string, std::string>::const_iterator it = _tokenizerConfigs.begin();
    for (; it != _tokenizerConfigs.end(); ++it) {
        tokenizerName.append(it->second);
        tokenizerName.append("_");
    }
}

void AnalyzerInfo::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json)
{
    json.Jsonize(TOKENIZER_CONFIGS, _tokenizerConfigs, std::map<std::string, std::string>());
    json.Jsonize(TOKENIZER_NAME, _tokenizerName, _tokenizerName);
    json.Jsonize(STOPWORDS, _stopwords, _stopwords);
    json.Jsonize(NORMALIZE_OPTIONS, _normalizeOptions, _normalizeOptions);
}

}} // namespace indexlibv2::analyzer
