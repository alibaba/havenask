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
#pragma once

#include <memory>

#include "autil/Log.h"
#include "autil/codec/NormalizeOptions.h"
#include "autil/legacy/jsonizable.h"
#include "indexlib/analyzer/AnalyzerDefine.h"

namespace indexlibv2 { namespace analyzer {

class AnalyzerInfo : public autil::legacy::Jsonizable
{
public:
    std::string getTokenizerConfig(const std::string& key) const;
    const std::map<std::string, std::string>& getTokenizerConfigs() const;
    void setTokenizerConfig(const std::string& key, const std::string& value);
    const std::string& getTokenizerName() const;
    void setTokenizerName(const std::string& tokenizerName);
    const autil::codec::NormalizeOptions& getNormalizeOptions() const { return _normalizeOptions; }
    void setNormalizeOptions(const autil::codec::NormalizeOptions& options) { _normalizeOptions = options; }

    void addStopWord(const std::string& word);
    const std::set<std::string>& getStopWords() const { return _stopwords; }
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;

    // only for make Compatible config
    void constructTokenizerName(std::string& tokenizerName) const;

private:
    std::map<std::string, std::string> _tokenizerConfigs;
    std::string _tokenizerName;
    std::set<std::string> _stopwords;
    autil::codec::NormalizeOptions _normalizeOptions;

private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<AnalyzerInfo> AnalyzerInfoPtr;

}} // namespace indexlibv2::analyzer
