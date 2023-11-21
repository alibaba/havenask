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

#include <map>
#include <memory>
#include <stddef.h>
#include <string>

#include "autil/legacy/jsonizable.h"
#include "build_service/analyzer/TokenizerConfig.h"
#include "build_service/util/Log.h"
#include "indexlib/analyzer/AnalyzerInfo.h"
#include "indexlib/analyzer/TraditionalTables.h"

namespace build_service { namespace analyzer {

class AnalyzerInfos : public autil::legacy::Jsonizable
{
public:
    typedef std::map<std::string, indexlibv2::analyzer::AnalyzerInfo> AnalyzerInfoMap;
    typedef AnalyzerInfoMap::const_iterator ConstIterator;

public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;
    bool validate() const;

public:
    void addAnalyzerInfo(const std::string& name, const indexlibv2::analyzer::AnalyzerInfo& analyzerInfo);
    const indexlibv2::analyzer::AnalyzerInfo* getAnalyzerInfo(const std::string& analyzerName) const;

    size_t getAnalyzerCount() const { return _analyzerInfos.size(); }

    const indexlibv2::analyzer::TraditionalTables& getTraditionalTables() const { return _traditionalTables; }

    void setTraditionalTables(const indexlibv2::analyzer::TraditionalTables& tables) { _traditionalTables = tables; }
    const TokenizerConfig& getTokenizerConfig() const { return _tokenizerConfig; }
    ConstIterator begin() const { return _analyzerInfos.begin(); }
    ConstIterator end() const { return _analyzerInfos.end(); }

    void merge(const AnalyzerInfos& analyzerInfos);

    void makeCompatibleWithOldConfig();

private:
    AnalyzerInfoMap _analyzerInfos;
    TokenizerConfig _tokenizerConfig;
    indexlibv2::analyzer::TraditionalTables _traditionalTables;

private:
    BS_LOG_DECLARE();
};

typedef std::shared_ptr<AnalyzerInfos> AnalyzerInfosPtr;

}} // namespace build_service::analyzer
