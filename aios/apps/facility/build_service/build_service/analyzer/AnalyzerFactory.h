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
#include <string>

#include "build_service/common_define.h"
#include "build_service/config/ResourceReader.h"
#include "build_service/util/Log.h"
#include "indexlib/analyzer/IAnalyzerFactory.h"

namespace build_service { namespace analyzer {

class AnalyzerInfos;

BS_TYPEDEF_PTR(AnalyzerInfos);
class TokenizerManager;

BS_TYPEDEF_PTR(TokenizerManager);
class AnalyzerFactory;

BS_TYPEDEF_PTR(AnalyzerFactory);

class AnalyzerFactory : public indexlibv2::analyzer::IAnalyzerFactory
{
public:
    AnalyzerFactory();
    virtual ~AnalyzerFactory();

    static AnalyzerFactoryPtr create(const config::ResourceReaderPtr& resourceReaderPtr);

    indexlibv2::analyzer::Analyzer* createAnalyzer(const std::string& name) const override;
    bool hasAnalyzer(const std::string& name) const override;

public: // for test
    bool init(const AnalyzerInfosPtr analyzerInfosPtr, const TokenizerManagerPtr& tokenizerManagerPtr);

private:
    std::map<std::string, indexlibv2::analyzer::Analyzer*> _analyzerMap;
    TokenizerManagerPtr _tokenizerManagerPtr;

    BS_LOG_DECLARE();
};

}} // namespace build_service::analyzer
