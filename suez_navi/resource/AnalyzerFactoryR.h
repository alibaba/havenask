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

#include "navi/engine/Resource.h"
#include "build_service/analyzer/AnalyzerFactory.h"

namespace suez_navi {

class AnalyzerFactoryR : public navi::Resource
{
public:
    AnalyzerFactoryR();
    ~AnalyzerFactoryR();
    AnalyzerFactoryR(const AnalyzerFactoryR &) = delete;
    AnalyzerFactoryR &operator=(const AnalyzerFactoryR &) = delete;
public:
    void def(navi::ResourceDefBuilder &builder) const override;
    bool config(navi::ResourceConfigContext &ctx) override;
    navi::ErrorCode init(navi::ResourceInitContext &ctx) override;
public:
    const build_service::analyzer::AnalyzerFactoryPtr &getFactory() const;
public:
    static const std::string RESOURCE_ID;
private:
    std::string _configPath;
    build_service::analyzer::AnalyzerFactoryPtr _factory;
};

NAVI_TYPEDEF_PTR(AnalyzerFactoryR);

}

