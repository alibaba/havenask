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
#include "suez_navi/resource/AnalyzerFactoryR.h"

using namespace build_service::analyzer;
using namespace build_service::config;

namespace suez_navi {

const std::string AnalyzerFactoryR::RESOURCE_ID = "analyzer_factory_r";

AnalyzerFactoryR::AnalyzerFactoryR() {
}

AnalyzerFactoryR::~AnalyzerFactoryR() {
}

void AnalyzerFactoryR::def(navi::ResourceDefBuilder &builder) const {
    builder.name(RESOURCE_ID);
}

bool AnalyzerFactoryR::config(navi::ResourceConfigContext &ctx) {
    ctx.Jsonize("config_path", _configPath, _configPath);
    return true;
}

navi::ErrorCode AnalyzerFactoryR::init(navi::ResourceInitContext &ctx) {
    ResourceReaderPtr resourceReader(new ResourceReader(_configPath));
    auto factory = build_service::analyzer::AnalyzerFactory::create(resourceReader);
    if (!factory) {
        NAVI_KERNEL_LOG(ERROR,
                        "create AnalyzerFactory failed, config path: [%s]",
                        _configPath.c_str());
        return navi::EC_ABORT;
    }
    _factory = factory;
    return navi::EC_NONE;
}

const build_service::analyzer::AnalyzerFactoryPtr &AnalyzerFactoryR::getFactory() const {
    return _factory;
}

REGISTER_RESOURCE(AnalyzerFactoryR);

}

