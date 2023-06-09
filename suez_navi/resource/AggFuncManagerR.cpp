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
#include "suez_navi/resource/AggFuncManagerR.h"

using namespace isearch::sql;

namespace suez_navi {

const std::string AggFuncManagerR::RESOURCE_ID = "agg_func_manager_r";

AggFuncManagerR::AggFuncManagerR() {
}

AggFuncManagerR::~AggFuncManagerR() {
}

void AggFuncManagerR::def(navi::ResourceDefBuilder &builder) const {
    builder.name(RESOURCE_ID);
}

bool AggFuncManagerR::config(navi::ResourceConfigContext &ctx) {
    ctx.Jsonize("config_path", _configPath, _configPath);
    ctx.Jsonize("modules", _aggConfig.modules, _aggConfig.modules);
    return true;
}

navi::ErrorCode AggFuncManagerR::init(navi::ResourceInitContext &ctx) {
    build_service::config::ResourceReaderPtr resourceReader(
        new build_service::config::ResourceReader(_configPath));
    AggFuncManagerPtr manager(new AggFuncManager(resourceReader));
    if (!manager->init(_aggConfig)) {
        NAVI_KERNEL_LOG(
            ERROR,
            "init sql agg func manager failed, config_path [%s], config[%s]",
            _configPath.c_str(),
            autil::legacy::ToJsonString(_aggConfig).c_str());
        return navi::EC_ABORT;
    }
    _manager = manager;
    return navi::EC_NONE;
}

const isearch::sql::AggFuncManagerPtr &AggFuncManagerR::getManager() const {
    return _manager;
}

REGISTER_RESOURCE(AggFuncManagerR);

}

