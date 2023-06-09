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
#include "suez_navi/resource/TvfFuncManagerR.h"

using namespace isearch::sql;

namespace suez_navi {

const std::string TvfFuncManagerR::RESOURCE_ID = "tvf_func_manager_r";

TvfFuncManagerR::TvfFuncManagerR() {
}

TvfFuncManagerR::~TvfFuncManagerR() {
}

void TvfFuncManagerR::def(navi::ResourceDefBuilder &builder) const {
    builder.name(RESOURCE_ID);
}

bool TvfFuncManagerR::config(navi::ResourceConfigContext &ctx) {
    ctx.Jsonize("config_path", _configPath, _configPath);
    ctx.Jsonize("modules", _tvfConfig.modules, _tvfConfig.modules);
    ctx.Jsonize("tvf_profiles", _tvfConfig.tvfProfileInfos, _tvfConfig.tvfProfileInfos);
    return true;
}

navi::ErrorCode TvfFuncManagerR::init(navi::ResourceInitContext &ctx) {
    build_service::config::ResourceReaderPtr resourceReader(
        new build_service::config::ResourceReader(_configPath));
    isearch::sql::TvfFuncManagerPtr manager(new TvfFuncManager(resourceReader));
    if (!manager->init(_tvfConfig)) {
        NAVI_KERNEL_LOG(ERROR, "init TvfFuncManager failed, config_path [%s]",
                        _configPath.c_str());
        return navi::EC_ABORT;
    }
    return navi::EC_NONE;
}

const isearch::sql::TvfFuncManagerPtr &TvfFuncManagerR::getManager() const {
    return _manager;
}

REGISTER_RESOURCE(TvfFuncManagerR);

}

