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
#include "suez/turing/expression/plugin/RankManagerR.h"

namespace suez {
namespace turing {

const std::string RankManagerR::RESOURCE_ID = "suez_turing_rank_manager_r";

RankManagerR::RankManagerR() {}

RankManagerR::~RankManagerR() {}

void RankManagerR::def(navi::ResourceDefBuilder &builder) const {
    builder.name(RESOURCE_ID, navi::RS_BIZ)
        .dependOn(CavaPluginManagerR::RESOURCE_ID, true, BIND_RESOURCE_TO(_cavaPluginManagerR));
}

bool RankManagerR::config(navi::ResourceConfigContext &ctx) {
    NAVI_JSONIZE(ctx, "config_path", _configPath, _configPath);
    NAVI_JSONIZE(ctx, "rank_config", _rankConfig, _rankConfig);
    return true;
}

navi::ErrorCode RankManagerR::init(navi::ResourceInitContext &ctx) {
    const auto &cavaPluginManager = _cavaPluginManagerR->getManager();
    _manager = RankManager::create(_configPath, cavaPluginManager.get(), _rankConfig);
    if (!_manager) {
        NAVI_KERNEL_LOG(ERROR, "create rank manager failed");
        return navi::EC_ABORT;
    }
    return navi::EC_NONE;
}

REGISTER_RESOURCE(RankManagerR);

} // namespace turing
} // namespace suez
