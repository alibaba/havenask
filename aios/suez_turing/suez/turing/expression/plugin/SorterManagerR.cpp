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
#include "suez/turing/expression/plugin/SorterManagerR.h"

namespace suez {
namespace turing {

const std::string SorterManagerR::RESOURCE_ID = "suez_turing_sort_manager_r";

SorterManagerR::SorterManagerR() {}

SorterManagerR::~SorterManagerR() {}

void SorterManagerR::def(navi::ResourceDefBuilder &builder) const { builder.name(RESOURCE_ID, navi::RS_BIZ); }

bool SorterManagerR::config(navi::ResourceConfigContext &ctx) {
    NAVI_JSONIZE(ctx, "config_path", _configPath, _configPath);
    NAVI_JSONIZE(ctx, "sort_config", _sorterConfig, _sorterConfig);
    return true;
}

navi::ErrorCode SorterManagerR::init(navi::ResourceInitContext &ctx) {
    _manager = SorterManager::create(_configPath, _sorterConfig);
    if (!_manager) {
        NAVI_KERNEL_LOG(ERROR, "create sorter manager failed");
    }
    return navi::EC_NONE;
}

REGISTER_RESOURCE(SorterManagerR);

} // namespace turing
} // namespace suez
