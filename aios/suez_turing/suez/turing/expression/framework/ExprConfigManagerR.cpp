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
#include "suez/turing/expression/framework/ExprConfigManagerR.h"

namespace suez {
namespace turing {

const std::string ExprConfigManagerR::RESOURCE_ID = "suez_turing_expr_config_manager_r";

ExprConfigManagerR::ExprConfigManagerR() {}

ExprConfigManagerR::~ExprConfigManagerR() {}

void ExprConfigManagerR::def(navi::ResourceDefBuilder &builder) const { builder.name(RESOURCE_ID, navi::RS_BIZ); }

bool ExprConfigManagerR::config(navi::ResourceConfigContext &ctx) {
    NAVI_JSONIZE(ctx, "expr_configs", _exprConfigs, _exprConfigs);
    return true;
}

navi::ErrorCode ExprConfigManagerR::init(navi::ResourceInitContext &ctx) {
    _manager = std::make_shared<ExprConfigManager>();
    if (!_manager->init(_exprConfigs)) {
        NAVI_KERNEL_LOG(ERROR, "ExprConfigManager init failed");
        return navi::EC_ABORT;
    }
    return navi::EC_NONE;
}

REGISTER_RESOURCE(ExprConfigManagerR);

} // namespace turing
} // namespace suez
