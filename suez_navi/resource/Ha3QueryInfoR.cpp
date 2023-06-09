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
#include "suez_navi/resource/Ha3QueryInfoR.h"

namespace suez_navi {

const std::string Ha3QueryInfoR::RESOURCE_ID = "ha3_query_info_r";

Ha3QueryInfoR::Ha3QueryInfoR() {
}

Ha3QueryInfoR::~Ha3QueryInfoR() {
}

void Ha3QueryInfoR::def(navi::ResourceDefBuilder &builder) const {
    builder.name(RESOURCE_ID);
}

bool Ha3QueryInfoR::config(navi::ResourceConfigContext &ctx) {
    ctx.Jsonize("query_info", _queryInfo, _queryInfo);
    return true;
}

navi::ErrorCode Ha3QueryInfoR::init(navi::ResourceInitContext &ctx) {
    return navi::EC_NONE;
}

const isearch::common::QueryInfo &Ha3QueryInfoR::getQueryInfo() const {
    return _queryInfo;
}

REGISTER_RESOURCE(Ha3QueryInfoR);

}

