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
#include "navi/resource/QuerySessionR.h"

namespace navi {

const std::string QuerySessionR::RESOURCE_ID = "navi.query_session_r";

QuerySessionR::QuerySessionR(const std::shared_ptr<multi_call::QuerySession> &querySession)
    : _querySession(querySession)
{
}

QuerySessionR::QuerySessionR() {
}

QuerySessionR::~QuerySessionR() {
}

void QuerySessionR::def(ResourceDefBuilder &builder) const {
    builder.name(RESOURCE_ID, navi::RS_GRAPH);
}

bool QuerySessionR::config(ResourceConfigContext &ctx) {
    return true;
}

ErrorCode QuerySessionR::init(ResourceInitContext &ctx) {
    return navi::EC_NONE;
}

REGISTER_RESOURCE(QuerySessionR);

}

