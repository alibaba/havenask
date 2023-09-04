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
#include "suez/turing/navi/QueryMemPoolR.h"

namespace suez {
namespace turing {

const std::string QueryMemPoolR::RESOURCE_ID = "suez_turing.query_mem_pool_r";

QueryMemPoolR::QueryMemPoolR() {}

QueryMemPoolR::~QueryMemPoolR() {}

void QueryMemPoolR::def(navi::ResourceDefBuilder &builder) const { builder.name(RESOURCE_ID, navi::RS_GRAPH); }

bool QueryMemPoolR::config(navi::ResourceConfigContext &ctx) { return true; }

navi::ErrorCode QueryMemPoolR::init(navi::ResourceInitContext &ctx) {
    _pool = _graphMemoryPoolR->getPool();
    if (!_pool) {
        return navi::EC_ABORT;
    }
    return navi::EC_NONE;
}

const autil::mem_pool::PoolPtr &QueryMemPoolR::getPool() const { return _pool; }

REGISTER_RESOURCE(QueryMemPoolR);

} // namespace turing
} // namespace suez
