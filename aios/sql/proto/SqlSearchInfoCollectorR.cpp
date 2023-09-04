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
#include "sql/proto/SqlSearchInfoCollectorR.h"

#include <memory>

#include "navi/builder/ResourceDefBuilder.h"
#include "navi/engine/Resource.h"
#include "navi/proto/KernelDef.pb.h"

namespace navi {
class ResourceInitContext;
} // namespace navi

namespace sql {

const std::string SqlSearchInfoCollectorR::RESOURCE_ID = "sql_search_info_collector_r";

SqlSearchInfoCollectorR::SqlSearchInfoCollectorR() {}

SqlSearchInfoCollectorR::~SqlSearchInfoCollectorR() {}

void SqlSearchInfoCollectorR::def(navi::ResourceDefBuilder &builder) const {
    builder.name(RESOURCE_ID, navi::RS_SUB_GRAPH);
}

bool SqlSearchInfoCollectorR::config(navi::ResourceConfigContext &ctx) {
    return true;
}

navi::ErrorCode SqlSearchInfoCollectorR::init(navi::ResourceInitContext &ctx) {
    _collector = std::make_shared<SqlSearchInfoCollector>();
    return navi::EC_NONE;
}

const SqlSearchInfoCollectorPtr &SqlSearchInfoCollectorR::getCollector() const {
    return _collector;
}

REGISTER_RESOURCE(SqlSearchInfoCollectorR);

} // namespace sql
