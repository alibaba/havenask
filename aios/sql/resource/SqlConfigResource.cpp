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
#include "sql/resource/SqlConfigResource.h"

#include <engine/NaviConfigContext.h>
#include <iosfwd>

#include "navi/builder/ResourceDefBuilder.h"
#include "navi/common.h"
#include "navi/engine/Resource.h"
#include "navi/proto/KernelDef.pb.h"

namespace navi {
class ResourceInitContext;
} // namespace navi

using namespace std;

namespace sql {
AUTIL_LOG_SETUP(sql, SqlConfigResource);
const std::string SqlConfigResource::RESOURCE_ID = "SqlConfigResource";

SqlConfigResource::SqlConfigResource() {}

SqlConfigResource::SqlConfigResource(const SqlConfig &sqlConfig)
    : _sqlConfig(sqlConfig) {}

SqlConfigResource::SqlConfigResource(const SqlConfig &sqlConfig,
                                     const SwiftWriteConfig &swiftWriterConfig,
                                     const TableWriteConfig &tableWriterConfig)
    : _sqlConfig(sqlConfig)
    , _swiftWriterConfig(swiftWriterConfig)
    , _tableWriterConfig(tableWriterConfig) {}

void SqlConfigResource::def(navi::ResourceDefBuilder &builder) const {
    builder.name(RESOURCE_ID, navi::RS_BIZ);
}

bool SqlConfigResource::config(navi::ResourceConfigContext &ctx) {
    NAVI_JSONIZE(ctx, "iquan_sql_config", _sqlConfig, _sqlConfig);
    return true;
}

navi::ErrorCode SqlConfigResource::init(navi::ResourceInitContext &ctx) {
    return navi::EC_NONE;
}

REGISTER_RESOURCE(SqlConfigResource);

} // namespace sql
