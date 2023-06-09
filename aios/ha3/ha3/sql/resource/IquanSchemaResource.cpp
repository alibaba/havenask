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
#include "ha3/sql/resource/IquanSchemaResource.h"

#include "ha3/sql/common/Log.h"

using namespace std;

namespace isearch {
namespace sql {
AUTIL_LOG_SETUP(sql, IquanSchemaResource);

const std::string IquanSchemaResource::RESOURCE_ID = "IquanSchemaResource";

IquanSchemaResource::IquanSchemaResource() {}
void IquanSchemaResource::def(navi::ResourceDefBuilder &builder) const {
    builder.name(RESOURCE_ID)
        .depend(IquanResource::RESOURCE_ID, true, BIND_RESOURCE_TO(_iquanResouce));
}

bool IquanSchemaResource::config(navi::ResourceConfigContext &ctx) {
    ctx.Jsonize("iquan_table_models", _sqlTableModels, _sqlTableModels);
    return true;
}

navi::ErrorCode IquanSchemaResource::init(navi::ResourceInitContext &ctx) {
    auto _sqlClient = _iquanResouce->getSqlClient();
    if (_sqlClient == nullptr) {
        SQL_LOG(ERROR, "get sql client empty");
        return navi::EC_INIT_RESOURCE;
    }
    iquan::Status status = _sqlClient->updateTables(_sqlTableModels);
    if (!status.ok()) {
        SQL_LOG(ERROR, "update table info failed, error msg: %s", status.errorMessage().c_str());
        return navi::EC_INIT_RESOURCE;
    }
    return navi::EC_NONE;
}

REGISTER_RESOURCE(IquanSchemaResource);

} // namespace sql
} // end namespace isearch
