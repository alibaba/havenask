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
#include "indexlib/config/SchemaResolver.h"

#include "indexlib/config/TabletSchema.h"

namespace indexlibv2::config {

AUTIL_LOG_SETUP(indexlib.config, SchemaResolver);

Status SchemaResolver::Resolve(const std::string& indexPath, TabletSchema* schema)
{
    auto legacySchema = schema->GetLegacySchema();
    UnresolvedSchema* unresolvedSchema = schema->_impl.get();
    const char* tableName = schema->GetTableName().c_str();
    if (legacySchema) {
        auto status = ResolveLegacySchema(indexPath, legacySchema.get());
        RETURN_IF_STATUS_ERROR(status, "resolve legacy schema failed, table[%s]", tableName);
    }
    Status status = ResolveTabletSchema(indexPath, unresolvedSchema);
    RETURN_IF_STATUS_ERROR(status, "resolve tablet schema failed, table[%s]", tableName);
    return Status::OK();
}

Status SchemaResolver::LegacySchemaToTabletSchema(const indexlib::config::IndexPartitionSchema& legacySchema,
                                                  TabletSchema* schema)
{
    return LegacySchemaToTabletSchema(legacySchema, schema->_impl.get());
}

} // namespace indexlibv2::config
