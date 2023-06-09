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
#include "indexlib/config/TabletSchemaDelegation.h"

#include "indexlib/config/SchemaResolver.h"
#include "indexlib/config/TabletSchema.h"

namespace indexlibv2::config {
AUTIL_LOG_SETUP(indexlib.config, TabletSchemaDelegation);

std::unique_ptr<TabletSchema> TabletSchemaDelegation::Deserialize(const std::string& jsonStr)
{
    auto schema = std::make_unique<TabletSchema>();
    bool ret = schema->Deserialize(jsonStr);
    if (!ret) {
        return nullptr;
    }
    return schema;
}

Status TabletSchemaDelegation::Resolve(SchemaResolver* schemaResolver, const std::string& indexPath,
                                       TabletSchema* schema)
{
    assert(schemaResolver);
    return schemaResolver->Resolve(indexPath, schema);
}
Status TabletSchemaDelegation::LegacySchemaToTabletSchema(SchemaResolver* schemaResolver,
                                                          const indexlib::config::IndexPartitionSchema& legacySchema,
                                                          TabletSchema* schema)
{
    assert(schemaResolver);
    return schemaResolver->LegacySchemaToTabletSchema(legacySchema, schema);
}

Status TabletSchemaDelegation::Check(SchemaResolver* schemaResolver, const config::TabletSchema& schema)
{
    assert(schemaResolver);
    return schemaResolver->Check(schema);
}

} // namespace indexlibv2::config
