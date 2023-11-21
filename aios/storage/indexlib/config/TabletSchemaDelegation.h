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
#pragma once

#include <memory>
#include <string>

#include "autil/Log.h"
#include "indexlib/base/Status.h"

namespace indexlib::config {
class IndexPartitionSchema;
}

namespace indexlibv2::config {
class TabletSchema;
class SchemaResolver;

// for framework::TabletSchemaLoader to call private interface of SchemaResolver and TabletSchema
// should not use it elsewhere
class TabletSchemaDelegation
{
protected:
    static std::unique_ptr<TabletSchema> Deserialize(const std::string& jsonStr);
    static Status Resolve(SchemaResolver* schemaResolver, const std::string& indexPath, TabletSchema* schema);
    static Status LegacySchemaToTabletSchema(SchemaResolver* schemaResolver,
                                             const indexlib::config::IndexPartitionSchema& legacySchema,
                                             TabletSchema* schema);
    static Status Check(SchemaResolver* schemaResolver, const config::TabletSchema& schema);

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::config
