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

#include <string>

#include "autil/Log.h"
#include "indexlib/base/Status.h"

namespace indexlib::config {
class IndexPartitionSchema;
}

namespace indexlibv2::config {
class TabletSchema;
class UnresolvedSchema;

// SchemaResolver is the interface class for rewrite and validate schema.
class SchemaResolver
{
public:
    SchemaResolver() = default;
    virtual ~SchemaResolver() = default;

private:
    Status Resolve(const std::string& indexPath, TabletSchema* schema);
    Status LegacySchemaToTabletSchema(const indexlib::config::IndexPartitionSchema& legacySchema, TabletSchema* schema);

protected:
    virtual Status Check(const config::TabletSchema& schema) const = 0;

protected:
    virtual Status LegacySchemaToTabletSchema(const indexlib::config::IndexPartitionSchema& legacySchema,
                                              config::UnresolvedSchema* schema) = 0;
    virtual Status ResolveLegacySchema(const std::string& indexPath,
                                       indexlib::config::IndexPartitionSchema* schema) = 0;
    virtual Status ResolveTabletSchema(const std::string& indexPath, config::UnresolvedSchema* schema) = 0;

private:
    friend class TabletSchemaDelegation;
    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::config
