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

#include "indexlib/config/SchemaResolver.h"

namespace indexlibv2::table {

class AggregationSchemaResolver : public config::SchemaResolver
{
private:
    Status Check(const config::TabletSchema& schema) override;

private:
    Status LegacySchemaToTabletSchema(const indexlib::config::IndexPartitionSchema& legacySchema,
                                      config::UnresolvedSchema* schema) override;
    Status ResolveLegacySchema(const std::string& indexPath, indexlib::config::IndexPartitionSchema* schema) override;
    Status ResolveTabletSchema(const std::string& indexPath, config::UnresolvedSchema* schema) override;

private:
    Status MaybeSetupUnique(config::UnresolvedSchema* schema);

private:
    static const std::string PRIMARY_KEY_HASH_NAME_SUFFIX;
};

} // namespace indexlibv2::table
