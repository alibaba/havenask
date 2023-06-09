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
#include "autil/Log.h"
#include "indexlib/base/Status.h"
#include "indexlib/config/SchemaResolver.h"

namespace indexlibv2::table {

class KKVSchemaResolver : public config::SchemaResolver
{
public:
    KKVSchemaResolver();
    ~KKVSchemaResolver();

private:
    Status Check(const config::TabletSchema& schema) override;
    Status LegacySchemaToTabletSchema(const indexlib::config::IndexPartitionSchema& legacySchema,
                                      config::UnresolvedSchema* schema) override;
    Status ResolveLegacySchema(const std::string& indexPath, indexlib::config::IndexPartitionSchema* schema) override;
    Status ResolveTabletSchema(const std::string& indexPath, config::UnresolvedSchema* schema) override;

private:
    static Status ResolvePkStoreConfig(config::UnresolvedSchema* schema);
    void ResolveGeneralizedValue(config::UnresolvedSchema* schema);
    Status FillIndexConfigs(const indexlib::config::IndexPartitionSchema& legacySchema,
                            config::UnresolvedSchema* schema);
    void FillTTLToSettings(const indexlib::config::IndexPartitionSchema& legacySchema,
                           config::UnresolvedSchema* schema);

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::table
