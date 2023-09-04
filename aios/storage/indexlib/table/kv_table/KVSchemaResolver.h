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

namespace indexlibv2::config {
class TabletOptions;
}

namespace indexlibv2::table {

class KVSchemaResolver : public config::SchemaResolver
{
public:
    KVSchemaResolver();
    ~KVSchemaResolver();

public:
    static std::pair<Status, bool> NeedResolvePkStoreConfig(config::UnresolvedSchema* schema);
    static Status ResolvePkStoreConfig(const std::string& indexTypeStr, config::UnresolvedSchema* schema);
    Status Check(const config::TabletSchema& schema) const override;

private:
    Status LegacySchemaToTabletSchema(const indexlib::config::IndexPartitionSchema& legacySchema,
                                      config::UnresolvedSchema* schema) override;
    Status ResolveLegacySchema(const std::string& indexPath, indexlib::config::IndexPartitionSchema* schema) override;
    Status ResolveTabletSchema(const std::string& indexPath, config::UnresolvedSchema* schema) override;

private:
    Status ResolveGeneralValue(config::UnresolvedSchema* schema);
    Status FillIndexConfigs(const indexlib::config::IndexPartitionSchema& legacySchema,
                            config::UnresolvedSchema* schema);
    void FillTTLToSettings(const indexlib::config::IndexPartitionSchema& legacySchema,
                           config::UnresolvedSchema* schema);

private:
    std::shared_ptr<config::TabletOptions> _options;
    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::table
