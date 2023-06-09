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
#include "fslib/common/common_type.h"
#include "indexlib/base/Constant.h"
#include "indexlib/base/Status.h"
#include "indexlib/config/TabletSchema.h"
#include "indexlib/config/TabletSchemaDelegation.h"
#include "indexlib/file_system/IDirectory.h"

namespace indexlibv2::config {
class TabletOptions;
} // namespace indexlibv2::config

namespace indexlibv2::framework {
class ITabletFactory;

class TabletSchemaLoader : public config::TabletSchemaDelegation
{
public:
    static Status ListSchema(const std::shared_ptr<indexlib::file_system::IDirectory>& dir, fslib::FileList* fileList);

    static std::unique_ptr<config::TabletSchema>
    GetSchema(const std::shared_ptr<indexlib::file_system::IDirectory>& dir, schemaid_t schemaId);

    static std::unique_ptr<config::TabletSchema> LoadSchema(const std::string& schemaStr);

    static std::unique_ptr<config::TabletSchema> LoadSchema(const std::string& dir, const std::string& schemaFileName);

    static std::unique_ptr<config::TabletSchema>
    LoadSchema(const std::shared_ptr<indexlib::file_system::IDirectory>& dir, const std::string& schemaFileName);

    static Status ResolveSchema(const std::shared_ptr<config::TabletOptions>& options, const std::string& indexPath,
                                config::TabletSchema* schema);

    static schemaid_t GetSchemaId(const std::string& schemaFileName);

private:
    static bool IsNotSchemaFile(const std::string& fileName);
    static bool ExtractSchemaId(const std::string& fileName, schemaid_t& schemaId);
    static std::unique_ptr<ITabletFactory> CreateTabletFactory(const std::string& tableType,
                                                               const std::shared_ptr<config::TabletOptions>& options);

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::framework
