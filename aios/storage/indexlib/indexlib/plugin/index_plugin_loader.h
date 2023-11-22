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

#include "indexlib/config/module_info.h"
#include "indexlib/misc/common.h"
#include "indexlib/misc/log.h"

DECLARE_REFERENCE_CLASS(plugin, PluginManager);
DECLARE_REFERENCE_CLASS(config, IndexSchema);
DECLARE_REFERENCE_CLASS(config, IndexPartitionOptions);

namespace indexlib { namespace plugin {

class IndexPluginLoader
{
public:
    IndexPluginLoader();
    ~IndexPluginLoader();

public:
    static std::string GetIndexModuleFileName(const std::string& indexerName);

    static PluginManagerPtr Load(const std::string& moduleRootPath, const config::IndexSchemaPtr& indexSchema,
                                 const config::IndexPartitionOptions& options);

private:
    static bool FillCustomizedIndexModuleInfos(const std::string& moduleRootPath,
                                               const config::IndexSchemaPtr& indexSchema,
                                               config::ModuleInfos& moduleInfos);
    static bool FillOfflineModuleInfos(const std::string& moduleRootPath, const config::IndexPartitionOptions& options,
                                       config::ModuleInfos& moduleInfos);

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(IndexPluginLoader);

}} // namespace indexlib::plugin
