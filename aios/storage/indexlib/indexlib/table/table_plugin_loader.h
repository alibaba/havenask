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
#ifndef __INDEXLIB_TABLE_PLUGIN_LOADER_H
#define __INDEXLIB_TABLE_PLUGIN_LOADER_H

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/config/index_partition_options.h"
#include "indexlib/indexlib.h"

DECLARE_REFERENCE_CLASS(config, IndexPartitionSchema);
DECLARE_REFERENCE_CLASS(plugin, PluginManager);

namespace indexlib { namespace table {

class TablePluginLoader
{
public:
    TablePluginLoader();
    ~TablePluginLoader();

public:
    static plugin::PluginManagerPtr Load(const std::string& pluginPath, const config::IndexPartitionSchemaPtr& schema,
                                         const config::IndexPartitionOptions& options);

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(TablePluginLoader);
}} // namespace indexlib::table

#endif //__INDEXLIB_TABLE_PLUGIN_LOADER_H
