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
#ifndef __INDEXLIB_INDEX_PLUGIN_UTIL_H
#define __INDEXLIB_INDEX_PLUGIN_UTIL_H

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/config/customized_index_config.h"
#include "indexlib/config/module_info.h"
#include "indexlib/index/normal/inverted_index/customized_index/index_module_factory.h"
#include "indexlib/indexlib.h"
#include "indexlib/plugin/Module.h"
#include "indexlib/plugin/plugin_manager.h"

namespace indexlib { namespace index {

class IndexPluginUtil
{
public:
    IndexPluginUtil();
    ~IndexPluginUtil();

public:
    static IndexModuleFactory* GetFactory(const config::CustomizedIndexConfigPtr& indexConfig,
                                          const plugin::PluginManagerPtr& pluginManager);

    static Indexer* CreateIndexer(const config::IndexConfigPtr& indexConfig,
                                  const plugin::PluginManagerPtr& pluginManager);

    static IndexerUserData* CreateIndexerUserData(const config::IndexConfigPtr& indexConfig,
                                                  const plugin::PluginManagerPtr& pluginManager);

    static IndexReducer* CreateIndexReducer(const config::IndexConfigPtr& indexConfig,
                                            const plugin::PluginManagerPtr& pluginManager);

    static IndexSegmentRetriever* CreateIndexSegmentRetriever(const config::IndexConfigPtr& indexConfig,
                                                              const plugin::PluginManagerPtr& pluginManager);

    static IndexRetriever* CreateIndexRetriever(const config::IndexConfigPtr& indexConfig,
                                                const plugin::PluginManagerPtr& pluginManager);

    static std::string INDEX_MODULE_FACTORY_SUFFIX;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(IndexPluginUtil);
}} // namespace indexlib::index

#endif //__INDEXLIB_INDEX_PLUGIN_UTIL_H
