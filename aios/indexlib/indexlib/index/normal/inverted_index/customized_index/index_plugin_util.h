#ifndef __INDEXLIB_INDEX_PLUGIN_UTIL_H
#define __INDEXLIB_INDEX_PLUGIN_UTIL_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/plugin/Module.h"
#include "indexlib/plugin/plugin_manager.h"
#include "indexlib/config/module_info.h"
#include "indexlib/config/customized_index_config.h"
#include "indexlib/index/normal/inverted_index/customized_index/index_module_factory.h"

IE_NAMESPACE_BEGIN(index);

class IndexPluginUtil
{
public:
    IndexPluginUtil();
    ~IndexPluginUtil();
public:
    static IndexModuleFactory* GetFactory(
            const config::CustomizedIndexConfigPtr& indexConfig,
            const plugin::PluginManagerPtr& pluginManager);

    static Indexer* CreateIndexer(
            const config::IndexConfigPtr& indexConfig,
            const plugin::PluginManagerPtr& pluginManager);

    static IndexerUserData* CreateIndexerUserData(
            const config::IndexConfigPtr& indexConfig,
            const plugin::PluginManagerPtr& pluginManager);

    static IndexReducer* CreateIndexReducer(
            const config::IndexConfigPtr& indexConfig,
            const plugin::PluginManagerPtr& pluginManager);

    static IndexSegmentRetriever* CreateIndexSegmentRetriever(
            const config::IndexConfigPtr& indexConfig,
            const plugin::PluginManagerPtr& pluginManager);    

    static IndexRetriever* CreateIndexRetriever(
            const config::IndexConfigPtr& indexConfig,
            const plugin::PluginManagerPtr& pluginManager);

    static std::string INDEX_MODULE_FACTORY_SUFFIX;
    
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(IndexPluginUtil);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_INDEX_PLUGIN_UTIL_H
