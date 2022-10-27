#ifndef __INDEXLIB_INDEX_PLUGIN_LOADER_H
#define __INDEXLIB_INDEX_PLUGIN_LOADER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/config/module_info.h"

DECLARE_REFERENCE_CLASS(plugin, PluginManager);
DECLARE_REFERENCE_CLASS(config, IndexSchema);
DECLARE_REFERENCE_CLASS(config, IndexPartitionOptions);

IE_NAMESPACE_BEGIN(plugin);

class IndexPluginLoader
{
public:
    IndexPluginLoader();
    ~IndexPluginLoader();
public:
    static std::string GetIndexModuleFileName(const std::string& indexerName);
    
    static PluginManagerPtr Load(const std::string& moduleRootPath,
                                 const config::IndexSchemaPtr& indexSchema,
                                 const config::IndexPartitionOptions& options);
private:
    static bool FillCustomizedIndexModuleInfos(const std::string& moduleRootPath,
        const config::IndexSchemaPtr& indexSchema, config::ModuleInfos& moduleInfos);
    static bool FillOfflineModuleInfos(const std::string& moduleRootPath,
        const config::IndexPartitionOptions& options, config::ModuleInfos& moduleInfos);

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(IndexPluginLoader);

IE_NAMESPACE_END(plugin);

#endif //__INDEXLIB_INDEX_PLUGIN_LOADER_H
