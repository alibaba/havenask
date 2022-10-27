#ifndef __INDEXLIB_TABLE_PLUGIN_LOADER_H
#define __INDEXLIB_TABLE_PLUGIN_LOADER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/config/index_partition_options.h"

DECLARE_REFERENCE_CLASS(config, IndexPartitionSchema);
DECLARE_REFERENCE_CLASS(plugin, PluginManager);

IE_NAMESPACE_BEGIN(table);

class TablePluginLoader
{
public:
    TablePluginLoader();
    ~TablePluginLoader();
public:
    static plugin::PluginManagerPtr Load(const std::string& pluginPath,
                                         const config::IndexPartitionSchemaPtr& schema,
                                         const config::IndexPartitionOptions& options);
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(TablePluginLoader);

IE_NAMESPACE_END(table);

#endif //__INDEXLIB_TABLE_PLUGIN_LOADER_H
