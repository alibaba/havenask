#ifndef __INDEXLIB_TABLE_FACTORY_WRAPPER_H
#define __INDEXLIB_TABLE_FACTORY_WRAPPER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/util/key_value_map.h"
#include "indexlib/config/index_partition_options.h"

DECLARE_REFERENCE_CLASS(table, TableWriter);
DECLARE_REFERENCE_CLASS(table, TableResource);
DECLARE_REFERENCE_CLASS(table, TableFactory);
DECLARE_REFERENCE_CLASS(table, MergePolicy);
DECLARE_REFERENCE_CLASS(table, TableMerger);
DECLARE_REFERENCE_CLASS(table, TableReader);
DECLARE_REFERENCE_CLASS(plugin, PluginManager);
DECLARE_REFERENCE_CLASS(config, IndexPartitionSchema);

IE_NAMESPACE_BEGIN(table);

class TableFactoryWrapper
{
public:
    TableFactoryWrapper(const config::IndexPartitionSchemaPtr& schema,
                        const config::IndexPartitionOptions& options,
                        const plugin::PluginManagerPtr& pluginManager);
    virtual ~TableFactoryWrapper();
public:
    virtual bool Init();
    virtual TableWriterPtr CreateTableWriter() const;
    virtual TableResourcePtr CreateTableResource() const;
    virtual MergePolicyPtr CreateMergePolicy() const;
    virtual TableMergerPtr CreateTableMerger() const;
    virtual TableReaderPtr CreateTableReader() const; 
    const plugin::PluginManagerPtr& GetPluginManager() const;

private:
    config::IndexPartitionSchemaPtr mSchema;
    config::IndexPartitionOptions mOptions;
    plugin::PluginManagerPtr mPluginManager;
    util::KeyValueMap mParameters;
    static std::string TABLE_FACTORY_NAME;
    TableFactory* mTableFactory;
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(TableFactoryWrapper);

IE_NAMESPACE_END(table);

#endif //__INDEXLIB_TABLE_FACTORY_WRAPPER_H
