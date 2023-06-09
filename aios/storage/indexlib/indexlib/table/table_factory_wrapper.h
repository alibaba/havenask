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
#ifndef __INDEXLIB_TABLE_FACTORY_WRAPPER_H
#define __INDEXLIB_TABLE_FACTORY_WRAPPER_H

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/config/index_partition_options.h"
#include "indexlib/indexlib.h"
#include "indexlib/util/KeyValueMap.h"

DECLARE_REFERENCE_CLASS(table, TableWriter);
DECLARE_REFERENCE_CLASS(table, TableResource);
DECLARE_REFERENCE_CLASS(table, TableFactory);
DECLARE_REFERENCE_CLASS(table, MergePolicy);
DECLARE_REFERENCE_CLASS(table, TableMerger);
DECLARE_REFERENCE_CLASS(table, TableReader);
DECLARE_REFERENCE_CLASS(table, ExecutorProvider);
DECLARE_REFERENCE_CLASS(plugin, PluginManager);
DECLARE_REFERENCE_CLASS(config, IndexPartitionSchema);

namespace indexlib { namespace table {

class TableFactoryWrapper
{
public:
    TableFactoryWrapper(const config::IndexPartitionSchemaPtr& schema, const config::IndexPartitionOptions& options,
                        const plugin::PluginManagerPtr& pluginManager);
    virtual ~TableFactoryWrapper();

public:
    virtual bool Init();
    virtual TableWriterPtr CreateTableWriter() const;
    virtual TableResourcePtr CreateTableResource() const;
    virtual MergePolicyPtr CreateMergePolicy() const;
    virtual TableMergerPtr CreateTableMerger() const;
    virtual TableReaderPtr CreateTableReader() const;
    virtual std::function<void()> CreateCleanResourceTask() const;
    const plugin::PluginManagerPtr& GetPluginManager() const;
    virtual ExecutorProviderPtr CreateExecutorProvider() const;

private:
    config::IndexPartitionSchemaPtr mSchema;
    config::IndexPartitionOptions mOptions;
    plugin::PluginManagerPtr mPluginManager;
    util::KeyValueMap mParameters;
    TableFactory* mTableFactory;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(TableFactoryWrapper);
}} // namespace indexlib::table

#endif //__INDEXLIB_TABLE_FACTORY_WRAPPER_H
