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
#include "indexlib/table/table_factory_wrapper.h"

#include <assert.h>
#include <cstddef>
#include <map>
#include <memory>
#include <string>
#include <string_view>

#include "alog/Logger.h"
#include "indexlib/config/customized_table_config.h"
#include "indexlib/config/index_partition_options.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/config/load_config_list.h"
#include "indexlib/index_base/index_meta/version.h"
#include "indexlib/plugin/Module.h"
#include "indexlib/plugin/ModuleFactory.h"
#include "indexlib/plugin/plugin_manager.h"
#include "indexlib/table/executor_provider.h"
#include "indexlib/table/merge_policy.h"
#include "indexlib/table/table_factory.h"
#include "indexlib/table/table_merger.h"
#include "indexlib/table/table_reader.h"
#include "indexlib/table/table_resource.h"
#include "indexlib/table/table_writer.h"

using namespace std;

using namespace indexlib::config;
using namespace indexlib::plugin;

namespace indexlib { namespace table {
IE_LOG_SETUP(table, TableFactoryWrapper);

TableFactoryWrapper::TableFactoryWrapper(const config::IndexPartitionSchemaPtr& schema,
                                         const config::IndexPartitionOptions& options,
                                         const plugin::PluginManagerPtr& pluginManager)
    : mSchema(schema)
    , mOptions(options)
    , mPluginManager(pluginManager)
    , mTableFactory(NULL)
{
}

TableFactoryWrapper::~TableFactoryWrapper()
{
    if (mTableFactory) {
        mTableFactory->Close(mParameters);
    }
    mTableFactory = NULL;
}

bool TableFactoryWrapper::Init()
{
    if (!mSchema) {
        IE_LOG(ERROR, "schema is NULL");
        return false;
    }
    if (!mPluginManager) {
        IE_LOG(ERROR, "PluginManager is NULL in table[%s]", mSchema->GetSchemaName().c_str());
        return false;
    }
    CustomizedTableConfigPtr customTableConfig = mSchema->GetCustomizedTableConfig();
    if (!customTableConfig) {
        IE_LOG(ERROR, "customized_table_config is NULL in Schema[%s]", mSchema->GetSchemaName().c_str());
        return false;
    }
    mParameters = customTableConfig->GetParameters();
    mParameters[string(TableFactory::SCHEMA_NAME)] = mSchema->GetSchemaName();
    string moduleName = customTableConfig->GetPluginName();
    Module* module = mPluginManager->getModule(moduleName);
    if (!module) {
        IE_LOG(ERROR, "cannot find module [%s]", moduleName.c_str());
        return false;
    }
    mTableFactory = dynamic_cast<TableFactory*>(module->getModuleFactory(string(TableFactory::TABLE_FACTORY_NAME)));
    if (!mTableFactory) {
        IE_LOG(ERROR, "get %s from module [%s] fail!", string(TableFactory::TABLE_FACTORY_NAME).c_str(),
               moduleName.c_str());
        return false;
    }
    return true;
}

TableWriterPtr TableFactoryWrapper::CreateTableWriter() const
{
    assert(mTableFactory);
    return TableWriterPtr(mTableFactory->CreateTableWriter(mParameters));
}

TableResourcePtr TableFactoryWrapper::CreateTableResource() const
{
    assert(mTableFactory);
    return TableResourcePtr(mTableFactory->CreateTableResource(mParameters));
}

MergePolicyPtr TableFactoryWrapper::CreateMergePolicy() const
{
    assert(mTableFactory);
    return MergePolicyPtr(mTableFactory->CreateMergePolicy(mParameters));
}

TableMergerPtr TableFactoryWrapper::CreateTableMerger() const
{
    assert(mTableFactory);
    return TableMergerPtr(mTableFactory->CreateTableMerger(mParameters));
}

TableReaderPtr TableFactoryWrapper::CreateTableReader() const
{
    assert(mTableFactory);
    return TableReaderPtr(mTableFactory->CreateTableReader(mParameters));
}

std::function<void()> TableFactoryWrapper::CreateCleanResourceTask() const
{
    assert(mTableFactory);
    return mTableFactory->CreateCleanResourceTask(mParameters);
}

const PluginManagerPtr& TableFactoryWrapper::GetPluginManager() const { return mPluginManager; }

ExecutorProviderPtr TableFactoryWrapper::CreateExecutorProvider() const
{
    assert(mTableFactory);
    return ExecutorProviderPtr(mTableFactory->CreateExecutorProvider(mParameters));
}
}} // namespace indexlib::table
