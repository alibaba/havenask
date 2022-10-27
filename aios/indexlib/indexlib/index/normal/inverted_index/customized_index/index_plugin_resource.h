#ifndef __INDEXLIB_INDEX_PLUGIN_RESOURCE_H
#define __INDEXLIB_INDEX_PLUGIN_RESOURCE_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/plugin/plugin_manager.h"
#include "indexlib/index_base/index_meta/partition_meta.h"

IE_NAMESPACE_BEGIN(index);

class IndexPluginResource : public plugin::PluginResource
{
public:
    IndexPluginResource(const config::IndexPartitionSchemaPtr& _schema,
                        const config::IndexPartitionOptions& _options,
                        const util::CounterMapPtr& _counterMap,
                        const index_base::PartitionMeta& _partitionMeta,
                        const std::string& _pluginPath,
                        misc::MetricProviderPtr _metricProvider = nullptr
                        )
        : PluginResource(_schema, _options, _counterMap, _pluginPath, _metricProvider)
        , partitionMeta(_partitionMeta)
    {}
    ~IndexPluginResource() {}
    index_base::PartitionMeta partitionMeta; 
};

DEFINE_SHARED_PTR(IndexPluginResource);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_INDEX_PLUGIN_RESOURCE_H
