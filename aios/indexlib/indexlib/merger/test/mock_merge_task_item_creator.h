#ifndef __INDEXLIB_MOCKMERGETASKITEMCREATOR_H
#define __INDEXLIB_MOCKMERGETASKITEMCREATOR_H

#include "indexlib/common_define.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/merger/merge_task_item_creator.h"
#include "indexlib/index/normal/summary/local_disk_summary_merger.h"
#include "indexlib/index/normal/attribute/accessor/attribute_merger.h"
#include "indexlib/index/normal/inverted_index/accessor/index_merger.h"
#include "indexlib/index_base/merge_task_resource_manager.h"
#include "indexlib/config/summary_config.h"

IE_NAMESPACE_BEGIN(merger);

class MockMergeTaskItemCreator : public MergeTaskItemCreator
{
public:
    MockMergeTaskItemCreator(IndexMergeMeta* mergeMeta,
                             const merger::SegmentDirectoryPtr& segDir,
                             const merger::SegmentDirectoryPtr& subSegDir,
                             const config::IndexPartitionSchemaPtr& schema,
                             const plugin::PluginManagerPtr& pluginManager,
                             const config::MergeConfig& mergeConfig,
                             index_base::MergeTaskResourceManagerPtr& resMgr)
        : MergeTaskItemCreator(mergeMeta, segDir, subSegDir, schema,
                               pluginManager, mergeConfig, resMgr)
    {
    }
public:
    index::SummaryMergerPtr CreateSummaryMerger(
        const config::SummarySchemaPtr &summarySchema,
        const std::string& summaryGroupName) const override
    {
        return fakeSummaryMerger;
    }
    index::AttributeMergerPtr CreateAttributeMerger(
        const config::AttributeSchemaPtr &schema,
        const std::string &attrName,
        bool isPackAttribute, bool isOptimize) const override
    {
        return fakeAttributeMerger;
    }
    index::IndexMergerPtr CreateIndexMerger(
        const config::IndexConfigPtr& indexConfig) const override
    {
        return fakeIndexMerger;
    }
 
public:
    index::SummaryMergerPtr fakeSummaryMerger;
    index::AttributeMergerPtr fakeAttributeMerger;
    index::IndexMergerPtr fakeIndexMerger;
};

IE_NAMESPACE_END(merger);

#endif //__INDEXLIB_MOCKMERGETASKITEMCREATOR_H
