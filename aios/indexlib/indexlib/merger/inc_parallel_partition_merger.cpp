#include "indexlib/merger/inc_parallel_partition_merger.h"
#include "indexlib/storage/file_system_wrapper.h"
#include "indexlib/merger/sorted_index_partition_merger.h"
#include "indexlib/merger/parallel_partition_data_merger.h"
#include "indexlib/file_system/indexlib_file_system.h"
#include "indexlib/file_system/indexlib_file_system_impl.h"
#include "indexlib/util/memory_control/memory_quota_controller_creator.h"
#include "indexlib/index_base/version_committer.h"
#include "indexlib/table/table_plugin_loader.h"
#include "indexlib/table/table_factory_wrapper.h"
#include "indexlib/plugin/plugin_manager.h"
#include "indexlib/plugin/index_plugin_loader.h"
#include "indexlib/merger/custom_partition_merger.h"

using namespace std;
IE_NAMESPACE_USE(misc);
IE_NAMESPACE_USE(storage);
IE_NAMESPACE_USE(common);
IE_NAMESPACE_USE(index_base);
IE_NAMESPACE_USE(file_system);
IE_NAMESPACE_USE(util);
IE_NAMESPACE_USE(table);
IE_NAMESPACE_USE(plugin);
IE_NAMESPACE_BEGIN(merger);
IE_LOG_SETUP(merger, IncParallelPartitionMerger);

IncParallelPartitionMerger::IncParallelPartitionMerger(
    const IndexPartitionMergerPtr &merger,
    const vector<string> &srcPaths,
    const SortDescriptions &sortDesc,
    MetricProviderPtr metricProvider,
    const plugin::PluginManagerPtr &pluginManager,
    const PartitionRange &targetRange)
    : mMerger(merger)
    , mSrcPaths(srcPaths)
    , mSortDesc(sortDesc)
    , mMetricProvider(metricProvider)
{
    IndexPartitionMerger::mPartitionRange = targetRange;
    mPluginManager = pluginManager;
}

IncParallelPartitionMerger::~IncParallelPartitionMerger()
{
}

bool IncParallelPartitionMerger::hasOngongingModifyOperationsChange(
        const index_base::Version& version, const config::IndexPartitionOptions& options) {
    auto versionOpIds = version.GetOngoingModifyOperations();
    sort(versionOpIds.begin(), versionOpIds.end());
    auto targetOpIds = options.GetOngoingModifyOperationIds();
    sort(targetOpIds.begin(), targetOpIds.end());
    if (versionOpIds.size() != targetOpIds.size()) {
        return true;
    }
    for (size_t i = 0; i < versionOpIds.size(); i++) {
        if (versionOpIds[i] != targetOpIds[i]) {
            return true;
        }
    }
    return false;
}

void IncParallelPartitionMerger::PrepareMerge(int64_t currentTs)
{
    string destPath = mMerger->GetMergeIndexRoot();
    auto schema = mMerger->GetSchema();
    // reuse FileSystem
    IndexlibFileSystemPtr fileSystem(
            new IndexlibFileSystemImpl(destPath));
    FileSystemOptions fsOptions;
    fsOptions.enableAsyncFlush = false;
    fsOptions.useCache = false;
    fsOptions.memoryQuotaController =
        MemoryQuotaControllerCreator::CreatePartitionMemoryController();
    fsOptions.enablePathMetaContainer = true;
    fsOptions.isOffline = true;
    fileSystem->Init(fsOptions);
    
    ParallelPartitionDataMerger dataMerger(destPath, schema,
            mMerger->GetOptions(), fileSystem);
    Version version = dataMerger.MergeSegmentData(mSrcPaths);
    auto options = mMerger->GetOptions();
    if (hasOngongingModifyOperationsChange(version, options)) {
        version.SetVersionId(version.GetVersionId() + 1);
        version.SyncSchemaVersionId(schema);
        version.SetOngoingModifyOperations(options.GetOngoingModifyOperationIds());
        index_base::VersionCommitter committer(destPath, version, INVALID_KEEP_VERSION_COUNT);
        committer.Commit();
    }
    SegmentDirectoryPtr segDir(new SegmentDirectory(destPath, version));
    bool needDeletionMap = schema->GetTableType() == tt_index;
    bool hasSub = false;
    if (schema->GetSubIndexPartitionSchema())
    {
        hasSub = true;
    }
    segDir->Init(hasSub, needDeletionMap, fileSystem);
    if (schema->GetTableType() == tt_customized) {
        TableFactoryWrapperPtr tableFactory(
            new TableFactoryWrapper(schema, options, mPluginManager));
        auto targetRange = mMerger->GetPartitionRange();
        if (!tableFactory->Init()) {
            INDEXLIB_FATAL_ERROR(Runtime,
                                 "init table factory failed for schema[%s]",
                                 schema->GetSchemaName().c_str());
        }
        mMerger.reset(new CustomPartitionMerger(segDir, schema, options,
                                                DumpStrategyPtr(), mMetricProvider, tableFactory, targetRange));
    } else {
        if (mSortDesc.size() > 0) {
            mMerger.reset(new SortedIndexPartitionMerger(
                              segDir, schema, options, mSortDesc, DumpStrategyPtr(),
                              mMetricProvider, mPluginManager));
        } else {
            mMerger.reset(
                new IndexPartitionMerger(segDir, schema, options, DumpStrategyPtr(),
                                         mMetricProvider, mPluginManager));
        }
    }
    mMerger->PrepareMerge(currentTs);
}

void IncParallelPartitionMerger::EndMerge(const MergeMetaPtr& mergeMeta,
        versionid_t alignVersionId)
{
    mMerger->EndMerge(mergeMeta, alignVersionId);

    string destPath = mMerger->GetMergeIndexRoot();
    IndexlibFileSystemPtr fileSystem(
            new IndexlibFileSystemImpl(destPath));
    FileSystemOptions fsOptions;
    fsOptions.enableAsyncFlush = false;
    fsOptions.useCache = false;
    fsOptions.memoryQuotaController =
        MemoryQuotaControllerCreator::CreatePartitionMemoryController();
    fsOptions.enablePathMetaContainer = true;
    fsOptions.isOffline = true;
    fileSystem->Init(fsOptions);
    
    auto schema = mMerger->GetSchema();
    ParallelPartitionDataMerger dataMerger(destPath, schema,
            mMerger->GetOptions(), fileSystem);
    dataMerger.RemoveParallBuildDirectory();
}

IE_NAMESPACE_END(merger);

