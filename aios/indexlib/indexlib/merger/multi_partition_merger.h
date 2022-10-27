#ifndef __INDEXLIB_MULTI_PARTITION_MERGER_H
#define __INDEXLIB_MULTI_PARTITION_MERGER_H

#include <tr1/memory>
#include "indexlib/misc/metric_provider.h"
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/config/index_partition_options.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/merger/index_partition_merger.h"
#include "indexlib/merger/multi_part_segment_directory.h"

DECLARE_REFERENCE_CLASS(plugin, PluginManager);
IE_NAMESPACE_BEGIN(merger);
class MultiPartitionMerger
{
public:
    typedef std::vector<std::string> StringVec;
    typedef std::vector<SortPattern> SortPatternVec;
public:
    MultiPartitionMerger(const config::IndexPartitionOptions& options,
                         misc::MetricProviderPtr metricProvider,
                         const std::string& indexPluginPath);

    virtual ~MultiPartitionMerger();

public:
    /** 
     * Merge multi partition index.
     * 
     * @param mergeSrc, the file system where the index partitions to merge
     * are stored.
     * @param mergeDest, the file system dirctory where the index partition
     * to be stored.
     * 
     * @exception throw BadParameterException or FileIOException if failed.
     */
    void Merge(const std::vector<std::string>& mergeSrc,
               const std::string& mergeDest);

    virtual IndexPartitionMergerPtr CreatePartitionMerger(
            const std::vector<std::string>& mergeSrc,
            const std::string& mergeDest);

protected:
    virtual void GetFirstVersion(const std::string& partPath, index_base::Version& version);
    void CreateDestPath(const std::string& destPath,
                        const config::IndexPartitionSchemaPtr& schema,
                        const index_base::PartitionMeta &partMeta);
    
    virtual void CheckSrcPath(const std::vector<std::string>& mergeSrc, 
                              const config::IndexPartitionSchemaPtr& schema,
                              const index_base::PartitionMeta &partMeta,
                              std::vector<index_base::Version>& versions);
    void CheckPartitionConsistence(const std::string& rootDir,
                                   const config::IndexPartitionSchemaPtr& schema, 
                                   const index_base::PartitionMeta &partMeta);

    void CheckSchema(const std::string& rootDir,
                     const config::IndexPartitionSchemaPtr& schema);
    
    void CheckIndexFormat(const std::string& rootDir);

    void CheckPartitionMeta(const std::string& rootDir, 
                            const index_base::PartitionMeta &partMeta);
protected:
    config::IndexPartitionOptions mOptions;
    misc::MetricProviderPtr mMetricProvider;
    std::string mIndexPluginPath;
    MultiPartSegmentDirectoryPtr mSegmentDirectory;
    IE_NAMESPACE(config)::IndexPartitionSchemaPtr mSchema;
    
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(MultiPartitionMerger);

IE_NAMESPACE_END(merger);

#endif //__INDEXLIB_MULTI_PARTITION_MERGER_H
