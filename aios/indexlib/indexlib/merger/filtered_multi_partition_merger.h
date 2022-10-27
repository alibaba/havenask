#ifndef __INDEXLIB_FILTERED_MULTI_PARTITION_MERGER_H
#define __INDEXLIB_FILTERED_MULTI_PARTITION_MERGER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/misc/log.h"
#include "indexlib/misc/common.h"
#include "indexlib/merger/multi_partition_merger.h"
#include "indexlib/merger/doc_filter_creator.h"
#include "indexlib/merger/merge_meta.h"
#include "indexlib/index_base/segment/multi_part_segment_directory.h"

IE_NAMESPACE_BEGIN(merger);

class FilteredMultiPartitionMerger : public MultiPartitionMerger
{
public:
    FilteredMultiPartitionMerger(const config::IndexPartitionOptions& options,
                         misc::MetricProviderPtr metricProvider,
                         const std::string& indexPluginPath,
                         const DocFilterCreatorPtr& docFilterCreator);
    ~FilteredMultiPartitionMerger();
    bool Init(const std::vector<std::string>& mergeSrc,
              versionid_t version,
              const std::string& mergeDest);
    IE_NAMESPACE(merger)::MergeMetaPtr CreateMergeMeta(
        int32_t parallelNum, int64_t timestampInSecond);
    std::string GetMergeMetaDir();
    MergeMetaPtr LoadMergeMeta(const std::string& mergeMetaDir, bool onlyLoadBasicInfo) {
        return mPartitionMerger->LoadMergeMeta(mergeMetaDir, onlyLoadBasicInfo);
    }
    void DoMerge(const IE_NAMESPACE(merger)::MergeMetaPtr& mergeMeta, int32_t instanceId);
    void EndMerge(const IE_NAMESPACE(merger)::MergeMetaPtr& mergeMeta, versionid_t alignVersion);

private:
    void Merge(const std::vector<std::string>& mergeSrc,
               const std::string& mergeDest);
    
    /* IndexPartitionMergerPtr CreatePartitionMerger( */
    /*     const std::vector<std::string>& mergeSrc, */
    /*     const std::string& mergeDest); */
    void GetFirstVersion(const std::string& partPath, index_base::Version& version) override;
    void CheckSrcPath(const std::vector<std::string>& mergeSrc, 
                      const config::IndexPartitionSchemaPtr& schema,
                      const index_base::PartitionMeta &partMeta,
                      std::vector<index_base::Version>& versions) override;

    void RewriteLoadConfigs(config::IndexPartitionOptions& options);
    int64_t getCacheBlockSize();

private:
    DocFilterCreatorPtr mDocFilterCreator;
    std::vector<std::string> mMergeSrcs;
    std::string mDestPath;
    versionid_t mTargetVersion;
    IndexPartitionMergerPtr mPartitionMerger;
    
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(FilteredMultiPartitionMerger);

IE_NAMESPACE_END(merger);

#endif //__INDEXLIB_FILTERED_MULTI_PARTITION_MERGER_H
