#ifndef __INDEXLIB_TRUNCATE_INDEX_WRITER_CREATOR_H
#define __INDEXLIB_TRUNCATE_INDEX_WRITER_CREATOR_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/index/normal/inverted_index/truncate/bucket_map.h"
#include "indexlib/index_base/index_meta/output_segment_merge_info.h"
#include "indexlib/index/normal/reclaim_map/reclaim_map.h"
#include "indexlib/config/merge_io_config.h"

DECLARE_REFERENCE_CLASS(config, IndexConfig);
DECLARE_REFERENCE_CLASS(config, IndexPartitionSchema);
DECLARE_REFERENCE_CLASS(config, TruncateOptionConfig);
DECLARE_REFERENCE_CLASS(config, TruncateIndexProperty);
DECLARE_REFERENCE_CLASS(config, MergeConfig);
DECLARE_REFERENCE_CLASS(index, TruncateIndexWriter);
DECLARE_REFERENCE_CLASS(index, DocInfoAllocator);
DECLARE_REFERENCE_CLASS(index, TruncateMetaReader);
DECLARE_REFERENCE_CLASS(index, BucketMap);
DECLARE_REFERENCE_CLASS(index, BucketVectorAllocator);
DECLARE_REFERENCE_CLASS(index, TruncateAttributeReaderCreator);
DECLARE_REFERENCE_CLASS(storage, ArchiveFolder);
DECLARE_REFERENCE_CLASS(storage, FileWrapper);

IE_NAMESPACE_BEGIN(index);

class TruncateIndexWriterCreator
{
public:
    TruncateIndexWriterCreator(bool optimize = false);
    ~TruncateIndexWriterCreator();

public:
    void Init(const config::IndexPartitionSchemaPtr& schema, 
              const config::MergeConfig& mergeConfig,
              const index_base::OutputSegmentMergeInfos& outputSegmentMergeInfos,
              const ReclaimMapPtr& reclaimMap,
              const storage::ArchiveFolderPtr& truncateMetaRoot,
              TruncateAttributeReaderCreator *truncateAttrCreator,
              const BucketMaps *bucketMaps,
              int64_t beginTimeSecond);

    TruncateIndexWriterPtr Create(
            const config::IndexConfigPtr& indexConfig,
            const config::MergeIOConfig& ioConfig);

private:
    TruncateIndexWriterPtr CreateSingleIndexWriter(
            const config::TruncateIndexProperty& truncateIndexProperty, 
            const config::IndexConfigPtr& indexConfig,
            const BucketVectorAllocatorPtr& bucketVecAllocator, 
            const config::MergeIOConfig& ioConfig);

    DocInfoAllocatorPtr CreateDocInfoAllocator(
            const config::TruncateIndexProperty& truncateIndexProperty);

    bool GetReferenceFields(
            std::vector<std::string> &fieldNames,
            const config::TruncateIndexProperty& truncateIndexProperty);

    bool CheckReferenceField(const std::string &fieldName);

    void DeclareReference(const std::string& fieldName, 
                          const DocInfoAllocatorPtr& allocator);

    TruncateMetaReaderPtr CreateMetaReader(
            const config::TruncateIndexProperty& truncateIndexProperty, 
            const storage::FileWrapperPtr &metaFilePath);

    int64_t GetBaseTime(int64_t beginTime);
    bool NeedTruncMeta(const config::TruncateIndexProperty& truncateIndexProperty);
    bool CheckBitmapConfigValid(const config::IndexConfigPtr& indexConfig,
                                const config::TruncateIndexProperty& truncateIndexProperty);
private:
    typedef std::map<std::string, DocInfoAllocatorPtr> AllocatorMap;
    config::TruncateOptionConfigPtr mTruncOptionConfig;
    TruncateAttributeReaderCreator *mTruncateAttrCreator;
    //std::string mIndexDumpPath;
    std::string mTruncateMetaDir;
    AllocatorMap mAllocators;
    const BucketMaps *mBucketMaps;
    bool mIsOptimize;
    //
    index_base::OutputSegmentMergeInfos mOutputSegmentMergeInfos;
    ReclaimMapPtr mReclaimMap;
    storage::ArchiveFolderPtr mTruncateMetaFolder;

private:
    friend class TruncateIndexWriterCreatorTest;

private:
    IE_LOG_DECLARE();
};


DEFINE_SHARED_PTR(TruncateIndexWriterCreator);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_TRUNCATE_INDEX_WRITER_CREATOR_H
