#ifndef __INDEXLIB_ON_DISK_SEGMENT_SIZE_CALCULATOR_H
#define __INDEXLIB_ON_DISK_SEGMENT_SIZE_CALCULATOR_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"

DECLARE_REFERENCE_CLASS(index_base, SegmentData);
DECLARE_REFERENCE_CLASS(index_base, SegmentFileMeta);
DECLARE_REFERENCE_CLASS(file_system, Directory);
DECLARE_REFERENCE_CLASS(config, IndexPartitionSchema);
DECLARE_REFERENCE_CLASS(config, IndexSchema);
DECLARE_REFERENCE_CLASS(config, AttributeSchema);
DECLARE_REFERENCE_CLASS(config, SummarySchema);

IE_NAMESPACE_BEGIN(index);

class OnDiskSegmentSizeCalculator
{
public:
    typedef std::map<std::string, int64_t> SizeInfoMap;
    
public:
    OnDiskSegmentSizeCalculator();
    virtual ~OnDiskSegmentSizeCalculator();
public:
    int64_t GetSegmentSize(const index_base::SegmentData& segmentData,
                           const config::IndexPartitionSchemaPtr& schema,
                           bool includePatch = false);
    int64_t CollectSegmentSizeInfo(const index_base::SegmentData& segmentData,
                                   const config::IndexPartitionSchemaPtr& schema,
                                   SizeInfoMap& sizeInfos);
private:
    int64_t CollectSegmentSizeInfo(const file_system::DirectoryPtr& segDirectory,
                                   const config::IndexPartitionSchemaPtr& schema,
                                   SizeInfoMap& sizeInfos) const;
    
    int64_t CollectPatchSegmentSizeInfo(
            const index_base::SegmentData& segmentData,
            const config::IndexPartitionSchemaPtr& schema,
            SizeInfoMap& sizeInfos) const;
    
    int64_t GetKeyValueSegmentSize(
        const index_base::SegmentData& segmentData,
        const config::IndexPartitionSchemaPtr& schema,
        SizeInfoMap& sizeInfos) const;

    int64_t GetCustomTableSegmentSize(
        const index_base::SegmentData& segmentData) const;

    int64_t GetKvSegmentSize(
        const file_system::DirectoryPtr& dir,
        const config::IndexSchemaPtr& indexSchema,
        const index_base::SegmentFileMetaPtr& meta,
        const std::string& path) const;

    int64_t GetKkvSegmentSize(
        const file_system::DirectoryPtr& dir,
        const config::IndexSchemaPtr& indexSchema,
        const index_base::SegmentFileMetaPtr& meta,
        const std::string& path) const;

    int64_t GetSegmentIndexSize(
            const file_system::DirectoryPtr& segDirectory,
            const config::IndexSchemaPtr& indexSchema,
            const index_base::SegmentFileMetaPtr& meta,
            SizeInfoMap& sizeInfos) const;
    
    int64_t GetSegmentAttributeSize(
            const file_system::DirectoryPtr& segDirectory,
            const config::AttributeSchemaPtr& attrSchema,
            const index_base::SegmentFileMetaPtr& meta,
            SizeInfoMap& sizeInfos) const;
    
    int64_t GetSegmentSummarySize(
            const file_system::DirectoryPtr& segDirectory,
            const config::SummarySchemaPtr& summarySchema,
            const index_base::SegmentFileMetaPtr& meta) const;

    int64_t GetFileLength(const file_system::DirectoryPtr& directory,
                          const index_base::SegmentFileMetaPtr& meta,
                          const std::string& path,
                          const std::string& fileName) const;
    
    int64_t GetPackIndexSize(const file_system::DirectoryPtr& indexDirectory,
                             const index_base::SegmentFileMetaPtr& meta,
                             const std::string& indexName) const;
    
    int64_t GetTextIndexSize(const file_system::DirectoryPtr& indexDirectory,
                             const index_base::SegmentFileMetaPtr& meta,
                             const std::string& indexName) const;
    int64_t GetRangeIndexSize(const file_system::DirectoryPtr& indexDirectory,
                              const index_base::SegmentFileMetaPtr& meta,
                              const std::string& indexName) const;

    int64_t GetCustomizedIndexSize(const file_system::DirectoryPtr& indexDirectory,
                                   const index_base::SegmentFileMetaPtr& meta,
                                   const std::string& indexName) const;

    int64_t GetPKIndexSize(const file_system::DirectoryPtr& indexDirectory,
                           const index_base::SegmentFileMetaPtr& meta,
                           const std::string& indexName,
                           const std::string& fieldName) const;

    int64_t CalculateSingleAttributeSize(
        const file_system::DirectoryPtr& directory,
        const index_base::SegmentFileMetaPtr& meta,
        const std::string& attrName) const;

    int64_t CalculateSingleAttributePatchSize(
        const file_system::DirectoryPtr& directory,
        const index_base::SegmentFileMetaPtr& meta,
        const std::string& attrName) const;    

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(OnDiskSegmentSizeCalculator);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_ON_DISK_SEGMENT_SIZE_CALCULATOR_H
