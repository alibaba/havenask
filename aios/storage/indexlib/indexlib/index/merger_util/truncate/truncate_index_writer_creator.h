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
#ifndef __INDEXLIB_TRUNCATE_INDEX_WRITER_CREATOR_H
#define __INDEXLIB_TRUNCATE_INDEX_WRITER_CREATOR_H

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/config/merge_io_config.h"
#include "indexlib/index/merger_util/truncate/bucket_map.h"
#include "indexlib/index/util/reclaim_map.h"
#include "indexlib/index_base/index_meta/output_segment_merge_info.h"
#include "indexlib/indexlib.h"

DECLARE_REFERENCE_CLASS(config, IndexConfig);
DECLARE_REFERENCE_CLASS(config, IndexPartitionSchema);
DECLARE_REFERENCE_CLASS(config, TruncateOptionConfig);
DECLARE_REFERENCE_CLASS(config, TruncateIndexProperty);
DECLARE_REFERENCE_CLASS(config, DiversityConstrain);
DECLARE_REFERENCE_CLASS(config, MergeConfig);
DECLARE_REFERENCE_CLASS(index::legacy, TruncateIndexWriter);
DECLARE_REFERENCE_CLASS(index::legacy, DocInfoAllocator);
DECLARE_REFERENCE_CLASS(index::legacy, TruncateMetaReader);
DECLARE_REFERENCE_CLASS(index::legacy, BucketMap);
DECLARE_REFERENCE_CLASS(index::legacy, BucketVectorAllocator);
DECLARE_REFERENCE_CLASS(index::legacy, TruncateAttributeReaderCreator);
DECLARE_REFERENCE_CLASS(file_system, ArchiveFolder);
DECLARE_REFERENCE_CLASS(storage, FileWrapper);

namespace indexlib::index::legacy {

class TruncateIndexWriterCreator
{
public:
    TruncateIndexWriterCreator(bool optimize = false);
    ~TruncateIndexWriterCreator();

public:
    void Init(const config::IndexPartitionSchemaPtr& schema, const config::MergeConfig& mergeConfig,
              const index_base::OutputSegmentMergeInfos& outputSegmentMergeInfos, const ReclaimMapPtr& reclaimMap,
              const file_system::ArchiveFolderPtr& truncateMetaRoot,
              TruncateAttributeReaderCreator* truncateAttrCreator, const BucketMaps* bucketMaps,
              int64_t beginTimeSecond);

    TruncateIndexWriterPtr Create(const config::IndexConfigPtr& indexConfig, const config::MergeIOConfig& ioConfig);

private:
    TruncateIndexWriterPtr CreateSingleIndexWriter(const config::TruncateIndexProperty& truncateIndexProperty,
                                                   const config::IndexConfigPtr& indexConfig,
                                                   const BucketVectorAllocatorPtr& bucketVecAllocator,
                                                   const config::MergeIOConfig& ioConfig);

    DocInfoAllocatorPtr CreateDocInfoAllocator(const config::TruncateIndexProperty& truncateIndexProperty);

    bool GetReferenceFields(std::vector<std::string>& fieldNames,
                            const config::TruncateIndexProperty& truncateIndexProperty);
    bool GetReferenceFieldsFromConstrain(const config::DiversityConstrain& constrain,
                                         std::vector<std::string>* fieldNames);

    bool CheckReferenceField(const std::string& fieldName);

    void DeclareReference(const std::string& fieldName, const DocInfoAllocatorPtr& allocator);

    TruncateMetaReaderPtr CreateMetaReader(const config::TruncateIndexProperty& truncateIndexProperty,
                                           const file_system::FileReaderPtr& metaFilePath);

    int64_t GetBaseTime(int64_t beginTime);
    bool NeedTruncMeta(const config::TruncateIndexProperty& truncateIndexProperty);
    bool CheckBitmapConfigValid(const config::IndexConfigPtr& indexConfig,
                                const config::TruncateIndexProperty& truncateIndexProperty);

private:
    typedef std::map<std::string, DocInfoAllocatorPtr> AllocatorMap;
    config::TruncateOptionConfigPtr mTruncOptionConfig;
    TruncateAttributeReaderCreator* mTruncateAttrCreator;
    // std::string mIndexDumpPath;
    std::string mTruncateMetaDir;
    AllocatorMap mAllocators;
    const BucketMaps* mBucketMaps;
    bool mIsOptimize;
    //
    index_base::OutputSegmentMergeInfos mOutputSegmentMergeInfos;
    ReclaimMapPtr mReclaimMap;
    file_system::ArchiveFolderPtr mTruncateMetaFolder;

private:
    friend class TruncateIndexWriterCreatorTest;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(TruncateIndexWriterCreator);
} // namespace indexlib::index::legacy

#endif //__INDEXLIB_TRUNCATE_INDEX_WRITER_CREATOR_H
