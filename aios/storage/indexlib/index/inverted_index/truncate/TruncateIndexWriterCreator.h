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
#pragma once
#include <memory>

#include "indexlib/config/IIndexConfig.h"
#include "indexlib/file_system/fslib/IoConfig.h"
#include "indexlib/file_system/relocatable/RelocatableFolder.h"
#include "indexlib/index/DocMapper.h"
#include "indexlib/index/IIndexMerger.h"
#include "indexlib/index/inverted_index/config/DiversityConstrain.h"
#include "indexlib/index/inverted_index/config/InvertedIndexConfig.h"
#include "indexlib/index/inverted_index/config/TruncateIndexProperty.h"
#include "indexlib/index/inverted_index/config/TruncateOptionConfig.h"
#include "indexlib/index/inverted_index/truncate/BucketMap.h"
#include "indexlib/index/inverted_index/truncate/BucketVectorAllocator.h"
#include "indexlib/index/inverted_index/truncate/DocInfoAllocator.h"
#include "indexlib/index/inverted_index/truncate/TruncateAttributeReaderCreator.h"
#include "indexlib/index/inverted_index/truncate/TruncateIndexWriter.h"
#include "indexlib/index/inverted_index/truncate/TruncateMetaFileReaderCreator.h"
#include "indexlib/index/inverted_index/truncate/TruncateMetaReader.h"

namespace indexlib::index {

class TruncateIndexWriterCreator
{
public:
    TruncateIndexWriterCreator(const indexlibv2::index::IIndexMerger::SegmentMergeInfos& segmentMergeInfos,
                               bool optimize);
    ~TruncateIndexWriterCreator() = default;

public:
    void Init(const std::shared_ptr<indexlibv2::config::TruncateOptionConfig>& truncateOptionConfig,
              const std::shared_ptr<indexlibv2::index::DocMapper>& docMapper,
              const std::shared_ptr<TruncateAttributeReaderCreator>& truncateAttrCreator, const BucketMaps& bucketMaps,
              const std::shared_ptr<TruncateMetaFileReaderCreator>& truncateMetaFileReaderCreator,
              int64_t srcVersionTimestamp);

    std::shared_ptr<TruncateIndexWriter> Create(const std::shared_ptr<indexlibv2::config::IIndexConfig>& indexConfig,
                                                const file_system::IOConfig& ioConfig);

private:
    std::shared_ptr<TruncateIndexWriter>
    CreateSingleIndexWriter(const indexlibv2::config::TruncateIndexProperty& truncateIndexProperty,
                            const std::shared_ptr<indexlibv2::config::InvertedIndexConfig>& indexConfig,
                            const std::shared_ptr<BucketVectorAllocator>& bucketVecAllocator,
                            const file_system::IOConfig& ioConfig);

    std::shared_ptr<DocInfoAllocator>
    CreateDocInfoAllocator(const indexlibv2::config::TruncateIndexProperty& truncateIndexProperty);

    bool GetReferenceFields(const indexlibv2::config::TruncateIndexProperty& truncateIndexProperty,
                            std::vector<std::string>& fieldNames) const;
    bool GetReferenceFieldsFromConstrain(const indexlibv2::config::DiversityConstrain& constrain,
                                         std::vector<std::string>* fieldNames) const;

    bool CheckReferenceField(const std::string& fieldName) const;

    void DeclareReference(const std::string& fieldName, const std::shared_ptr<DocInfoAllocator>& allocator);

    std::pair<Status, std::shared_ptr<TruncateMetaReader>>
    CreateMetaReader(const indexlibv2::config::TruncateIndexProperty& truncateIndexProperty,
                     const std::shared_ptr<file_system::FileReader>& metaFilePath);

    int64_t GetBaseTime(int64_t beginTime);
    bool NeedTruncMeta(const indexlibv2::config::TruncateIndexProperty& truncateIndexProperty) const;
    bool CheckBitmapConfigValid(const std::shared_ptr<indexlibv2::config::InvertedIndexConfig>& indexConfig,
                                const indexlibv2::config::TruncateIndexProperty& truncateIndexProperty) const;

private:
    const indexlibv2::index::IIndexMerger::SegmentMergeInfos _segmentMergeInfos;
    const bool _isOptimize;
    std::shared_ptr<indexlibv2::config::TruncateOptionConfig> _truncateOptionConfig;
    std::shared_ptr<TruncateAttributeReaderCreator> _truncateAttributeReaderCreator;
    std::map<std::string, std::shared_ptr<DocInfoAllocator>> _allocators;
    BucketMaps _bucketMaps;

    std::shared_ptr<indexlibv2::index::DocMapper> _docMapper;
    std::shared_ptr<TruncateMetaFileReaderCreator> _truncateMetaFileReaderCreator;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlib::index
