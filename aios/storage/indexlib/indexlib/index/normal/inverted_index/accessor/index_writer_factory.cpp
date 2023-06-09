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
#include "indexlib/index/normal/inverted_index/accessor/index_writer_factory.h"

#include <sstream>

#include "indexlib/framework/SegmentMetrics.h"
#include "indexlib/index/normal/framework/index_writer.h"
#include "indexlib/index/normal/inverted_index/accessor/index_format_writer_creator.h"
#include "indexlib/index/normal/inverted_index/accessor/multi_sharding_index_writer.h"
#include "indexlib/index/normal/inverted_index/accessor/normal_index_writer.h"
#include "indexlib/index/normal/inverted_index/accessor/section_attribute_writer.h"
#include "indexlib/index/normal/inverted_index/builtin_index/date/date_index_writer.h"
#include "indexlib/index/normal/inverted_index/builtin_index/dynamic/dynamic_index_writer.h"
#include "indexlib/index/normal/inverted_index/builtin_index/range/range_index_writer.h"
#include "indexlib/index/normal/inverted_index/customized_index/customized_index_writer.h"
#include "indexlib/index/normal/primarykey/primary_key_writer_typed.h"
#include "indexlib/index/normal/trie/trie_index_writer.h"
#include "indexlib/util/Exception.h"

namespace indexlib { namespace index {
IE_LOG_SETUP(index, IndexWriterFactory);

size_t IndexWriterFactory::EstimateIndexWriterInitMemUse(const config::IndexConfigPtr& indexConfig,
                                                         const config::IndexPartitionOptions& options,
                                                         const plugin::PluginManagerPtr& pluginManager,
                                                         std::shared_ptr<framework::SegmentMetrics> segmentMetrics,
                                                         const index_base::PartitionSegmentIteratorPtr& segIter)
{
    std::unique_ptr<IndexWriter> indexWriter =
        CreateIndexWriterWithoutInit(INVALID_SEGMENTID, indexConfig, segmentMetrics, options,
                                     /*buildResourceMetrics=*/nullptr, pluginManager, segIter);
    return indexWriter->EstimateInitMemUse(indexConfig, segIter);
}

std::unique_ptr<index::IndexWriter>
IndexWriterFactory::ConstructIndexWriter(segmentid_t segmentId, const config::IndexConfigPtr& indexConfig,
                                         std::shared_ptr<framework::SegmentMetrics> segmentMetrics,
                                         const config::IndexPartitionOptions& options)
{
    assert(indexConfig);
    InvertedIndexType indexType = indexConfig->GetInvertedIndexType();
    std::string indexName = InvertedIndexUtil::GetIndexName(indexConfig);
    std::unique_ptr<IndexWriter> indexWriter = nullptr;
    size_t lastSegmentDistinctTermCount = 0;
    if (indexType != it_range) {
        lastSegmentDistinctTermCount = segmentMetrics->GetDistinctTermCount(indexName);
    }
    switch (indexType) {
    case it_string:
    case it_spatial:
    case it_text:
    case it_pack:
    case it_number:
    case it_number_int8:
    case it_number_uint8:
    case it_number_int16:
    case it_number_uint16:
    case it_number_int32:
    case it_number_uint32:
    case it_number_int64:
    case it_number_uint64:
    case it_expack:
        indexWriter = std::make_unique<NormalIndexWriter>(segmentId, lastSegmentDistinctTermCount, options);
        break;
    case it_primarykey64:
        indexWriter = std::make_unique<UInt64PrimaryKeyWriterTyped>();
        break;
    case it_primarykey128:
        indexWriter = std::make_unique<UInt128PrimaryKeyWriterTyped>();
        break;
    case it_trie:
        indexWriter = std::make_unique<TrieIndexWriter>();
        break;
    case it_datetime:
        indexWriter = std::make_unique<DateIndexWriter>(lastSegmentDistinctTermCount, options);
        break;
    case it_range:
        indexWriter = std::make_unique<RangeIndexWriter>(indexName, segmentMetrics, options);
        break;
    default: {
        std::stringstream s;
        s << "Index writer type: " << config::IndexConfig::InvertedIndexTypeToStr(indexType) << " not implemented yet!";
        INDEXLIB_THROW(util::UnImplementException, "%s", s.str().c_str());
    } break;
    }
    return indexWriter;
}

std::unique_ptr<index::IndexWriter> IndexWriterFactory::CreateIndexWriter(
    segmentid_t segmentId, const config::IndexConfigPtr& indexConfig,
    std::shared_ptr<framework::SegmentMetrics> segmentMetrics, const config::IndexPartitionOptions& options,
    util::BuildResourceMetrics* buildResourceMetrics, const plugin::PluginManagerPtr& pluginManager,
    const index_base::PartitionSegmentIteratorPtr& segIter)
{
    config::IndexConfig::IndexShardingType shardingType = indexConfig->GetShardingType();
    if (shardingType == config::IndexConfig::IndexShardingType::IST_NO_SHARDING or
        shardingType == config::IndexConfig::IndexShardingType::IST_IS_SHARDING) {
        std::unique_ptr<index::IndexWriter> indexWriter = CreateIndexWriterWithoutInit(
            segmentId, indexConfig, segmentMetrics, options, buildResourceMetrics, pluginManager, segIter);
        if (indexConfig->GetInvertedIndexType() == it_customized) {
            auto customizedIndexWriter = static_cast<CustomizedIndexWriter*>(indexWriter.get());
            customizedIndexWriter->CustomizedInit(indexConfig, buildResourceMetrics, segIter);
        } else {
            indexWriter->Init(indexConfig, buildResourceMetrics);
        }
        return indexWriter;
    }

    assert(shardingType == config::IndexConfig::IndexShardingType::IST_NEED_SHARDING);
    auto multiShardingIndexWriter = std::make_unique<MultiShardingIndexWriter>(segmentId, segmentMetrics, options);
    multiShardingIndexWriter->Init(indexConfig, buildResourceMetrics);
    return multiShardingIndexWriter;
}

std::unique_ptr<index::IndexWriter> IndexWriterFactory::CreateIndexWriterWithoutInit(
    segmentid_t segmentId, const config::IndexConfigPtr& indexConfig,
    std::shared_ptr<framework::SegmentMetrics> segmentMetrics, const config::IndexPartitionOptions& options,
    util::BuildResourceMetrics* buildResourceMetrics, const plugin::PluginManagerPtr& pluginManager,
    const index_base::PartitionSegmentIteratorPtr& segIter)
{
    assert(indexConfig);
    config::IndexConfig::IndexShardingType shardingType = indexConfig->GetShardingType();
    if (shardingType == config::IndexConfig::IndexShardingType::IST_NO_SHARDING or
        shardingType == config::IndexConfig::IndexShardingType::IST_IS_SHARDING) {
        if (indexConfig->GetInvertedIndexType() == it_customized) {
            assert(shardingType != config::IndexConfig::IndexShardingType::IST_NEED_SHARDING);
            size_t lastSegmentDistinctTermCount = segmentMetrics->GetDistinctTermCount(indexConfig->GetIndexName());
            return std::make_unique<CustomizedIndexWriter>(pluginManager, lastSegmentDistinctTermCount,
                                                           /*ignoreBuildError=*/options.IsOnline());
        }
        return ConstructIndexWriter(segmentId, indexConfig, segmentMetrics, options);
    }
    assert(shardingType == config::IndexConfig::IndexShardingType::IST_NEED_SHARDING);
    return std::make_unique<MultiShardingIndexWriter>(segmentId, segmentMetrics, options);
}

// Return true if the given indexConfig needs to use SectionAttributeWriter to build.
bool IndexWriterFactory::NeedSectionAttributeWriter(const config::IndexConfigPtr& indexConfig)
{
    LegacyIndexFormatOption indexFormatOption;
    indexFormatOption.Init(indexConfig);
    return indexFormatOption.HasSectionAttribute();
}

std::unique_ptr<index::SectionAttributeWriter>
IndexWriterFactory::CreateSectionAttributeWriter(const config::IndexConfigPtr& indexConfig,
                                                 util::BuildResourceMetrics* buildResourceMetrics)
{
    std::shared_ptr<LegacyIndexFormatOption> indexFormatOption(new LegacyIndexFormatOption);
    indexFormatOption->Init(indexConfig);
    return legacy::IndexFormatWriterCreator::CreateSectionAttributeWriter(indexConfig, indexFormatOption,
                                                                          buildResourceMetrics);
}

}} // namespace indexlib::index
