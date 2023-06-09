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
#include <map>
#include <memory>
#include <string>
#include <vector>

#include "autil/Log.h"
#include "autil/NoCopyable.h"
#include "indexlib/base/Status.h"
#include "indexlib/base/Types.h"
#include "indexlib/document/IDocument.h"
#include "indexlib/document/normal/ModifiedTokens.h"
#include "indexlib/index/common/Constant.h"
#include "indexlib/index/common/IndexerOrganizerMeta.h"
#include "indexlib/index/common/Types.h"
#include "indexlib/index/inverted_index/InvertedIndexMetrics.h"
#include "indexlib/index/inverted_index/config/InvertedIndexConfig.h"

namespace indexlibv2::framework {
class Segment;
} // namespace indexlibv2::framework
namespace indexlib::index {
class IInvertedDiskIndexer;
class IInvertedMemIndexer;

struct SingleInvertedIndexBuildInfoHolder {
    SingleInvertedIndexBuildInfoHolder(std::shared_ptr<indexlibv2::config::InvertedIndexConfig> _outerIndexConfig,
                                       std::shared_ptr<indexlibv2::config::InvertedIndexConfig> _innerIndexConfig,
                                       size_t _shardId)
    {
        outerIndexConfig = _outerIndexConfig;
        innerIndexConfig = _innerIndexConfig;
        shardId = _shardId;
        buildingIndexer = nullptr;
        singleFieldIndexMetrics = nullptr;
    }
    SingleInvertedIndexBuildInfoHolder()
    {
        outerIndexConfig = nullptr;
        innerIndexConfig = nullptr;
        shardId = INVALID_SHARDID;
        buildingIndexer = nullptr;
        singleFieldIndexMetrics = nullptr;
    }
    // In case of sharded inverted index, outerIndexConfig will be of type IST_NEED_SHARDING config, and
    // innterIndexConfig will be of type IST_IS_SHARDING config, shardId will be valid.
    // In case of non-sharded inverted index, outerIndexConfig will be of type IST_NO_SHARDING config, and
    // innerIndexConfig will be nullptr, shardId will be INVALID_SHARDID.
    std::shared_ptr<indexlibv2::config::InvertedIndexConfig> outerIndexConfig;
    std::shared_ptr<indexlibv2::config::InvertedIndexConfig> innerIndexConfig;
    size_t shardId;
    std::map<std::pair<docid_t, uint64_t>, std::shared_ptr<IInvertedDiskIndexer>> diskIndexers;
    std::map<std::pair<docid_t, uint64_t>, indexlibv2::framework::Segment*> diskSegments;
    std::map<std::pair<docid_t, uint64_t>, std::shared_ptr<IInvertedMemIndexer>> dumpingIndexers;
    std::shared_ptr<IInvertedMemIndexer> buildingIndexer;
    InvertedIndexMetrics::SingleFieldIndexMetrics* singleFieldIndexMetrics;
};

// TODO: Support metrics for inverted index here.
class InvertedIndexerOrganizerUtil : public autil::NoCopyable
{
public:
    InvertedIndexerOrganizerUtil() = default;
    ~InvertedIndexerOrganizerUtil() = default;

public:
    static Status UpdateOneIndexForReplay(docid_t docId, const IndexerOrganizerMeta& indexerOrganizerMeta,
                                          SingleInvertedIndexBuildInfoHolder* buildInfoHolder,
                                          const document::ModifiedTokens& modifiedTokens);

    static Status UpdateOneIndexForBuild(docid_t docId, const IndexerOrganizerMeta& indexerOrganizerMeta,
                                         SingleInvertedIndexBuildInfoHolder* buildInfoHolder,
                                         const document::ModifiedTokens& modifiedTokens);

    static bool ShouldSkipUpdateIndex(const indexlibv2::config::InvertedIndexConfig* indexConfig);
    static const std::vector<document::ModifiedTokens>& GetModifiedTokens(indexlibv2::document::IDocument* doc);

private:
    template <typename castedMultiShardType>
    static Status UpdateOneSegmentDiskIndex(SingleInvertedIndexBuildInfoHolder* buildInfoHolder, docid_t docId,
                                            const IndexerOrganizerMeta& indexerOrganizerMeta,
                                            const document::ModifiedTokens& modifiedTokens, size_t shardId);
    template <typename castedMultiShardType>
    static Status UpdateOneSegmentDumpingIndex(SingleInvertedIndexBuildInfoHolder* buildInfoHolder, docid_t docId,
                                               const IndexerOrganizerMeta& indexerOrganizerMeta,
                                               const document::ModifiedTokens& modifiedTokens, size_t shardId);

    static void UpdateBuildResourceMetrics(SingleInvertedIndexBuildInfoHolder* buildInfoHolder);

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlib::index
