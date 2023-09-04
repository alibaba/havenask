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
#include "indexlib/index/inverted_index/SingleInvertedIndexBuilder.h"

#include "indexlib/index/common/IndexerOrganizerUtil.h"
#include "indexlib/index/inverted_index/InvertedDiskIndexer.h"
#include "indexlib/index/inverted_index/InvertedMemIndexer.h"
#include "indexlib/index/inverted_index/MultiShardInvertedDiskIndexer.h"
#include "indexlib/index/inverted_index/MultiShardInvertedMemIndexer.h"
namespace indexlib::index {
namespace {
using indexlibv2::framework::Segment;
}

AUTIL_LOG_SETUP(indexlib.index, SingleInvertedIndexBuilder);

SingleInvertedIndexBuilder::SingleInvertedIndexBuilder() {}

SingleInvertedIndexBuilder::~SingleInvertedIndexBuilder() {}

Status SingleInvertedIndexBuilder::AddDocument(indexlibv2::document::IDocument* doc)
{
    assert(doc->GetDocOperateType() == ADD_DOC);
    indexlibv2::document::extractor::IDocumentInfoExtractor* docInfoExtractor =
        _buildInfoHolder.buildingIndexer->GetDocInfoExtractor();

    document::IndexDocument* indexDocument = nullptr;
    Status st = docInfoExtractor->ExtractField(doc, (void**)&indexDocument);
    if (!st.IsOK()) {
        return st;
    }
    if (_buildInfoHolder.shardId != INVALID_SHARDID) {
        return std::dynamic_pointer_cast<MultiShardInvertedMemIndexer>(_buildInfoHolder.buildingIndexer)
            ->AddDocument(indexDocument, _buildInfoHolder.shardId);
    }
    return std::dynamic_pointer_cast<InvertedMemIndexer>(_buildInfoHolder.buildingIndexer)->AddDocument(indexDocument);
}

Status SingleInvertedIndexBuilder::UpdateDocument(indexlibv2::document::IDocument* doc)
{
    if (_shouldSkipUpdateIndex) {
        return Status::OK();
    }
    const std::vector<document::ModifiedTokens>& multiFieldModifiedTokens =
        InvertedIndexerOrganizerUtil::GetModifiedTokens(doc);
    for (const document::ModifiedTokens& modifiedTokens : multiFieldModifiedTokens) {
        if (!modifiedTokens.Valid()) { // IndexDocument.cpp 中有占位的 _modifiedTokens
            continue;
        }
        fieldid_t fieldId = modifiedTokens.FieldId();
        if (!_buildInfoHolder.outerIndexConfig->IsInIndex(fieldId)) {
            continue;
        }
        RETURN_STATUS_DIRECTLY_IF_ERROR(InvertedIndexerOrganizerUtil::UpdateOneIndexForBuild(
            doc->GetDocId(), _indexerOrganizerMeta, &_buildInfoHolder, modifiedTokens));
    }
    return Status::OK();
}

Status
SingleInvertedIndexBuilder::Init(const indexlibv2::framework::TabletData& tabletData,
                                 const std::shared_ptr<indexlibv2::config::InvertedIndexConfig>& outerIndexConfig,
                                 const std::shared_ptr<indexlibv2::config::InvertedIndexConfig>& innerIndexConfig,
                                 size_t shardId, bool isOnline)
{
    _shouldSkipUpdateIndex = InvertedIndexerOrganizerUtil::ShouldSkipUpdateIndex(outerIndexConfig.get());

    _buildInfoHolder.outerIndexConfig = outerIndexConfig;
    _buildInfoHolder.innerIndexConfig = innerIndexConfig;
    _buildInfoHolder.shardId = shardId;

    indexlib::index::IndexerOrganizerMeta::InitIndexerOrganizerMeta(tabletData, &_indexerOrganizerMeta);

    std::shared_ptr<InvertedIndexMetrics> indexMetrics;

    docid_t baseDocId = 0;
    auto slice = tabletData.CreateSlice();
    for (auto it = slice.begin(); it != slice.end(); it++) {
        Segment* segment = it->get();
        auto segStatus = segment->GetSegmentStatus();
        auto docCount = segment->GetSegmentInfo()->docCount;
        std::pair<docid_t, uint64_t> key(baseDocId, docCount);
        baseDocId += docCount;

        // Lazily init disk indexer. Current segment will load data once Segment::GetIndexer() is called.
        if (segStatus == Segment::SegmentStatus::ST_BUILT) {
            // MultiShardInvertedDiskIndexer's GetSingleShardIndexer requires indexer to be Open()'ed before it's
            // called. If segment doc count is 0, Open() will be skipped because of an optimization.
            if (docCount == 0) {
                continue;
            }
            if (!isOnline) {
                AUTIL_LOG(INFO, "Skip updating inverted index for disk segment in offline build");
                continue;
            }
            _buildInfoHolder.diskIndexers[key] = nullptr;
            _buildInfoHolder.diskSegments[key] = segment;
            continue;
        }

        std::shared_ptr<IInvertedMemIndexer> dumpingMemIndexer = nullptr;
        std::shared_ptr<IInvertedMemIndexer> buildingMemIndexer = nullptr;
        Status st = IndexerOrganizerUtil::GetIndexer<IInvertedDiskIndexer, IInvertedMemIndexer>(
            segment, _buildInfoHolder.outerIndexConfig, /*diskIndexer=*/nullptr, &dumpingMemIndexer,
            &buildingMemIndexer);
        RETURN_STATUS_DIRECTLY_IF_ERROR(st);
        if (dumpingMemIndexer == nullptr && buildingMemIndexer == nullptr) {
            continue;
        }
        if (segStatus == Segment::SegmentStatus::ST_DUMPING) {
            assert(dumpingMemIndexer != nullptr);
            _buildInfoHolder.dumpingIndexers[key] = dumpingMemIndexer;
        } else if (segStatus == Segment::SegmentStatus::ST_BUILDING) {
            assert(buildingMemIndexer != nullptr);
            _buildInfoHolder.buildingIndexer = buildingMemIndexer;
            indexMetrics = buildingMemIndexer->GetMetrics();
        } else {
            assert(false);
        }
    }
    if (indexMetrics != nullptr) {
        std::string metricsIndexName = GetIndexName();
        autil::StringUtil::replace(metricsIndexName, '@', '_');
        _buildInfoHolder.singleFieldIndexMetrics = indexMetrics->AddSingleFieldIndex(metricsIndexName);
    }
    return Status::OK();
}

std::string SingleInvertedIndexBuilder::GetIndexName() const
{
    if (_buildInfoHolder.shardId == INVALID_SHARDID) {
        return _buildInfoHolder.outerIndexConfig->GetIndexName();
    }
    if (_buildInfoHolder.innerIndexConfig == nullptr) {
        return _buildInfoHolder.outerIndexConfig->GetIndexName() + "_section";
    }
    return _buildInfoHolder.innerIndexConfig->GetIndexName();
}

} // namespace indexlib::index
