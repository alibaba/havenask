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
#include "indexlib/index/inverted_index/InplaceInvertedIndexModifier.h"

#include "indexlib/document/DocumentIterator.h"
#include "indexlib/document/normal/NormalDocument.h"
#include "indexlib/framework/Segment.h"
#include "indexlib/index/common/IndexerOrganizerUtil.h"
#include "indexlib/index/inverted_index/Common.h"
#include "indexlib/index/inverted_index/config/InvertedIndexConfig.h"

namespace indexlib ::index {
namespace {
using indexlibv2::config::FieldConfig;
using indexlibv2::config::IIndexConfig;
using indexlibv2::framework::Segment;
} // namespace

AUTIL_LOG_SETUP(index, InplaceInvertedIndexModifier);

InplaceInvertedIndexModifier::InplaceInvertedIndexModifier(
    const std::shared_ptr<indexlibv2::config::ITabletSchema>& schema)
    : InvertedIndexModifier(schema)
{
}

Status InplaceInvertedIndexModifier::Update(indexlibv2::document::IDocumentBatch* docBatch)
{
    auto iter = indexlibv2::document::DocumentIterator<indexlibv2::document::IDocument>::Create(docBatch);
    while (iter->HasNext()) {
        indexlibv2::document::IDocument* doc = iter->Next().get();
        const std::vector<document::ModifiedTokens>& multiFieldModifiedTokens =
            InvertedIndexerOrganizerUtil::GetModifiedTokens(doc);
        for (const auto& modifiedTokens : multiFieldModifiedTokens) {
            RETURN_STATUS_DIRECTLY_IF_ERROR(
                UpdateOneFieldTokens(doc->GetDocId(), modifiedTokens, /*isForReplay=*/false));
        }
    }
    return Status::OK();
}

Status InplaceInvertedIndexModifier::UpdateOneFieldTokens(docid_t docId, const document::ModifiedTokens& modifiedTokens,
                                                          bool isForReplay)
{
    if (!modifiedTokens.Valid()) { // IndexDocument.cpp 中有占位的 _modifiedTokens
        return Status::OK();
    }
    fieldid_t fieldId = modifiedTokens.FieldId();
    std::shared_ptr<FieldConfig> fieldConfig = _schema->GetFieldConfig(fieldId);
    if (fieldConfig == nullptr) {
        return Status::InvalidArgs("Field ID[%d] is invalid", fieldId);
    }
    std::vector<std::shared_ptr<IIndexConfig>> indexConfigs =
        _schema->GetIndexConfigsByFieldName(fieldConfig->GetFieldName());
    for (std::shared_ptr<IIndexConfig> iIndexConfig : indexConfigs) {
        assert(iIndexConfig);
        std::shared_ptr<indexlibv2::config::InvertedIndexConfig> indexConfig =
            std::dynamic_pointer_cast<indexlibv2::config::InvertedIndexConfig>(iIndexConfig);
        if (InvertedIndexerOrganizerUtil::ShouldSkipUpdateIndex(indexConfig.get())) {
            continue;
        }
        indexid_t indexId = indexConfig->GetIndexId();
        auto buildInfoHolderIter = _buildInfoHolders.find(indexId);
        if (buildInfoHolderIter == _buildInfoHolders.end()) {
            return Status::InternalError("IndexId[%d] is not found", indexId);
        }
        SingleInvertedIndexBuildInfoHolder* buildInfoHolder = &(buildInfoHolderIter->second);
        RETURN_STATUS_DIRECTLY_IF_ERROR(
            isForReplay ? InvertedIndexerOrganizerUtil::UpdateOneIndexForReplay(docId, _indexerOrganizerMeta,
                                                                                buildInfoHolder, modifiedTokens)
                        : InvertedIndexerOrganizerUtil::UpdateOneIndexForBuild(docId, _indexerOrganizerMeta,
                                                                               buildInfoHolder, modifiedTokens));
    }
    return Status::OK();
}

Status InplaceInvertedIndexModifier::Init(const indexlibv2::framework::TabletData& tabletData)
{
    auto indexConfigs = _schema->GetIndexConfigs(INVERTED_INDEX_TYPE_STR);
    if (indexConfigs.empty()) {
        AUTIL_LOG(INFO, "No index configs found in schema");
        return Status::OK();
    }

    indexlib::index::IndexerOrganizerMeta::InitIndexerOrganizerMeta(tabletData, &_indexerOrganizerMeta);

    std::shared_ptr<InvertedIndexMetrics> indexMetrics;
    docid_t baseDocId = 0;
    auto slice = tabletData.CreateSlice();
    for (auto it = slice.begin(); it != slice.end(); it++) {
        Segment* segment = it->get();
        auto segStatus = segment->GetSegmentStatus();
        uint64_t docCount = segment->GetSegmentInfo()->docCount;
        std::pair<docid_t, uint64_t> key(baseDocId, docCount);
        _segmentId2BaseDocIdAndDocCountPair[segment->GetSegmentId()] = key;
        for (const auto& ic : indexConfigs) {
            auto indexConfig = std::dynamic_pointer_cast<indexlibv2::config::InvertedIndexConfig>(ic);
            if (!indexConfig->IsIndexUpdatable()) {
                continue;
            }

            std::string indexName = indexConfig->GetIndexName();
            assert(indexConfig != nullptr);
            indexid_t indexId = indexConfig->GetIndexId();
            std::shared_ptr<IInvertedDiskIndexer> diskIndexer = nullptr;
            std::shared_ptr<IInvertedMemIndexer> dumpingMemIndexer = nullptr;
            std::shared_ptr<IInvertedMemIndexer> buildingMemIndexer = nullptr;
            Status st = indexlib::index::IndexerOrganizerUtil::GetIndexer<IInvertedDiskIndexer, IInvertedMemIndexer>(
                segment, indexConfig, &diskIndexer, &dumpingMemIndexer, &buildingMemIndexer);
            RETURN_STATUS_DIRECTLY_IF_ERROR(st);
            if (diskIndexer == nullptr && dumpingMemIndexer == nullptr && buildingMemIndexer == nullptr) {
                continue;
            }

            if (_buildInfoHolders.find(indexId) == _buildInfoHolders.end()) {
                SingleInvertedIndexBuildInfoHolder buildInfoHolder;
                _buildInfoHolders[indexId] = std::move(buildInfoHolder);
            }

            if (segStatus == Segment::SegmentStatus::ST_BUILT) {
                // MultiShardInvertedDiskIndexer's GetSingleShardIndexer requires indexer to be Open()'ed before it's
                // called.
                // If segment doc count is 0, Open() will be skipped because of an optimization.
                if (docCount == 0) {
                    continue;
                }
                _buildInfoHolders.at(indexId).diskIndexers[key] = diskIndexer;
            } else if (segStatus == Segment::SegmentStatus::ST_DUMPING) {
                _buildInfoHolders.at(indexId).dumpingIndexers[key] = dumpingMemIndexer;
            } else if (segStatus == Segment::SegmentStatus::ST_BUILDING) {
                indexMetrics = buildingMemIndexer->GetMetrics();
                assert(_buildInfoHolders.at(indexId).buildingIndexer == nullptr);
                _buildInfoHolders.at(indexId).buildingIndexer = buildingMemIndexer;
            }
        }
        baseDocId += docCount;
    }
    InitFieldMetrics(indexMetrics);
    return Status::OK();
}

void InplaceInvertedIndexModifier::InitFieldMetrics(const std::shared_ptr<InvertedIndexMetrics>& indexMetrics)
{
    if (indexMetrics == nullptr) {
        return;
    }
    for (const auto& iConfig : _schema->GetIndexConfigs(INVERTED_INDEX_TYPE_STR)) {
        auto indexConfig = std::dynamic_pointer_cast<indexlibv2::config::InvertedIndexConfig>(iConfig);
        indexid_t indexId = indexConfig->GetIndexId();
        std::string metricsIndexName = indexConfig->GetIndexName();
        if (indexConfig->GetShardingType() == indexlibv2::config::InvertedIndexConfig::IST_IS_SHARDING) {
            autil::StringUtil::replace(metricsIndexName, '@', '_');
        }
        auto _buildInfoHolder = _buildInfoHolders.find(indexId);
        if (_buildInfoHolder != _buildInfoHolders.end()) {
            _buildInfoHolder->second.singleFieldIndexMetrics = indexMetrics->AddSingleFieldIndex(metricsIndexName);
        }
    }
}

bool InplaceInvertedIndexModifier::UpdateOneIndexTermsForReplay(IndexUpdateTermIterator* termIter)
{
    indexid_t indexId = termIter->GetIndexId();
    auto keyIter = _segmentId2BaseDocIdAndDocCountPair.find(termIter->GetSegmentId());
    if (keyIter == _segmentId2BaseDocIdAndDocCountPair.end()) {
        return false;
    }
    if (_buildInfoHolders.find(indexId) == _buildInfoHolders.end()) {
        AUTIL_LOG(ERROR, "Can not find matched indexer with indexId[%d] in segment[%d]", indexId,
                  termIter->GetSegmentId());
        return false;
    }

    const std::pair<docid_t, uint64_t>& baseDocId2DocCount = keyIter->second;
    const auto& diskIndexers = _buildInfoHolders.at(indexId).diskIndexers;
    auto diskIndexerIter = diskIndexers.find(baseDocId2DocCount);
    if (diskIndexerIter == diskIndexers.end()) {
        AUTIL_LOG(ERROR, "Can not find matched indexer with indexId[%d] in segment[%d]", indexId,
                  termIter->GetSegmentId());
        return false;
    }
    diskIndexerIter->second->UpdateTerms(termIter);
    return true;
}

} // namespace indexlib::index
