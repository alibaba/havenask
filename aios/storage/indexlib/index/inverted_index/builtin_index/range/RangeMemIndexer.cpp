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
#include "indexlib/index/inverted_index/builtin_index/range/RangeMemIndexer.h"

#include <any>

#include "indexlib/document/IDocument.h"
#include "indexlib/document/normal/IndexDocument.h"
#include "indexlib/document/normal/IndexTokenizeField.h"
#include "indexlib/document/normal/Token.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/framework/MetricsManager.h"
#include "indexlib/framework/SegmentMetrics.h"
#include "indexlib/index/BuildingIndexMemoryUseUpdater.h"
#include "indexlib/index/inverted_index/InvertedLeafMemReader.h"
#include "indexlib/index/inverted_index/builtin_index/range/RangeInfo.h"
#include "indexlib/index/inverted_index/builtin_index/range/RangeLeafMemReader.h"
#include "indexlib/index/inverted_index/config/InvertedIndexConfig.h"
#include "indexlib/index/inverted_index/config/RangeIndexConfig.h"
#include "indexlib/util/memory_control/BuildResourceMetrics.h"

namespace indexlib::index {
namespace {
using indexlibv2::index::BuildingIndexMemoryUseUpdater;
} // namespace

AUTIL_LOG_SETUP(indexlib.index, RangeMemIndexer);

RangeMemIndexer::RangeMemIndexer(const indexlibv2::index::MemIndexerParameter& indexerParam)
    : InvertedMemIndexer(indexerParam, nullptr)
    , _rangeInfo(new RangeInfo)
{
}

RangeMemIndexer::~RangeMemIndexer() {}

Status RangeMemIndexer::Init(const std::shared_ptr<indexlibv2::config::IIndexConfig>& indexConfig,
                             indexlibv2::document::extractor::IDocumentInfoExtractorFactory* docInfoExtractorFactory)
{
    auto status = InvertedMemIndexer::Init(indexConfig, docInfoExtractorFactory);
    RETURN_IF_STATUS_ERROR(status, "range mem indexer init failed");
    _rangeIndexConfig = std::dynamic_pointer_cast<indexlibv2::config::RangeIndexConfig>(indexConfig);
    if (_rangeIndexConfig == nullptr) {
        AUTIL_LOG(ERROR,
                  "IndexType[%s] IndexName[%s] invalid index config, "
                  "dynamic cast to range index config failed.",
                  indexConfig->GetIndexType().c_str(), indexConfig->GetIndexName().c_str());
        return Status::InvalidArgs("invalid index config, dynamic cast to legacy index config failed.");
    }
    // TODO: Maybe support multi sharding for range index.
    assert(_rangeIndexConfig->GetShardingType() != indexlibv2::config::InvertedIndexConfig::IST_NEED_SHARDING);

    SetDistinctTermCount(_rangeIndexConfig->GetIndexName(), _indexerParam.segmentMetrics);
    assert(_indexerParam.buildResourceMetrics);
    {
        std::shared_ptr<indexlibv2::config::InvertedIndexConfig> bottomLevelConfig =
            _rangeIndexConfig->GetBottomLevelIndexConfig();
        auto bottomMemIndexer = CreateInvertedMemIndexer(bottomLevelConfig);
        if (!bottomMemIndexer) {
            RETURN_IF_STATUS_ERROR(Status::InternalError(), "create bottom inverted mem indexer fail");
        }
        auto status = bottomMemIndexer->Init(bottomLevelConfig, docInfoExtractorFactory);
        RETURN_IF_STATUS_ERROR(status, "bottom mem indexer init fail");
        _bottomLevelMemIndexer = bottomMemIndexer;
        auto metricsNode = _indexerParam.buildResourceMetrics->AllocateNode();
        _bottomLevelMemUpdater = std::make_shared<BuildingIndexMemoryUseUpdater>(metricsNode);
    }
    {
        std::shared_ptr<indexlibv2::config::InvertedIndexConfig> highLevelConfig =
            _rangeIndexConfig->GetHighLevelIndexConfig();
        auto highMemIndexer = CreateInvertedMemIndexer(highLevelConfig);
        if (!highMemIndexer) {
            RETURN_IF_STATUS_ERROR(Status::InternalError(), "create high inverted mem indexer fail");
        }
        auto status = highMemIndexer->Init(highLevelConfig, docInfoExtractorFactory);
        RETURN_IF_STATUS_ERROR(status, "high mem indexer init fail");
        _highLevelMemIndexer = highMemIndexer;
        auto metricsNode = _indexerParam.buildResourceMetrics->AllocateNode();
        _highLevelMemUpdater = std::make_shared<BuildingIndexMemoryUseUpdater>(metricsNode);
    }
    return Status::OK();
}

void RangeMemIndexer::SetDistinctTermCount(const std::string& indexName,
                                           const std::shared_ptr<framework::SegmentMetrics>& segmentMetrics)
{
    auto highLevelIndexName = indexlibv2::config::RangeIndexConfig::GetHighLevelIndexName(indexName);
    auto bottomLevelIndexName = indexlibv2::config::RangeIndexConfig::GetBottomLevelIndexName(indexName);
    size_t highLevelTermCount =
        segmentMetrics != nullptr ? segmentMetrics->GetDistinctTermCount(highLevelIndexName) : 0;
    size_t bottomLevelTermCount =
        segmentMetrics != nullptr ? segmentMetrics->GetDistinctTermCount(bottomLevelIndexName) : 0;
    if (highLevelTermCount > 0 && bottomLevelTermCount > 0) {
        return;
    }

    size_t totalTermCount = segmentMetrics != nullptr ? segmentMetrics->GetDistinctTermCount(indexName) : 0;
    if (totalTermCount == 0) {
        totalTermCount = HASHMAP_INIT_SIZE * 10;
    }
    highLevelTermCount = totalTermCount * (1 - DEFAULT_BOTTOM_LEVEL_TERM_COUNT_RATIO);
    bottomLevelTermCount = totalTermCount * DEFAULT_BOTTOM_LEVEL_TERM_COUNT_RATIO;
    if (segmentMetrics != nullptr) {
        segmentMetrics->SetDistinctTermCount(highLevelIndexName, highLevelTermCount);
        segmentMetrics->SetDistinctTermCount(bottomLevelIndexName, bottomLevelTermCount);
    }
}

std::shared_ptr<InvertedMemIndexer>
RangeMemIndexer::CreateInvertedMemIndexer(const std::shared_ptr<indexlibv2::config::InvertedIndexConfig>& indexConfig)
{
    auto invertedIndexMetrics = InvertedIndexMetrics::Create(indexConfig, _indexerParam.metricsManager);
    return std::make_shared<InvertedMemIndexer>(_indexerParam, invertedIndexMetrics);
}

void RangeMemIndexer::EndDocument(const document::IndexDocument& indexDocument)
{
    _bottomLevelMemIndexer->EndDocument(indexDocument);
    _highLevelMemIndexer->EndDocument(indexDocument);
    _basePos = 0;
}

Status RangeMemIndexer::AddField(const document::Field* field)
{
    if (!field) {
        return Status::OK();
    }
    auto tokenizeField = dynamic_cast<const document::IndexTokenizeField*>(field);
    if (!tokenizeField) {
        AUTIL_LOG(ERROR, "fieldTag [%d] is not IndexTokenizeField", static_cast<int8_t>(field->GetFieldTag()));
        return Status::ConfigError("field tag is not IndexTokenizeField");
    }
    if (tokenizeField->GetSectionCount() <= 0) {
        return Status::OK();
    }

    for (auto iterField = tokenizeField->Begin(); iterField != tokenizeField->End(); iterField++) {
        const document::Section* section = *iterField;
        if (section == NULL || section->GetTokenCount() <= 0) {
            continue;
        }

        const document::Token* token = section->GetToken(0);
        _rangeInfo->Update(token->GetHashKey());

        pos_t tokenBasePos = _basePos + token->GetPosIncrement();
        fieldid_t fieldId = tokenizeField->GetFieldId();
        auto status = _bottomLevelMemIndexer->AddToken(token, fieldId, tokenBasePos);
        RETURN_IF_STATUS_ERROR(status, "bottom mem indexer add token fail, hashKey[%lu] fieldid[%d]",
                               token->GetHashKey(), fieldId);
        for (size_t i = 1; i < section->GetTokenCount(); i++) {
            const document::Token* token = section->GetToken(i);
            tokenBasePos += token->GetPosIncrement();
            status = _highLevelMemIndexer->AddToken(token, fieldId, tokenBasePos);
            RETURN_IF_STATUS_ERROR(status, "high mem indexer add token fail, hashKey[%lu] fieldid[%d]",
                                   token->GetHashKey(), fieldId);
        }
        _basePos += section->GetLength();
    }
    return Status::OK();
}

Status RangeMemIndexer::DoDump(autil::mem_pool::PoolBase* dumpPool,
                               const std::shared_ptr<file_system::Directory>& directory,
                               const std::shared_ptr<indexlibv2::framework::DumpParams>& dumpParams)
{
    const std::string& indexName = GetIndexName();
    AUTIL_LOG(DEBUG, "Dumping index : [%s]", indexName.c_str());
    file_system::DirectoryPtr indexDirectory = directory->MakeDirectory(_rangeIndexConfig->GetIndexName());
    auto status = _bottomLevelMemIndexer->Dump(dumpPool, indexDirectory, dumpParams);
    RETURN_IF_STATUS_ERROR(status, "bottom mem indexer dump fail, indexName[%s]", indexName.c_str());
    status = _highLevelMemIndexer->Dump(dumpPool, indexDirectory, dumpParams);
    RETURN_IF_STATUS_ERROR(status, "high mem indexer dump fail, indexName[%s]", indexName.c_str());
    status = _rangeInfo->Store(indexDirectory->GetIDirectory());
    RETURN_IF_STATUS_ERROR(status, "writer range info fail, indexName[%s]", indexName.c_str());
    return Status::OK();
}

std::shared_ptr<IndexSegmentReader> RangeMemIndexer::CreateInMemReader()
{
    auto bottomReader = std::dynamic_pointer_cast<InvertedLeafMemReader>(_bottomLevelMemIndexer->CreateInMemReader());
    assert(bottomReader);
    auto highReader = std::dynamic_pointer_cast<InvertedLeafMemReader>(_highLevelMemIndexer->CreateInMemReader());
    assert(highReader);
    return std::make_shared<RangeLeafMemReader>(bottomReader, highReader, _rangeInfo);
}

void RangeMemIndexer::FillStatistics(const std::shared_ptr<framework::SegmentMetrics>& segmentMetrics)
{
    SetDistinctTermCount(_rangeIndexConfig->GetIndexName(), segmentMetrics);
}
void RangeMemIndexer::UpdateMemUse(BuildingIndexMemoryUseUpdater* memUpdater)
{
    _bottomLevelMemIndexer->UpdateMemUse(_bottomLevelMemUpdater.get());
    _highLevelMemIndexer->UpdateMemUse(_highLevelMemUpdater.get());
}

void RangeMemIndexer::Seal()
{
    _bottomLevelMemIndexer->Seal();
    _highLevelMemIndexer->Seal();
}
} // namespace indexlib::index
