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
#include "indexlib/index/inverted_index/MultiShardInvertedDiskIndexer.h"

#include "indexlib/file_system/Directory.h"
#include "indexlib/index/IIndexFactory.h"
#include "indexlib/index/IndexFactoryCreator.h"
#include "indexlib/index/attribute/MultiSliceAttributeDiskIndexer.h"
#include "indexlib/index/attribute/MultiValueAttributeDiskIndexer.h"
#include "indexlib/index/inverted_index/InvertedDiskIndexer.h"
#include "indexlib/index/inverted_index/config/PackageIndexConfig.h"
#include "indexlib/index/inverted_index/config/SectionAttributeConfig.h"
#include "indexlib/index/inverted_index/format/ShardingIndexHasher.h"
#include "indexlib/index/inverted_index/patch/IndexUpdateTermIterator.h"

namespace indexlib::index {
namespace {
using indexlibv2::config::IIndexConfig;
using indexlibv2::config::InvertedIndexConfig;
} // namespace

AUTIL_LOG_SETUP(indexlib.index, MultiShardInvertedDiskIndexer);

MultiShardInvertedDiskIndexer::MultiShardInvertedDiskIndexer(const indexlibv2::index::IndexerParameter& indexerParam)
    : _indexerParam(indexerParam)
{
}

Status MultiShardInvertedDiskIndexer::Open(const std::shared_ptr<IIndexConfig>& indexConfig,
                                           const std::shared_ptr<file_system::IDirectory>& indexDirectory)
{
    if (_indexerParam.docCount == 0) {
        // there is no document here, so do nothing
        AUTIL_LOG(INFO, "doc count is zero in index [%s] for segment [%d], just do nothing",
                  indexConfig->GetIndexName().c_str(), _indexerParam.segmentId);
        return Status::OK();
    }
    auto config = std::dynamic_pointer_cast<InvertedIndexConfig>(indexConfig);
    if (config == nullptr) {
        auto status = Status::InvalidArgs("IndexType[%s] IndexName[%s] invalid index config, "
                                          "dynamic cast to legacy index config failed.",
                                          config->GetIndexType().c_str(), config->GetIndexName().c_str());
        AUTIL_LOG(ERROR, "%s", status.ToString().c_str());
        return status;
    }

    if (config->GetShardingType() != InvertedIndexConfig::IST_NEED_SHARDING) {
        auto status = Status::InvalidArgs("index [%s] should be need_sharding index", config->GetIndexName().c_str());
        AUTIL_LOG(ERROR, "%s", status.ToString().c_str());
        return status;
    }

    const auto& shardIndexConfigs = config->GetShardingIndexConfigs();
    if (shardIndexConfigs.empty()) {
        auto status = Status::InvalidArgs("index [%s] has none sharding indexConfigs", config->GetIndexName().c_str());
        AUTIL_LOG(ERROR, "%s", status.ToString().c_str());
        return status;
    }
    _indexHasher = std::make_unique<ShardingIndexHasher>();
    _indexHasher->Init(config);
    auto indexFactoryCreator = indexlibv2::index::IndexFactoryCreator::GetInstance();
    for (const auto& shardConfig : shardIndexConfigs) {
        std::string indexType = shardConfig->GetIndexType();
        std::string indexName = shardConfig->GetIndexName();
        auto [status, indexFactory] = indexFactoryCreator->Create(indexType);
        if (!status.IsOK()) {
            AUTIL_LOG(ERROR, "create index factory for index type [%s] failed", indexType.c_str());
            return status;
        }
        auto indexer = indexFactory->CreateDiskIndexer(shardConfig, _indexerParam);
        if (indexer == nullptr) {
            AUTIL_LOG(WARN, "get null indexer [%s]", indexName.c_str());
            return Status::Unimplement();
        }
        status = indexer->Open(shardConfig, indexDirectory);
        if (!status.IsOK()) {
            AUTIL_LOG(ERROR, "create built index [%s] failed", indexName.c_str());
            return status;
        }
        auto invertedDiskIndexer = std::dynamic_pointer_cast<InvertedDiskIndexer>(indexer);
        if (invertedDiskIndexer == nullptr) {
            AUTIL_LOG(ERROR, "[%s] cast to inverted disk indexer failed", indexName.c_str());
            return Status::InternalError();
        }
        _shardIndexers.push_back(invertedDiskIndexer);
        auto [_, ret] = _indexers.emplace(indexName, invertedDiskIndexer);
        if (!ret) {
            AUTIL_LOG(ERROR, "duplicate shard config index name[%s]", indexName.c_str());
            return Status::InvalidArgs();
        }
    }
    auto indexType = config->GetInvertedIndexType();
    if (indexType == it_pack || indexType == it_expack) {
        auto packageIndexConfig = std::dynamic_pointer_cast<indexlibv2::config::PackageIndexConfig>(config);
        assert(packageIndexConfig);
        std::shared_ptr<indexlibv2::config::SectionAttributeConfig> sectionAttrConf =
            packageIndexConfig->GetSectionAttributeConfig();
        if (packageIndexConfig->HasSectionAttribute() && sectionAttrConf) {
            std::shared_ptr<indexlibv2::config::AttributeConfig> attrConfig =
                sectionAttrConf->CreateAttributeConfig(packageIndexConfig->GetIndexName());
            auto [status, indexFactory] = indexFactoryCreator->Create(attrConfig->GetIndexType());
            RETURN_IF_STATUS_ERROR(status, "get index factory for index type [%s] failed",
                                   attrConfig->GetIndexType().c_str());
            _multiSliceAttrDiskIndexer = indexFactory->CreateDiskIndexer(attrConfig, _indexerParam);
            if (!_multiSliceAttrDiskIndexer) {
                RETURN_IF_STATUS_ERROR(Status::InvalidArgs(),
                                       "create section attribute disk indexer failed, indexName[%s]",
                                       config->GetIndexName().c_str());
            }
            status = _multiSliceAttrDiskIndexer->Open(attrConfig, indexDirectory);
            RETURN_IF_STATUS_ERROR(status, "section attribute disk indexer init failed, indexName[%s]",
                                   config->GetIndexName().c_str());

            auto multiSliceDiskIndexer = std::dynamic_pointer_cast<indexlibv2::index::MultiSliceAttributeDiskIndexer>(
                _multiSliceAttrDiskIndexer);
            assert(multiSliceDiskIndexer);
            assert(multiSliceDiskIndexer->GetSliceCount() == 1);
            _sectionAttrDiskIndexer =
                multiSliceDiskIndexer->GetSliceIndexer<indexlibv2::index::MultiValueAttributeDiskIndexer<char>>(0);
            if (_sectionAttrDiskIndexer == nullptr) {
                RETURN_IF_STATUS_ERROR(Status::InvalidArgs(),
                                       "cast to MultiValueAttributeDiskIndexer<char> failed, indexName[%s]",
                                       config->GetIndexName().c_str());
            }
            _sectionAttrConfig = attrConfig;
        }
    }
    return Status::OK();
}

std::shared_ptr<InvertedDiskIndexer>
MultiShardInvertedDiskIndexer::GetSingleShardIndexer(const std::string& indexName) const
{
    auto iter = _indexers.find(indexName);
    if (iter == _indexers.end()) {
        AUTIL_LOG(WARN, "shard disk indexer [%s] not found", indexName.c_str());
        return nullptr;
    }
    return iter->second;
}

size_t MultiShardInvertedDiskIndexer::EstimateSectionAttributeMemUsed(
    const std::shared_ptr<InvertedIndexConfig>& config, const std::shared_ptr<file_system::IDirectory>& indexDirectory)
{
    auto indexType = config->GetInvertedIndexType();
    if (indexType != it_pack && indexType != it_expack) {
        return 0;
    }
    auto packageIndexConfig = std::dynamic_pointer_cast<indexlibv2::config::PackageIndexConfig>(config);
    assert(packageIndexConfig);
    auto sectionAttrConf = packageIndexConfig->GetSectionAttributeConfig();
    if (!packageIndexConfig->HasSectionAttribute() || !sectionAttrConf) {
        return 0;
    }
    auto indexFactoryCreator = indexlibv2::index::IndexFactoryCreator::GetInstance();
    auto attrConfig = sectionAttrConf->CreateAttributeConfig(packageIndexConfig->GetIndexName());
    auto [status, indexFactory] = indexFactoryCreator->Create(attrConfig->GetIndexType());
    if (!status.IsOK()) {
        AUTIL_LOG(ERROR, "get index factory for index type [%s] failed", attrConfig->GetIndexType().c_str());
        return 0;
    }
    auto attrDiskIndexer = indexFactory->CreateDiskIndexer(attrConfig, _indexerParam);
    assert(attrDiskIndexer);
    return attrDiskIndexer->EstimateMemUsed(attrConfig, indexDirectory);
}

size_t MultiShardInvertedDiskIndexer::EstimateMemUsed(const std::shared_ptr<IIndexConfig>& indexConfig,
                                                      const std::shared_ptr<file_system::IDirectory>& indexDirectory)
{
    if (_indexerParam.docCount == 0) {
        return 0;
    }
    size_t totalMemUse = 0;
    auto config = std::dynamic_pointer_cast<InvertedIndexConfig>(indexConfig);
    assert(config);
    assert(config->GetShardingType() == InvertedIndexConfig::IST_NEED_SHARDING);
    const auto& shardIndexConfigs = config->GetShardingIndexConfigs();
    auto indexFactoryCreator = indexlibv2::index::IndexFactoryCreator::GetInstance();
    for (const auto& shardConfig : shardIndexConfigs) {
        std::string indexType = shardConfig->GetIndexType();
        auto [status, indexFactory] = indexFactoryCreator->Create(indexType);
        if (!status.IsOK()) {
            AUTIL_LOG(ERROR, "create index factory for index type [%s] failed", indexType.c_str());
            continue;
        }
        auto indexer = indexFactory->CreateDiskIndexer(shardConfig, _indexerParam);
        if (!indexer) {
            AUTIL_LOG(ERROR, "create disk indexer [%s] failed", shardConfig->GetIndexName().c_str());
            continue;
        }
        totalMemUse += indexer->EstimateMemUsed(shardConfig, indexDirectory);
    }
    totalMemUse += EstimateSectionAttributeMemUsed(config, indexDirectory);

    return totalMemUse;
}

size_t MultiShardInvertedDiskIndexer::EvaluateCurrentMemUsed()
{
    size_t totalCurrentMemUse = 0;
    for (const auto& kv : _indexers) {
        const auto& indexer = kv.second;
        totalCurrentMemUse += indexer->EvaluateCurrentMemUsed();
    }
    if (_sectionAttrDiskIndexer) {
        totalCurrentMemUse += _sectionAttrDiskIndexer->EvaluateCurrentMemUsed();
    }
    return totalCurrentMemUse;
}

void MultiShardInvertedDiskIndexer::UpdateTokens(docid_t localDocId, const document::ModifiedTokens& modifiedTokens)
{
    UpdateTokens(localDocId, modifiedTokens, INVALID_SHARDID);
}
void MultiShardInvertedDiskIndexer::UpdateTokens(docid_t localDocId, const document::ModifiedTokens& modifiedTokens,
                                                 size_t shardId)
{
    for (size_t i = 0; i < modifiedTokens.NonNullTermSize(); ++i) {
        auto [termKey, op] = modifiedTokens[i];
        DictKeyInfo dictKeyInfo(termKey);
        size_t termShardId = _indexHasher->GetShardingIdx(dictKeyInfo);
        if (shardId == INVALID_SHARDID || termShardId == shardId) {
            _shardIndexers[termShardId]->UpdateOneTerm(localDocId, dictKeyInfo, op);
        }
    }
    document::ModifiedTokens::Operation op;
    if (modifiedTokens.GetNullTermOperation(&op)) {
        size_t termShardId = _indexHasher->GetShardingIdx(DictKeyInfo::NULL_TERM);
        if (shardId == INVALID_SHARDID || termShardId == shardId) {
            _shardIndexers[termShardId]->UpdateOneTerm(localDocId, DictKeyInfo::NULL_TERM, op);
        }
    }
}

void MultiShardInvertedDiskIndexer::UpdateTerms(IndexUpdateTermIterator* termIter)
{
    auto termKey = termIter->GetTermKey();
    size_t termShardId = _indexHasher->GetShardingIdx(termKey);
    _shardIndexers[termShardId]->UpdateTerms(termIter);
}

void MultiShardInvertedDiskIndexer::UpdateBuildResourceMetrics(size_t& poolMemory, size_t& totalMemory,
                                                               size_t& totalRetiredMemory, size_t& totalDocCount,
                                                               size_t& totalAlloc, size_t& totalFree,
                                                               size_t& totalTreeCount) const
{
    for (auto [key, indexer] : _indexers) {
        indexer->UpdateBuildResourceMetrics(poolMemory, totalMemory, totalRetiredMemory, totalDocCount, totalAlloc,
                                            totalFree, totalTreeCount);
    }
}

std::shared_ptr<indexlibv2::index::IDiskIndexer> MultiShardInvertedDiskIndexer::GetSectionAttributeDiskIndexer() const
{
    return _sectionAttrDiskIndexer;
}

} // namespace indexlib::index
