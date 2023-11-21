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
#include "indexlib/index/inverted_index/IndexAccessoryReader.h"

#include "indexlib/framework/TabletData.h"
#include "indexlib/index/inverted_index/Common.h"
#include "indexlib/index/inverted_index/SectionAttributeReader.h"
#include "indexlib/index/inverted_index/config/InvertedIndexConfig.h"
#include "indexlib/index/inverted_index/config/PackageIndexConfig.h"
#include "indexlib/index/inverted_index/section_attribute/SectionAttributeReaderImpl.h"

namespace indexlib::index {
namespace {
using indexlibv2::config::IIndexConfig;
using indexlibv2::config::InvertedIndexConfig;
using indexlibv2::config::PackageIndexConfig;
} // namespace

AUTIL_LOG_SETUP(indexlib.index, IndexAccessoryReader);

IndexAccessoryReader::IndexAccessoryReader(const std::vector<std::shared_ptr<IIndexConfig>>& indexConfigs,
                                           const indexlibv2::index::IndexReaderParameter& indexReaderParam)
    : _indexConfigs(indexConfigs)
    , _sortPatternFunc(indexReaderParam.sortPatternFunc)
{
}

IndexAccessoryReader::IndexAccessoryReader(const IndexAccessoryReader& other)
    : _indexConfigs(other._indexConfigs)
    , _sectionReaderMap(other._sectionReaderMap)
    , _sortPatternFunc(other._sortPatternFunc)
{
    CloneSectionReaders(other._sectionReaderVec, _sectionReaderVec);
}

IndexAccessoryReader::~IndexAccessoryReader() { _sectionReaderVec.clear(); }

Status IndexAccessoryReader::Open(const indexlibv2::framework::TabletData* tabletData)
{
    ShareSectionMap shareMap;
    for (auto& indexConfig : _indexConfigs) {
        auto invertedIndexConfig = std::dynamic_pointer_cast<InvertedIndexConfig>(indexConfig);
        if (!CheckSectionConfig(invertedIndexConfig)) {
            continue;
        }
        auto status = InitSectionReader(invertedIndexConfig, tabletData, shareMap);
        RETURN_IF_STATUS_ERROR(status, "init section reader fail. error [%s].", status.ToString().c_str());
    }

    ShareSectionMap::const_iterator it = shareMap.begin();
    for (; it != shareMap.end(); ++it) {
        const std::string& sharedIndexName = it->first;
        const ShareSectionIndexNames& indexNames = it->second;
        SectionReaderPosMap::const_iterator sit = _sectionReaderMap.find(sharedIndexName);
        assert(sit != _sectionReaderMap.end());
        for (size_t i = 0; i < indexNames.size(); ++i) {
            _sectionReaderMap.insert(make_pair(indexNames[i], sit->second));
        }
    }
    return Status::OK();
}

const std::shared_ptr<SectionAttributeReader> IndexAccessoryReader::GetSectionReader(const std::string& indexName) const
{
    SectionReaderPosMap::const_iterator it = _sectionReaderMap.find(indexName);
    if (it != _sectionReaderMap.end()) {
        return _sectionReaderVec[it->second];
    }
    return nullptr;
}

IndexAccessoryReader* IndexAccessoryReader::Clone() const { return new IndexAccessoryReader(*this); }

void IndexAccessoryReader::CloneSectionReaders(const SectionReaderVec& srcReaders, SectionReaderVec& clonedReaders)
{
    clonedReaders.clear();
    for (size_t i = 0; i < srcReaders.size(); ++i) {
        auto srcReader = dynamic_cast<SectionAttributeReaderImpl*>(srcReaders[i].get());
        assert(srcReader != nullptr);
        SectionAttributeReaderImpl* cloneReader = dynamic_cast<SectionAttributeReaderImpl*>(srcReader->Clone());
        assert(cloneReader != NULL);
        clonedReaders.push_back(std::shared_ptr<SectionAttributeReaderImpl>(cloneReader));
    }
}

bool IndexAccessoryReader::CheckSectionConfig(const std::shared_ptr<InvertedIndexConfig>& indexConfig) const
{
    auto indexType = indexConfig->GetInvertedIndexType();
    if (indexType == it_pack || indexType == it_expack) {
        auto packIndexConfig = std::dynamic_pointer_cast<PackageIndexConfig>(indexConfig);
        if (!packIndexConfig) {
            AUTIL_LOG(ERROR, "indexConfig must be PackageIndexConfig"
                             " for pack/expack index");
            return false;
        }
        return packIndexConfig->HasSectionAttribute();
    }
    return false;
}

Status IndexAccessoryReader::InitSectionReader(const std::shared_ptr<InvertedIndexConfig>& indexConfig,
                                               const indexlibv2::framework::TabletData* tabletData,
                                               ShareSectionMap& shareMap)
{
    std::string indexName = indexConfig->GetIndexName();
    auto packIndexConfig = std::dynamic_pointer_cast<PackageIndexConfig>(indexConfig);
    assert(packIndexConfig.get() != NULL);

    AddSharedRelations(packIndexConfig, shareMap);

    if (UseSharedSectionReader(packIndexConfig)) {
        return Status::OK();
    }
    auto sectionReader = std::make_shared<SectionAttributeReaderImpl>(GetSortPattern(packIndexConfig));
    auto status = sectionReader->Open(indexConfig, tabletData);
    RETURN_IF_STATUS_ERROR(status, "open section attribute reader fail. error [%s].", status.ToString().c_str());
    _sectionReaderVec.push_back(sectionReader);
    bool ret = _sectionReaderMap.insert(std::make_pair(indexName, _sectionReaderVec.size() - 1)).second;
    if (!ret) {
        AUTIL_LOG(ERROR, "insert section reader map fail.");
        return Status::Unknown("insert section reader map fail.");
    }
    return Status::OK();
}

void IndexAccessoryReader::AddSharedRelations(const std::shared_ptr<PackageIndexConfig>& packIndexConfig,
                                              ShareSectionMap& shareMap)
{
    InvertedIndexConfig::IndexShardingType shardingType = packIndexConfig->GetShardingType();
    if (shardingType == InvertedIndexConfig::IST_IS_SHARDING) {
        return;
    }

    std::string sharedIndexName;
    if (packIndexConfig->IsVirtual()) {
        sharedIndexName = packIndexConfig->GetNonTruncateIndexName();
        assert(!sharedIndexName.empty());
        shareMap[sharedIndexName].push_back(packIndexConfig->GetIndexName());
    } else {
        sharedIndexName = packIndexConfig->GetIndexName();
    }

    if (shardingType == InvertedIndexConfig::IST_NO_SHARDING) {
        return;
    }
    const std::vector<std::shared_ptr<InvertedIndexConfig>>& shardingIndexConfigs =
        packIndexConfig->GetShardingIndexConfigs();

    for (size_t i = 0; i < shardingIndexConfigs.size(); ++i) {
        shareMap[sharedIndexName].push_back(shardingIndexConfigs[i]->GetIndexName());
    }
}

bool IndexAccessoryReader::UseSharedSectionReader(const std::shared_ptr<PackageIndexConfig>& packIndexConfig)
{
    if (packIndexConfig->IsVirtual() || packIndexConfig->GetShardingType() == InvertedIndexConfig::IST_IS_SHARDING) {
        return true;
    }
    return false;
}

indexlibv2::config::SortPattern
IndexAccessoryReader::GetSortPattern(const std::shared_ptr<PackageIndexConfig>& indexConfig) const
{
    auto sectionAttrConf = indexConfig->GetSectionAttributeConfig();
    if (!sectionAttrConf) {
        return indexlibv2::config::sp_nosort;
    }

    auto attrConfig = sectionAttrConf->CreateAttributeConfig(indexConfig->GetIndexName());
    if (!attrConfig) {
        assert(false);
        return indexlibv2::config::sp_nosort;
    }

    if (_sortPatternFunc) {
        return _sortPatternFunc(attrConfig->GetAttrName());
    }
    return indexlibv2::config::sp_nosort;
}

} // namespace indexlib::index
