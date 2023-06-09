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
#include "indexlib/index/inverted_index/builtin_index/pack/PackIndexMerger.h"

#include "indexlib/index/IIndexFactory.h"
#include "indexlib/index/attribute/config/AttributeConfig.h"
#include "indexlib/index/inverted_index/config/PackageIndexConfig.h"
#include "indexlib/index/inverted_index/config/SectionAttributeConfig.h"

namespace indexlib::index {
namespace {
using indexlibv2::config::IIndexConfig;
using indexlibv2::config::InvertedIndexConfig;
using indexlibv2::config::PackageIndexConfig;
} // namespace

AUTIL_LOG_SETUP(indexlib.index, PackIndexMerger);

Status PackIndexMerger::Init(const std::shared_ptr<IIndexConfig>& indexConfig,
                             const std::map<std::string, std::any>& params)
{
    auto status = InvertedIndexMerger::Init(indexConfig, params);
    RETURN_IF_STATUS_ERROR(status, "init pack inverted index fail for [%s]", indexConfig->GetIndexName().c_str());

    const auto& packIndexConfig = std::dynamic_pointer_cast<PackageIndexConfig>(_indexConfig);
    assert(packIndexConfig);
    if (packIndexConfig->GetShardingType() != InvertedIndexConfig::IST_IS_SHARDING &&
        packIndexConfig->HasSectionAttribute()) {
        const auto& sectionAttrConfig = packIndexConfig->GetSectionAttributeConfig();
        assert(sectionAttrConfig);
        auto attrConfig = sectionAttrConfig->CreateAttributeConfig(packIndexConfig->GetIndexName());

        auto indexFactoryCreator = indexlibv2::index::IndexFactoryCreator::GetInstance();
        auto [status, indexFactory] = indexFactoryCreator->Create(attrConfig->GetIndexType());
        RETURN_IF_STATUS_ERROR(status, "get index factory for index [%s] failed", attrConfig->GetIndexName().c_str());
        _sectionMerger = indexFactory->CreateIndexMerger(attrConfig);
        assert(_sectionMerger);
        status = _sectionMerger->Init(attrConfig, params);
        RETURN_IF_STATUS_ERROR(status, "init section attribute merger fail for [%s]",
                               attrConfig->GetIndexName().c_str());
    }
    return Status::OK();
}

Status
PackIndexMerger::Merge(const SegmentMergeInfos& segMergeInfos,
                       const std::shared_ptr<indexlibv2::framework::IndexTaskResourceManager>& taskResourceManager)
{
    auto status = InvertedIndexMerger::Merge(segMergeInfos, taskResourceManager);
    RETURN_IF_STATUS_ERROR(status, "do merger operation for inverted index [%s] fail",
                           _indexConfig->GetIndexName().c_str());
    if (_sectionMerger) {
        status = _sectionMerger->Merge(segMergeInfos, taskResourceManager);
        RETURN_IF_STATUS_ERROR(status, "do merger operation for section inverted index [%s] fail",
                               _indexConfig->GetIndexName().c_str());
    }
    return Status::OK();
}

} // namespace indexlib::index
