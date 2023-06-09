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
#include "indexlib/table/normal_table/virtual_attribute/VirtualAttributeIndexFactory.h"

#include "indexlib/index/IIndexFactory.h"
#include "indexlib/index/IIndexMerger.h"
#include "indexlib/index/IndexFactoryCreator.h"
#include "indexlib/table/normal_table/virtual_attribute/Common.h"
#include "indexlib/table/normal_table/virtual_attribute/VirtualAttributeConfig.h"
#include "indexlib/table/normal_table/virtual_attribute/VirtualAttributeDiskIndexer.h"
#include "indexlib/table/normal_table/virtual_attribute/VirtualAttributeIndexReader.h"
#include "indexlib/table/normal_table/virtual_attribute/VirtualAttributeMemIndexer.h"

namespace indexlibv2::table {
AUTIL_LOG_SETUP(indexlib.table, VirtualAttributeIndexFactory);

VirtualAttributeIndexFactory::VirtualAttributeIndexFactory() : IIndexFactory() {}

VirtualAttributeIndexFactory::~VirtualAttributeIndexFactory() {}

std::shared_ptr<index::IDiskIndexer>
VirtualAttributeIndexFactory::CreateDiskIndexer(const std::shared_ptr<config::IIndexConfig>& indexConfig,
                                                const index::IndexerParameter& indexerParam) const
{
    auto indexFactoryCreator = index::IndexFactoryCreator::GetInstance();
    auto virtualAttrConfig = std::dynamic_pointer_cast<VirtualAttributeConfig>(indexConfig);
    assert(virtualAttrConfig);
    auto attrConfig = virtualAttrConfig->GetAttributeConfig();
    assert(attrConfig);
    auto [status, attrFactory] = indexFactoryCreator->Create(attrConfig->GetIndexType());
    if (!status.IsOK()) {
        AUTIL_LOG(ERROR, "get index factory for index type [%s] failed", attrConfig->GetIndexType().c_str());
        return nullptr;
    }
    auto attrIndexer = attrFactory->CreateDiskIndexer(attrConfig, indexerParam);
    if (!attrIndexer) {
        return nullptr;
    }
    return std::make_shared<VirtualAttributeDiskIndexer>(attrIndexer);
}
std::shared_ptr<index::IMemIndexer>
VirtualAttributeIndexFactory::CreateMemIndexer(const std::shared_ptr<config::IIndexConfig>& indexConfig,
                                               const index::IndexerParameter& indexerParam) const
{
    auto indexFactoryCreator = index::IndexFactoryCreator::GetInstance();
    auto virtualAttrConfig = std::dynamic_pointer_cast<VirtualAttributeConfig>(indexConfig);
    assert(virtualAttrConfig);
    auto attrConfig = virtualAttrConfig->GetAttributeConfig();
    assert(attrConfig);
    auto [status, attrFactory] = indexFactoryCreator->Create(attrConfig->GetIndexType());
    if (!status.IsOK()) {
        AUTIL_LOG(ERROR, "get index factory for index type [%s] failed", attrConfig->GetIndexType().c_str());
        return nullptr;
    }
    auto attrIndexer = attrFactory->CreateMemIndexer(attrConfig, indexerParam);
    if (!attrIndexer) {
        return nullptr;
    }
    return std::make_shared<VirtualAttributeMemIndexer>(attrIndexer);
}
std::unique_ptr<index::IIndexReader>
VirtualAttributeIndexFactory::CreateIndexReader(const std::shared_ptr<config::IIndexConfig>& indexConfig,
                                                const index::IndexerParameter& indexerParam) const
{
    auto virtualAttrConfig = std::dynamic_pointer_cast<VirtualAttributeConfig>(indexConfig);
    assert(virtualAttrConfig);
    auto attrConfig =
        std::dynamic_pointer_cast<indexlibv2::config::AttributeConfig>(virtualAttrConfig->GetAttributeConfig());
    assert(attrConfig);
    auto fieldType = attrConfig->GetFieldType();
    if (fieldType == ft_uint16) {
        return std::make_unique<VirtualAttributeIndexReader<uint16_t>>();
    } else if (fieldType == ft_int64) {
        return std::make_unique<VirtualAttributeIndexReader<int64_t>>();
    }
    AUTIL_LOG(ERROR, "not support field type [%d]", fieldType);
    assert(false);
    return nullptr;
}

class VirtualAttributeIndexMerger : public index::IIndexMerger
{
public:
    VirtualAttributeIndexMerger() : IIndexMerger() {}
    ~VirtualAttributeIndexMerger() {}
    Status Init(const std::shared_ptr<config::IIndexConfig>& indexConfig,
                const std::map<std::string, std::any>& params) override
    {
        return Status::OK();
    }
    Status Merge(const SegmentMergeInfos& segMergeInfos,
                 const std::shared_ptr<framework::IndexTaskResourceManager>& taskResourceManager) override
    {
        return Status::OK();
    }
};

std::unique_ptr<index::IIndexMerger>
VirtualAttributeIndexFactory::CreateIndexMerger(const std::shared_ptr<config::IIndexConfig>& indexConfig) const
{
    return std::make_unique<VirtualAttributeIndexMerger>();
}
std::unique_ptr<config::IIndexConfig>
VirtualAttributeIndexFactory::CreateIndexConfig(const autil::legacy::Any& any) const
{
    assert(false);
    return nullptr;
}

std::string VirtualAttributeIndexFactory::GetIndexPath() const { return VIRTUAL_ATTRIBUTE_INDEX_PATH; }
REGISTER_INDEX(normal_tablet_virtual_attribute, VirtualAttributeIndexFactory);
} // namespace indexlibv2::table
