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
#include "indexlib/table/normal_table/virtual_attribute/VirtualAttributeDiskIndexer.h"

#include "indexlib/index/attribute/MultiSliceAttributeDiskIndexer.h"
#include "indexlib/index/attribute/config/AttributeConfig.h"
#include "indexlib/table/normal_table/virtual_attribute/VirtualAttributeConfig.h"

namespace indexlibv2::table {
AUTIL_LOG_SETUP(indexlib.table, VirtualAttributeDiskIndexer);

VirtualAttributeDiskIndexer::VirtualAttributeDiskIndexer(const std::shared_ptr<index::IDiskIndexer>& attrDiskIndexer)
    : _impl(attrDiskIndexer)
{
}

VirtualAttributeDiskIndexer::~VirtualAttributeDiskIndexer() {}

Status VirtualAttributeDiskIndexer::Open(const std::shared_ptr<config::IIndexConfig>& indexConfig,
                                         const std::shared_ptr<indexlib::file_system::IDirectory>& indexDirectory)
{
    auto virtualAttrConfig = std::dynamic_pointer_cast<VirtualAttributeConfig>(indexConfig);
    assert(virtualAttrConfig);
    auto attrConfig =
        std::dynamic_pointer_cast<indexlibv2::config::AttributeConfig>(virtualAttrConfig->GetAttributeConfig());
    assert(attrConfig);
    return _impl->Open(attrConfig, indexDirectory);
}
size_t
VirtualAttributeDiskIndexer::EstimateMemUsed(const std::shared_ptr<config::IIndexConfig>& indexConfig,
                                             const std::shared_ptr<indexlib::file_system::IDirectory>& indexDirectory)
{
    auto virtualAttrConfig = std::dynamic_pointer_cast<VirtualAttributeConfig>(indexConfig);
    assert(virtualAttrConfig);
    auto attrConfig =
        std::dynamic_pointer_cast<indexlibv2::config::AttributeConfig>(virtualAttrConfig->GetAttributeConfig());
    assert(attrConfig);
    return _impl->EstimateMemUsed(attrConfig, indexDirectory);
}
size_t VirtualAttributeDiskIndexer::EvaluateCurrentMemUsed() { return _impl->EvaluateCurrentMemUsed(); }
std::shared_ptr<index::IDiskIndexer> VirtualAttributeDiskIndexer::GetDiskIndexer() const
{
    auto multiSliceDiskIndexer = std::dynamic_pointer_cast<indexlibv2::index::MultiSliceAttributeDiskIndexer>(_impl);
    assert(multiSliceDiskIndexer);
    assert(multiSliceDiskIndexer->GetSliceCount() == 1);
    return multiSliceDiskIndexer->GetSliceIndexer<index::IDiskIndexer>(0);
}

bool VirtualAttributeDiskIndexer::UpdateField(docid_t docId, const autil::StringView& value, bool isNull,
                                              const uint64_t* hashKey)
{
    return std::dynamic_pointer_cast<index::AttributeDiskIndexer>(_impl)->UpdateField(docId, value, isNull, hashKey);
}

} // namespace indexlibv2::table
