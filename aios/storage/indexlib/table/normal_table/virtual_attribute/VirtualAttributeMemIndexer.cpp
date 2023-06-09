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
#include "indexlib/table/normal_table/virtual_attribute/VirtualAttributeMemIndexer.h"

#include "indexlib/index/attribute/AttributeMemIndexer.h"
#include "indexlib/index/attribute/config/AttributeConfig.h"
#include "indexlib/table/normal_table/virtual_attribute/Common.h"
#include "indexlib/table/normal_table/virtual_attribute/VirtualAttributeConfig.h"

namespace indexlibv2::table {
AUTIL_LOG_SETUP(indexlib.table, VirtualAttributeMemIndexer);

VirtualAttributeMemIndexer::VirtualAttributeMemIndexer(const std::shared_ptr<index::IMemIndexer>& attrMemIndexer)
    : _impl(attrMemIndexer)
{
}

VirtualAttributeMemIndexer::~VirtualAttributeMemIndexer() {}

Status VirtualAttributeMemIndexer::Init(const std::shared_ptr<config::IIndexConfig>& indexConfig,
                                        document::extractor::IDocumentInfoExtractorFactory* docInfoExtractorFactory)
{
    auto virtualAttrConfig = std::dynamic_pointer_cast<VirtualAttributeConfig>(indexConfig);
    assert(virtualAttrConfig);
    auto attrConfig =
        std::dynamic_pointer_cast<indexlibv2::config::AttributeConfig>(virtualAttrConfig->GetAttributeConfig());
    assert(attrConfig);
    return _impl->Init(attrConfig, docInfoExtractorFactory);
}
Status VirtualAttributeMemIndexer::Build(document::IDocumentBatch* docBatch) { return _impl->Build(docBatch); }
Status VirtualAttributeMemIndexer::Build(const document::IIndexFields* indexFields, size_t n)
{
    return _impl->Build(indexFields, n);
}
bool VirtualAttributeMemIndexer::IsDirty() const { return _impl->IsDirty(); }
Status VirtualAttributeMemIndexer::Dump(autil::mem_pool::PoolBase* dumpPool,
                                        const std::shared_ptr<indexlib::file_system::Directory>& indexDirectory,
                                        const std::shared_ptr<framework::DumpParams>& params)
{
    return _impl->Dump(dumpPool, indexDirectory, params);
}
void VirtualAttributeMemIndexer::ValidateDocumentBatch(document::IDocumentBatch* docBatch)
{
    _impl->ValidateDocumentBatch(docBatch);
}
bool VirtualAttributeMemIndexer::IsValidDocument(document::IDocument* doc) { return _impl->IsValidDocument(doc); }
void VirtualAttributeMemIndexer::FillStatistics(
    const std::shared_ptr<indexlib::framework::SegmentMetrics>& segmentMetrics)
{
    _impl->FillStatistics(segmentMetrics);
}
void VirtualAttributeMemIndexer::UpdateMemUse(index::BuildingIndexMemoryUseUpdater* memUpdater)
{
    _impl->UpdateMemUse(memUpdater);
}
std::string VirtualAttributeMemIndexer::GetIndexName() const { return _impl->GetIndexName(); }
autil::StringView VirtualAttributeMemIndexer::GetIndexType() const { return VIRTUAL_ATTRIBUTE_INDEX_TYPE_STR; }
void VirtualAttributeMemIndexer::Seal() { _impl->Seal(); }
std::shared_ptr<index::IMemIndexer> VirtualAttributeMemIndexer::GetMemIndexer() const { return _impl; }

Status VirtualAttributeMemIndexer::AddDocument(document::IDocument* doc)
{
    return std::dynamic_pointer_cast<index::AttributeMemIndexer>(_impl)->AddDocument(doc);
}

bool VirtualAttributeMemIndexer::UpdateField(docid_t docId, const autil::StringView& attributeValue, bool isNull,
                                             const uint64_t* hashKey)
{
    return std::dynamic_pointer_cast<index::AttributeMemIndexer>(_impl)->UpdateField(docId, attributeValue, isNull,
                                                                                     hashKey);
}

bool VirtualAttributeMemIndexer::IsValidField(const document::IIndexFields* fields)
{
    return _impl->IsValidField(fields);
}

} // namespace indexlibv2::table
