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
#include "indexlib/index/primary_key/PrimaryKeyAttributeReader.h"

#include "indexlib/index/primary_key/PrimaryKeyDiskIndexer.h"
#include "indexlib/index/primary_key/PrimaryKeyWriter.h"
#include "indexlib/index/primary_key/config/PrimaryKeyIndexConfig.h"

namespace indexlibv2::index {
AUTIL_LOG_SETUP_TEMPLATE(indexlib.index, PrimaryKeyAttributeReader, Key);

template <typename Key>
Status PrimaryKeyAttributeReader<Key>::DoOpen(const std::shared_ptr<config::IIndexConfig>& indexConfig,
                                              const std::vector<AttributeReader::IndexerInfo>& indexers)
{
    auto primaryKeyIndexConfig = std::dynamic_pointer_cast<index::PrimaryKeyIndexConfig>(indexConfig);
    assert(primaryKeyIndexConfig->HasPrimaryKeyAttribute());
    std::vector<AttributeReader::IndexerInfo> attributeIndexers;
    for (auto& [indexer, segment, baseDocId] : indexers) {
        auto segStatus = segment->GetSegmentStatus();
        if (framework::Segment::SegmentStatus::ST_BUILT == segStatus) {
            auto pkDiskIndexer = std::dynamic_pointer_cast<PrimaryKeyDiskIndexer<Key>>(indexer);
            auto pkDirectory = segment->GetSegmentDirectory()->GetDirectory(indexConfig->GetIndexPath()[0], false);
            assert(pkDirectory);
            assert(pkDiskIndexer);
            auto status =
                pkDiskIndexer->OpenPKAttribute(indexConfig, (pkDirectory ? pkDirectory->GetIDirectory() : nullptr));
            RETURN_IF_STATUS_ERROR(status, "open pk attribute failed at [%s]", pkDirectory->DebugString().c_str());
            attributeIndexers.emplace_back(pkDiskIndexer->GetPKAttributeDiskIndexer(), segment, baseDocId);
        } else if (framework::Segment::SegmentStatus::ST_DUMPING == segStatus ||
                   framework::Segment::SegmentStatus::ST_BUILDING == segStatus) {
            auto primaryKeyWriter = std::dynamic_pointer_cast<PrimaryKeyWriter<Key>>(indexer);
            assert(primaryKeyWriter);
            attributeIndexers.emplace_back(primaryKeyWriter->GetPKAttributeMemIndexer(), segment, baseDocId);
        } else {
            assert(false);
        }
    }
    return this->template InitAllIndexers<SingleValueAttributeDiskIndexer<Key>, SingleValueAttributeMemIndexer<Key>,
                                          SingleValueAttributeMemReader<Key>>(
        primaryKeyIndexConfig->GetPKAttributeConfig(), attributeIndexers, this->_onDiskIndexers, this->_memReaders,
        this->_defaultValueReader);
}

template class PrimaryKeyAttributeReader<uint64_t>;
template class PrimaryKeyAttributeReader<autil::uint128_t>;
} // namespace indexlibv2::index
