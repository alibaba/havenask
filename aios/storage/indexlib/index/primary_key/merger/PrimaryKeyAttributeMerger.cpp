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
#include "indexlib/index/primary_key/merger/PrimaryKeyAttributeMerger.h"

#include "indexlib/index/primary_key/PrimaryKeyDiskIndexer.h"

namespace indexlibv2::index {
AUTIL_LOG_SETUP_TEMPLATE(indexlib.index, PrimaryKeyAttributeMerger, Key);

template <typename Key>
std::pair<Status, std::shared_ptr<IIndexer>>
PrimaryKeyAttributeMerger<Key>::GetDiskIndexer(const std::shared_ptr<framework::Segment>& segment)
{
    auto [status, diskIndexer] = segment->GetIndexer(_pkIndexConfig->GetIndexType(), _pkIndexConfig->GetIndexName());
    RETURN2_IF_STATUS_ERROR(status, nullptr, "failed to get pk disk indexer for segment [%d]", segment->GetSegmentId());
    assert(_pkIndexConfig->GetIndexPath().size() == 1);
    auto pkDirectory = segment->GetSegmentDirectory()->GetDirectory(_pkIndexConfig->GetIndexPath()[0], false);
    if (!pkDirectory) {
        RETURN2_IF_STATUS_ERROR(Status::Corruption(), nullptr, "get pk directory failed for segment [%d]",
                                segment->GetSegmentId());
    }
    auto pkDiskIndexer = std::dynamic_pointer_cast<PrimaryKeyDiskIndexer<Key>>(diskIndexer);
    assert(pkDiskIndexer);
    status = pkDiskIndexer->OpenPKAttribute(_pkIndexConfig, (pkDirectory ? pkDirectory->GetIDirectory() : nullptr));
    RETURN2_IF_STATUS_ERROR(status, nullptr, "open pk attribute for segment [%d] failed", segment->GetSegmentId());
    return {Status::OK(), pkDiskIndexer->GetPKAttributeDiskIndexer()};
}

template <typename Key>
std::pair<Status, std::shared_ptr<indexlib::file_system::IDirectory>>
PrimaryKeyAttributeMerger<Key>::GetOutputDirectory(const std::shared_ptr<indexlib::file_system::IDirectory>& segDir)
{
    return segDir->MakeDirectory(_pkIndexConfig->GetIndexPath()[0], indexlib::file_system::DirectoryOption())
        .StatusWith();
}

template class PrimaryKeyAttributeMerger<uint64_t>;
template class PrimaryKeyAttributeMerger<autil::uint128_t>;

} // namespace indexlibv2::index
