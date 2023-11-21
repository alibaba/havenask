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
#pragma once

#include "autil/Log.h"
#include "future_lite/coro/Lazy.h"
#include "indexlib/base/Types.h"
#include "indexlib/file_system/file/ReadOption.h"
#include "indexlib/index/IIndexReader.h"
#include "indexlib/index/common/ErrorCode.h"
#include "indexlib/index/source/Types.h"
#include "indexlib/index/source/config/SourceIndexConfig.h"

namespace indexlib::document {
class SourceDocument;
class SerializedSourceDocument;
} // namespace indexlib::document

namespace autil::mem_pool {
class PoolBase;
}

namespace indexlibv2 { namespace index {
class SourceMemIndexer;
class SourceDiskIndexer;

class SourceReader : public IIndexReader
{
public:
    SourceReader();
    ~SourceReader();

public:
    Status Open(const std::shared_ptr<config::IIndexConfig>& indexConfig,
                const framework::TabletData* tabletData) override;
    Status GetDocument(docid_t docId, indexlib::document::SourceDocument* sourceDocument) const;
    Status GetDocument(docid_t docId, const std::vector<index::sourcegroupid_t>& requiredGroupdIds,
                       indexlib::document::SourceDocument* sourceDocument) const;

    future_lite::coro::Lazy<indexlib::index::ErrorCodeVec>
    GetDocumentAsync(const std::vector<docid_t>& docIds, const std::vector<index::sourcegroupid_t>& requiredGroupdIds,
                     autil::mem_pool::PoolBase* sessionPool, indexlib::file_system::ReadOption option,
                     const std::vector<indexlib::document::SourceDocument*>* sourceDocs) const;

    future_lite::coro::Lazy<indexlib::index::ErrorCodeVec>
    GetDocumentAsync(const std::vector<docid_t>& docIds, const std::vector<index::sourcegroupid_t>& requiredGroupdIds,
                     autil::mem_pool::PoolBase* sessionPool, indexlib::file_system::ReadOption option,
                     const std::vector<indexlib::document::SerializedSourceDocument*>* sourceDocs) const;
    std::shared_ptr<indexlibv2::config::SourceIndexConfig> GetSourceConfig() const { return _sourceConfig; }

private:
    docid_t _buildingBaseDocId = 0;
    std::vector<index::sourcegroupid_t> _allGroupIds;
    std::vector<std::shared_ptr<SourceMemIndexer>> _memIndexers;
    std::vector<docid_t> _inMemSegmentBaseDocId;
    std::vector<std::shared_ptr<SourceDiskIndexer>> _diskIndexers;
    std::vector<uint64_t> _segmentDocCount;
    std::shared_ptr<indexlibv2::config::SourceIndexConfig> _sourceConfig;

private:
    AUTIL_LOG_DECLARE();
};

}} // namespace indexlibv2::index
