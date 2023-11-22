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
#include "autil/NoCopyable.h"
#include "future_lite/coro/Lazy.h"
#include "indexlib/file_system/file/ReadOption.h"
#include "indexlib/index/DiskIndexerParameter.h"
#include "indexlib/index/IDiskIndexer.h"
#include "indexlib/index/common/ErrorCode.h"
#include "indexlib/index/source/Types.h"

namespace autil::mem_pool {
class PoolBase;
}
namespace indexlib::document {
class SerializedSourceDocument;
} // namespace indexlib::document

namespace indexlibv2::config {
class SourceIndexConfig;
}
namespace indexlibv2::index {
class VarLenDataReader;

class SourceDiskIndexer : public IDiskIndexer
{
public:
    SourceDiskIndexer(const DiskIndexerParameter& indexerParam);
    ~SourceDiskIndexer() = default;

public:
    Status Open(const std::shared_ptr<config::IIndexConfig>& indexConfig,
                const std::shared_ptr<indexlib::file_system::IDirectory>& indexDirectory) override;
    size_t EstimateMemUsed(const std::shared_ptr<config::IIndexConfig>& indexConfig,
                           const std::shared_ptr<indexlib::file_system::IDirectory>& indexDirectory) override;
    size_t EvaluateCurrentMemUsed() override;

    future_lite::coro::Lazy<indexlib::index::ErrorCodeVec>
    GetDocument(const std::vector<docid_t>& docIds, const std::vector<index::sourcegroupid_t>& requiredGroupdIds,
                autil::mem_pool::PoolBase* pool, indexlib::file_system::ReadOption readOption,
                const std::vector<indexlib::document::SerializedSourceDocument*>* docs) const;

private:
    std::shared_ptr<config::SourceIndexConfig> _sourceIndexConfig;
    std::vector<std::shared_ptr<VarLenDataReader>> _groupReaders;
    std::shared_ptr<VarLenDataReader> _metaReader;
    DiskIndexerParameter _indexerParam;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::index
