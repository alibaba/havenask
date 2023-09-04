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
#include "indexlib/base/Status.h"
#include "indexlib/document/normal/IndexDocument.h"
#include "indexlib/document/normal/ModifiedTokens.h"
#include "indexlib/index/IndexerParameter.h"
#include "indexlib/index/inverted_index/IInvertedDiskIndexer.h"
#include "indexlib/index/inverted_index/InvertedLeafReader.h"
#include "indexlib/index/inverted_index/builtin_index/dynamic/DynamicMemIndexer.h"
#include "indexlib/index/inverted_index/format/IndexFormatOption.h"

namespace indexlib::file_system {
class FileReader;
}

namespace indexlibv2::config {
class InvertedIndexConfig;
}
namespace indexlib::index {
class BitmapDiskIndexer;
class DynamicPostingTable;

class InvertedDiskIndexer : public IInvertedDiskIndexer
{
public:
    explicit InvertedDiskIndexer(const indexlibv2::index::IndexerParameter& indexerParam);
    ~InvertedDiskIndexer() = default;

    Status Open(const std::shared_ptr<indexlibv2::config::IIndexConfig>& indexConfig,
                const std::shared_ptr<file_system::IDirectory>& indexDirectory) override;
    size_t EstimateMemUsed(const std::shared_ptr<indexlibv2::config::IIndexConfig>& indexConfig,
                           const std::shared_ptr<file_system::IDirectory>& indexDirectory) override;
    size_t EvaluateCurrentMemUsed() override;

    void UpdateTokens(docid_t localDocId, const document::ModifiedTokens& modifiedTokens) override;
    void UpdateTerms(IndexUpdateTermIterator* termIter) override;
    void UpdateBuildResourceMetrics(size_t& poolMemory, size_t& totalMemory, size_t& totalRetiredMemory,
                                    size_t& totalDocCount, size_t& totalAlloc, size_t& totalFree,
                                    size_t& totalTreeCount) const override;

public:
    void UpdateOneTerm(docid_t localDocId, const DictKeyInfo& dictKeyInfo, document::ModifiedTokens::Operation op);
    virtual std::shared_ptr<InvertedLeafReader> GetReader() const { return _leafReader; }
    std::shared_ptr<indexlibv2::config::InvertedIndexConfig> GetIndexConfig() const { return _indexConfig; }
    std::shared_ptr<BitmapDiskIndexer> GetBitmapDiskIndexer() const { return _bitmapDiskIndexer; }

    std::shared_ptr<file_system::ResourceFile> GetDynamicPostingResource() const { return _dynamicPostingResource; }
    uint64_t GetDocCount() const { return _indexerParam.docCount; }

    std::shared_ptr<IDiskIndexer> GetSectionAttributeDiskIndexer() const override;

private:
    Status OpenSectionAttrDiskIndexer(const std::shared_ptr<indexlibv2::config::InvertedIndexConfig>& config,
                                      const std::shared_ptr<file_system::IDirectory>& indexDir,
                                      const IndexFormatOption& formatOption);
    Status InitDynamicIndexer(const std::shared_ptr<file_system::IDirectory>& indexDir);
    void DoUpdateToken(const DictKeyInfo& termKey, bool isNumberIndex,
                       DynamicMemIndexer::DynamicPostingTable* postingTable, docid_t docId,
                       document::ModifiedTokens::Operation op);
    size_t
    EstimateSectionAttributeMemUse(const std::shared_ptr<indexlibv2::config::InvertedIndexConfig>& invertedConfig,
                                   const std::shared_ptr<file_system::IDirectory>& indexDirectory) const;

protected:
    indexlibv2::index::IndexerParameter _indexerParam;

private:
    IndexFormatOption _indexFormatOption;
    std::shared_ptr<file_system::Directory> _indexDirectory;
    std::shared_ptr<InvertedLeafReader> _leafReader;
    std::shared_ptr<BitmapDiskIndexer> _bitmapDiskIndexer;
    std::shared_ptr<file_system::ResourceFile> _dynamicPostingResource;
    std::shared_ptr<indexlibv2::config::InvertedIndexConfig> _indexConfig;
    std::shared_ptr<IDiskIndexer> _sectionAttrDiskIndexer;

    AUTIL_LOG_DECLARE();
};

} // namespace indexlib::index
