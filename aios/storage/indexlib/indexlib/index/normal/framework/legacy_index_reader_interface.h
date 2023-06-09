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
#include "indexlib/config/index_config.h"
#include "indexlib/document/normal/ModifiedTokens.h"
#include "indexlib/index/inverted_index/InvertedIndexReader.h"
#include "indexlib/index_base/index_meta/version.h"

namespace indexlib::index_base {
class PartitionData;
}

namespace indexlib::index {

class LegacyIndexReaderInterface
{
public:
    LegacyIndexReaderInterface() {}
    virtual ~LegacyIndexReaderInterface() {}

public:
    // hintReader is used to reduce duplicated access to file_system and FronJsonString()
    // notice hintReader may be nullptr
    virtual void Open(const std::shared_ptr<config::IndexConfig>& indexConfig,
                      const std::shared_ptr<index_base::PartitionData>& partitionData,
                      const InvertedIndexReader* hintReader = nullptr) = 0;
    virtual size_t EstimateLoadSize(const std::shared_ptr<index_base::PartitionData>& partitionData,
                                    const std::shared_ptr<config::IndexConfig>& indexConfig,
                                    const index_base::Version& lastLoadVersion) = 0;
    virtual void SetMultiFieldIndexReader(InvertedIndexReader* truncateReader) = 0;
    virtual void InitBuildResourceMetricsNode(util::BuildResourceMetrics* buildResourceMetrics) = 0;
    virtual void SetLegacyAccessoryReader(const std::shared_ptr<LegacyIndexAccessoryReader>& accessorReader) = 0;
    virtual std::shared_ptr<LegacyIndexAccessoryReader> GetLegacyAccessoryReader() const = 0;

    virtual void Update(docid_t docId, const document::ModifiedTokens& tokens) = 0;
    virtual void Update(docid_t docId, index::DictKeyInfo termKey, bool isDelete) = 0;
    // UpdateIndex is used to update patch format index.
    virtual void UpdateIndex(IndexUpdateTermIterator* iter) = 0;
};

} // namespace indexlib::index
