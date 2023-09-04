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

#include "autil/mem_pool/Pool.h"
#include "indexlib/base/Status.h"
#include "indexlib/document/IDocumentBatch.h"
#include "indexlib/index/IIndexer.h"

namespace indexlib::document {
class Document;
}

namespace indexlibv2::config {
class IIndexConfig;
}

namespace indexlibv2::document {
class IIndexFields;
namespace extractor {
class IDocumentInfoExtractorFactory;
}
} // namespace indexlibv2::document

namespace indexlibv2::framework {
struct DumpParams;
}

namespace indexlib::file_system {
class Directory;
}

namespace indexlib::framework {
class SegmentMetrics;
}

namespace indexlibv2::index {

class BuildingIndexMemoryUseUpdater;

class IMemIndexer : public IIndexer
{
public:
    IMemIndexer() = default;
    virtual ~IMemIndexer() = default;

    virtual Status Init(const std::shared_ptr<config::IIndexConfig>& indexConfig,
                        document::extractor::IDocumentInfoExtractorFactory* docInfoExtractorFactory) = 0;
    // deprecated
    virtual Status Build(document::IDocumentBatch* docBatch) = 0;
    virtual Status Build(const document::IIndexFields* indexFields, size_t n) = 0;
    virtual Status Dump(autil::mem_pool::PoolBase* dumpPool,
                        const std::shared_ptr<indexlib::file_system::Directory>& indexDirectory,
                        const std::shared_ptr<framework::DumpParams>& params) = 0;
    virtual void ValidateDocumentBatch(document::IDocumentBatch* docBatch) = 0;
    virtual bool IsValidDocument(document::IDocument* doc) = 0;
    virtual bool IsValidField(const document::IIndexFields* fields) = 0;
    virtual void FillStatistics(const std::shared_ptr<indexlib::framework::SegmentMetrics>& segmentMetrics) = 0;
    virtual void UpdateMemUse(BuildingIndexMemoryUseUpdater* memUpdater) = 0;
    virtual std::string GetIndexName() const = 0;
    virtual autil::StringView GetIndexType() const = 0;
    virtual void Seal() = 0;
    virtual bool IsDirty() const = 0;
};

} // namespace indexlibv2::index
