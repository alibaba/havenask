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
#include "gmock/gmock.h"
#include <memory>

#include "autil/mem_pool/PoolBase.h"
#include "indexlib/base/Status.h"
#include "indexlib/index/IMemIndexer.h"

namespace indexlib::document {
class Document;
}

namespace indexlibv2::config {
class IIndexConfig;
}

namespace indexlib::file_system {
class Directory;
}

namespace indexlib::framework {
class SegmentMetrics;
}

namespace indexlibv2::index {

class BuildingIndexMemoryUseUpdater;

class MockMemIndexer : public IMemIndexer
{
public:
    MockMemIndexer() = default;
    ~MockMemIndexer() = default;

    MOCK_METHOD(Status, Init,
                (const std::shared_ptr<config::IIndexConfig>&, document::extractor::IDocumentInfoExtractorFactory*),
                (override));
    MOCK_METHOD(Status, Build, (document::IDocumentBatch*), (override));
    MOCK_METHOD(Status, Build, (const document::IIndexFields*, size_t), (override));
    MOCK_METHOD(Status, Dump,
                (autil::mem_pool::PoolBase*, const std::shared_ptr<indexlib::file_system::Directory>&,
                 const std::shared_ptr<framework::DumpParams>&),
                (override));
    MOCK_METHOD(void, ValidateDocumentBatch, (document::IDocumentBatch*), (override));
    MOCK_METHOD(bool, IsValidDocument, (document::IDocument*), (override));
    MOCK_METHOD(bool, IsValidField, (const document::IIndexFields*), (override));
    MOCK_METHOD(void, FillStatistics, (const std::shared_ptr<indexlib::framework::SegmentMetrics>&), (override));
    MOCK_METHOD(void, UpdateMemUse, (BuildingIndexMemoryUseUpdater*), (override));
    MOCK_METHOD(std::string, GetIndexName, (), (const, override));
    MOCK_METHOD(autil::StringView, GetIndexType, (), (const, override));
    MOCK_METHOD(bool, IsDirty, (), (const, override));
    MOCK_METHOD(void, Seal, (), (override));
};

} // namespace indexlibv2::index
