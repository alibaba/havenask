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
#include "gmock/gmock.h"
#include <memory>

#include "indexlib/document/IIndexFieldsParser.h"
#include "indexlib/index/IIndexFactory.h"
#include "indexlib/index/IIndexMerger.h"

namespace indexlib::util {
class BuildResourceMetrics;
}

namespace indexlibv2::config {
class IIndexConfig;
} // namespace indexlibv2::config

namespace indexlibv2::index {

class IDiskIndexer;
class IMemIndexer;
class IIndexReader;

class MockIndexFactory : public IIndexFactory
{
public:
    MockIndexFactory() = default;
    ~MockIndexFactory() = default;

    MOCK_METHOD(std::shared_ptr<IDiskIndexer>, CreateDiskIndexer,
                (const std::shared_ptr<config::IIndexConfig>&, const DiskIndexerParameter&), (const, override));
    MOCK_METHOD(std::shared_ptr<IMemIndexer>, CreateMemIndexer,
                (const std::shared_ptr<config::IIndexConfig>&, const MemIndexerParameter&), (const, override));
    MOCK_METHOD(std::unique_ptr<IIndexReader>, CreateIndexReader,
                (const std::shared_ptr<config::IIndexConfig>&, const IndexReaderParameter&), (const, override));
    MOCK_METHOD(std::unique_ptr<IIndexMerger>, CreateIndexMerger, (const std::shared_ptr<config::IIndexConfig>&),
                (const, override));
    MOCK_METHOD(std::unique_ptr<config::IIndexConfig>, CreateIndexConfig, (const autil::legacy::Any&),
                (const, override));
    MOCK_METHOD(std::string, GetIndexPath, (), (const, override));
    MOCK_METHOD(std::unique_ptr<document::IIndexFieldsParser>, CreateIndexFieldsParser, (), (override));
};

} // namespace indexlibv2::index
