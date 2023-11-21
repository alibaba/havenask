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

#include <memory>

#include "autil/Log.h"
#include "indexlib/document/IIndexFieldsParser.h"
#include "indexlib/index/IndexFactoryCreator.h"

namespace indexlib::util {
template <typename T>
class PooledUniquePtr;
}

namespace autil::legacy {
class Any;
}

namespace autil::mem_pool {
class Pool;
}

namespace indexlibv2::config {
class IIndexConfig;
} // namespace indexlibv2::config

namespace indexlibv2::index {

class IDiskIndexer;
class IMemIndexer;
class IIndexReader;
class IIndexMerger;
struct DiskIndexerParameter;
struct MemIndexerParameter;
struct IndexReaderParameter;

class IIndexFactory
{
public:
    IIndexFactory() = default;
    virtual ~IIndexFactory() = default;

    virtual std::shared_ptr<IDiskIndexer> CreateDiskIndexer(const std::shared_ptr<config::IIndexConfig>& indexConfig,
                                                            const DiskIndexerParameter& indexerParam) const = 0;
    virtual std::shared_ptr<IMemIndexer> CreateMemIndexer(const std::shared_ptr<config::IIndexConfig>& indexConfig,
                                                          const MemIndexerParameter& memIndexerParam) const = 0;
    virtual std::unique_ptr<IIndexReader> CreateIndexReader(const std::shared_ptr<config::IIndexConfig>& indexConfig,
                                                            const IndexReaderParameter& indexReaderParam) const = 0;
    virtual std::unique_ptr<IIndexMerger>
    CreateIndexMerger(const std::shared_ptr<config::IIndexConfig>& indexConfig) const = 0;
    virtual std::unique_ptr<config::IIndexConfig> CreateIndexConfig(const autil::legacy::Any& any) const = 0;
    virtual std::string GetIndexPath() const = 0;
    virtual std::unique_ptr<document::IIndexFieldsParser> CreateIndexFieldsParser() = 0;
};

#define REGISTER_INDEX_FACTORY(INDEX_TYPE, INDEX_FACTORY)                                                              \
    __attribute__((constructor)) static void Register##INDEX_TYPE##Factory()                                           \
    {                                                                                                                  \
        auto indexFactoryCreator = indexlibv2::index::IndexFactoryCreator::GetInstance();                              \
        indexFactoryCreator->Register(#INDEX_TYPE, (new INDEX_FACTORY));                                               \
    }

} // namespace indexlibv2::index
