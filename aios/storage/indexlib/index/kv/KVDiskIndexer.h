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

#include "indexlib/index/IDiskIndexer.h"
#include "indexlib/index/kv/IKVSegmentReader.h"

namespace indexlibv2::config {
class KVIndexConfig;
}

namespace indexlib::file_system {
class Directory;
}

namespace indexlibv2::index {

class KVDiskIndexer : public IDiskIndexer
{
public:
    KVDiskIndexer() {}

    ~KVDiskIndexer() {}

public:
    Status Open(const std::shared_ptr<config::IIndexConfig>& indexConfig,
                const std::shared_ptr<indexlib::file_system::IDirectory>& indexDirectory) override;

    size_t EstimateMemUsed(const std::shared_ptr<config::IIndexConfig>& indexConfig,
                           const std::shared_ptr<indexlib::file_system::IDirectory>& indexDirectory) override;
    size_t EvaluateCurrentMemUsed() override;

    const std::shared_ptr<IKVSegmentReader>& GetReader() const { return _reader; }
    const std::shared_ptr<indexlibv2::config::KVIndexConfig>& GetIndexConfig() const { return _kvIndexConfig; }
    const std::shared_ptr<indexlib::file_system::Directory>& GetIndexDirectory() const { return _indexDirectory; }

private:
    std::shared_ptr<IKVSegmentReader> _reader;
    std::shared_ptr<indexlibv2::config::KVIndexConfig> _kvIndexConfig;
    std::shared_ptr<indexlib::file_system::Directory> _indexDirectory;
};

} // namespace indexlibv2::index
