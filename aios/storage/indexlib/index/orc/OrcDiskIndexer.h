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
#include "indexlib/index/IDiskIndexer.h"

namespace indexlibv2::config {
class OrcConfig;
}

namespace indexlib::file_system {
class FileReader;
}

namespace indexlibv2::index {

class IOrcReader;

class OrcDiskIndexer final : public IDiskIndexer
{
public:
    OrcDiskIndexer() = default;
    ~OrcDiskIndexer() = default;

public:
    Status Open(const std::shared_ptr<config::IIndexConfig>& indexConfig,
                const std::shared_ptr<indexlib::file_system::IDirectory>& indexDirectory) override;

public:
    const std::shared_ptr<IOrcReader>& GetReader() const { return _reader; }
    size_t EstimateMemUsed(const std::shared_ptr<config::IIndexConfig>& indexConfig,
                           const std::shared_ptr<indexlib::file_system::IDirectory>& indexDirectory) override;
    size_t EvaluateCurrentMemUsed() override;

    Status TEST_PrepareOrcLeafReader(const std::shared_ptr<indexlib::file_system::FileReader>& fileReader,
                                     const std::shared_ptr<config::OrcConfig>& orcConfig)
    {
        return PrepareOrcLeafReader(fileReader, orcConfig);
    }

private:
    Status PrepareOrcLeafReader(const std::shared_ptr<indexlib::file_system::FileReader>& fileReader,
                                const std::shared_ptr<config::OrcConfig>& orcConfig);

    std::shared_ptr<IOrcReader> _reader; // TODO: maybe use std::unique_ptr

    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::index
