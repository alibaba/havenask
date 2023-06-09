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

#include <map>
#include <memory>
#include <vector>

#include "indexlib/base/Types.h"
#include "indexlib/index/IIndexReader.h"
#include "indexlib/index/orc/IOrcReader.h"

namespace indexlibv2 { namespace config {
class OrcConfig;
}} // namespace indexlibv2::config

namespace indexlibv2::index {

class OrcReader final : public IIndexReader, public IOrcReader
{
public:
    OrcReader();
    OrcReader(std::map<segmentid_t, std::shared_ptr<IOrcReader>> segmentReaders);

    // for slice
    OrcReader(std::map<segmentid_t, std::shared_ptr<IOrcReader>> segmentReaders,
              const std::shared_ptr<config::OrcConfig>& config);

public:
    Status Open(const std::shared_ptr<config::IIndexConfig>& indexConfig,
                const framework::TabletData* tabletData) override;

    Status CreateIterator(const ReaderOptions& opts, std::unique_ptr<IOrcIterator>& orcIter) override;
    uint64_t GetTotalRowCount() const override;
    size_t EvaluateCurrentMemUsed() const override;

public:
    std::unique_ptr<OrcReader> Slice(const std::vector<segmentid_t>& segmentIds);
    const std::shared_ptr<config::OrcConfig>& GetConfig() const;

private:
    std::map<segmentid_t, std::shared_ptr<IOrcReader>> _segmentReaders;
    std::shared_ptr<config::OrcConfig> _config;
};

} // namespace indexlibv2::index
