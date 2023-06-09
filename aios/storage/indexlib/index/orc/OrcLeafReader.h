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

#include "indexlib/base/Status.h"
#include "indexlib/index/orc/IOrcReader.h"

namespace orc {
class Reader;
class ReaderOptions;
} // namespace orc

namespace indexlib { namespace file_system {
class FileReader;
}} // namespace indexlib::file_system

namespace indexlibv2 { namespace config {
class OrcConfig;
}} // namespace indexlibv2::config

namespace indexlibv2::index {

class OrcLeafReader : public IOrcReader
{
private:
    struct OrcFileMeta {
        uint64_t totalRowCount = 0;
        std::string fileTailStr;
    };

public:
    OrcLeafReader();
    ~OrcLeafReader();

public:
    Status Init(const std::shared_ptr<indexlib::file_system::FileReader>& fileReader, const config::OrcConfig* config);

    Status CreateIterator(const ReaderOptions& opts, std::unique_ptr<IOrcIterator>& orcIter) override;

    uint64_t GetTotalRowCount() const override { return _meta.totalRowCount; }
    virtual size_t EvaluateCurrentMemUsed() const override;

private:
    Status CreateOrcReader(const ReaderOptions& opts, std::unique_ptr<orc::Reader>& orcReader) const;

private:
    const config::OrcConfig* _orcConfig = nullptr;
    std::shared_ptr<indexlib::file_system::FileReader> _fileReader;
    OrcFileMeta _meta;
};

} // namespace indexlibv2::index
