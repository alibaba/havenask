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
#include "autil/NoCopyable.h"
#include "indexlib/base/Status.h"
#include "indexlib/base/Types.h"
#include "indexlib/index/IndexReaderParameter.h"

namespace indexlibv2 {
namespace framework {
class TabletData;
}
namespace config {
class IIndexConfig;
class InvertedIndexConfig;
class PackageIndexConfig;
} // namespace config
} // namespace indexlibv2

namespace indexlib::index {
class SectionAttributeReader;

class IndexAccessoryReader : private autil::NoCopyable
{
public:
    using SectionReaderPosMap = std::map<std::string, size_t>;
    using SectionReaderVec = std::vector<std::shared_ptr<SectionAttributeReader>>;

    using ShareSectionIndexNames = std::vector<std::string>;
    using ShareSectionMap = std::map<std::string, ShareSectionIndexNames>;

public:
    IndexAccessoryReader(const std::vector<std::shared_ptr<indexlibv2::config::IIndexConfig>>& indexConfigs,
                         const indexlibv2::index::IndexReaderParameter& indexerParam);
    IndexAccessoryReader(const IndexAccessoryReader& other);
    ~IndexAccessoryReader();

public:
    Status Open(const indexlibv2::framework::TabletData* tabletData);
    const std::shared_ptr<SectionAttributeReader> GetSectionReader(const std::string& indexName) const;
    IndexAccessoryReader* Clone() const;

private:
    void CloneSectionReaders(const SectionReaderVec& srcReaders, SectionReaderVec& clonedReaders);

    bool CheckSectionConfig(const std::shared_ptr<indexlibv2::config::InvertedIndexConfig>& indexConfig) const;

    Status InitSectionReader(const std::shared_ptr<indexlibv2::config::InvertedIndexConfig>& indexConfig,
                             const indexlibv2::framework::TabletData* tabletData, ShareSectionMap& shareMap);

    void AddSharedRelations(const std::shared_ptr<indexlibv2::config::PackageIndexConfig>& packIndexConfig,
                            ShareSectionMap& shareMap);

    bool UseSharedSectionReader(const std::shared_ptr<indexlibv2::config::PackageIndexConfig>& packIndexConfig);

    indexlibv2::config::SortPattern
    GetSortPattern(const std::shared_ptr<indexlibv2::config::PackageIndexConfig>& indexConfig) const;

private:
    std::vector<std::shared_ptr<indexlibv2::config::IIndexConfig>> _indexConfigs;

    SectionReaderPosMap _sectionReaderMap;
    SectionReaderVec _sectionReaderVec;
    indexlibv2::index::IndexReaderParameter::SortPatternFunc _sortPatternFunc;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlib::index
