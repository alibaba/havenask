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
#include <unordered_map>

#include "indexlib/base/Types.h"
#include "indexlib/table/normal_table/DimensionDescription.h"
#include "indexlib/table/normal_table/NormalTabletMeta.h"

namespace indexlibv2::index {
class AttributeReader;
}

namespace indexlibv2 { namespace table {

class SortedDocIdRangeSearcher
{
public:
    using AttributeReaderMap = std::unordered_map<std::string, std::shared_ptr<index::AttributeReader>>;

    SortedDocIdRangeSearcher(const AttributeReaderMap& sortAttrReaderMap, const NormalTabletMeta& tabletMeta);
    ~SortedDocIdRangeSearcher() = default;

    // rangeLimit = [begin, end)
    bool GetSortedDocIdRanges(const indexlib::table::DimensionDescriptionVector& dimensions,
                              const DocIdRange& rangeLimit, DocIdRangeVector& resultRanges) const;

private:
    bool RecurGetSortedDocIdRanges(const indexlib::table::DimensionDescriptionVector& dimensions,
                                   DocIdRangeVector& rangeLimits, DocIdRangeVector& resultRanges,
                                   size_t dimensionIdx) const;

    bool GetSortedDocIdRanges(const std::shared_ptr<indexlib::table::DimensionDescription>& dimension,
                              const DocIdRange& rangeLimit, DocIdRangeVector& resultRanges) const;
    void AppendRanges(const DocIdRangeVector& src, DocIdRangeVector& dest) const;

    bool ValidateDimensions(const indexlib::table::DimensionDescriptionVector& dimensions) const;
    std::shared_ptr<index::AttributeReader> GetAttributeReader(const std::string& attrName) const;

private:
    AttributeReaderMap _sortAttrReaderMap;
    NormalTabletMeta _normalTabletMeta;

private:
    AUTIL_LOG_DECLARE();
};

}} // namespace indexlibv2::table
