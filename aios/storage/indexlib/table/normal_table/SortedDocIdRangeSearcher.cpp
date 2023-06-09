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
#include "indexlib/table/normal_table/SortedDocIdRangeSearcher.h"

#include "indexlib/index/attribute/AttributeReader.h"

using namespace std;

namespace indexlibv2 { namespace table {
AUTIL_LOG_SETUP(indexlib.table, SortedDocIdRangeSearcher);

struct DocIdRangeCompare {
    bool operator()(const DocIdRange& i, const DocIdRange& j) { return i.first < j.first; }
};

SortedDocIdRangeSearcher::SortedDocIdRangeSearcher(const AttributeReaderMap& sortAttrReaderMap,
                                                   const NormalTabletMeta& tabletMeta)
    : _sortAttrReaderMap(sortAttrReaderMap)
    , _normalTabletMeta(tabletMeta)
{
}

bool SortedDocIdRangeSearcher::GetSortedDocIdRanges(const indexlib::table::DimensionDescriptionVector& dimensions,
                                                    const DocIdRange& rangeLimit, DocIdRangeVector& resultRanges) const
{
    resultRanges.clear();

    if (dimensions.empty()) {
        resultRanges.push_back(rangeLimit);
        return true;
    }

    if (!ValidateDimensions(dimensions)) {
        return false;
    }

    DocIdRangeVector rangeLimits;
    rangeLimits.push_back(rangeLimit);
    return RecurGetSortedDocIdRanges(dimensions, rangeLimits, resultRanges, 0);
}

bool SortedDocIdRangeSearcher::RecurGetSortedDocIdRanges(const indexlib::table::DimensionDescriptionVector& dimensions,
                                                         DocIdRangeVector& rangeLimits, DocIdRangeVector& resultRanges,
                                                         size_t dimensionIdx) const
{
    for (size_t i = 0; i < rangeLimits.size(); ++i) {
        DocIdRangeVector pieceRanges;
        if (!GetSortedDocIdRanges(dimensions[dimensionIdx], rangeLimits[i], pieceRanges)) {
            return false;
        }
        AppendRanges(pieceRanges, resultRanges);
    }

    if (resultRanges.empty()) {
        return true;
    }

    if (dimensionIdx + 1 != dimensions.size()) {
        rangeLimits = resultRanges;
        resultRanges.clear();
        if (!RecurGetSortedDocIdRanges(dimensions, rangeLimits, resultRanges, dimensionIdx + 1)) {
            return false;
        }
    }
    return true;
}

bool SortedDocIdRangeSearcher::GetSortedDocIdRanges(
    const std::shared_ptr<indexlib::table::DimensionDescription>& dimension, const DocIdRange& rangeLimit,
    DocIdRangeVector& resultRanges) const
{
    std::shared_ptr<index::AttributeReader> attrReader = GetAttributeReader(dimension->name);
    if (!attrReader) {
        return false;
    }

    DocIdRange pieceRange;
    for (size_t i = 0; i < dimension->ranges.size(); ++i) {
        if (!attrReader->GetSortedDocIdRange(dimension->ranges[i], rangeLimit, pieceRange)) {
            return false;
        }
        if (pieceRange.second > pieceRange.first) {
            resultRanges.push_back(pieceRange);
        }
    }

    for (size_t i = 0; i < dimension->values.size(); ++i) {
        indexlib::table::DimensionDescription::Range rangeDescription(dimension->values[i], dimension->values[i]);
        if (!attrReader->GetSortedDocIdRange(rangeDescription, rangeLimit, pieceRange)) {
            return false;
        }
        if (pieceRange.second > pieceRange.first) {
            resultRanges.push_back(pieceRange);
        }
    }

    sort(resultRanges.begin(), resultRanges.end(), DocIdRangeCompare());
    return true;
}

bool SortedDocIdRangeSearcher::ValidateDimensions(const indexlib::table::DimensionDescriptionVector& dimensions) const
{
    size_t count = dimensions.size();
    const indexlibv2::config::SortDescriptions& sortDescs = _normalTabletMeta.GetSortDescriptions();
    if (count > sortDescs.size() || dimensions[count - 1]->name != sortDescs[count - 1].GetSortFieldName()) {
        AUTIL_LOG(WARN, "dimensions are inconsistent with NormalTabletMeta");
        return false;
    }

    for (size_t i = 0; i + 1 < count; ++i) {
        if (dimensions[i]->name != sortDescs[i].GetSortFieldName()) {
            AUTIL_LOG(WARN, "dimensions are inconsistent with NormalTabletMeta");
            return false;
        }
        if (!dimensions[i]->ranges.empty()) {
            AUTIL_LOG(WARN, "high dimension %s can't have range content", dimensions[i]->name.c_str());
            return false;
        }
    }
    return true;
}

void SortedDocIdRangeSearcher::AppendRanges(const DocIdRangeVector& src, DocIdRangeVector& dest) const
{
    dest.insert(dest.end(), src.begin(), src.end());
}

std::shared_ptr<index::AttributeReader> SortedDocIdRangeSearcher::GetAttributeReader(const string& attrName) const
{
    AttributeReaderMap::const_iterator it = _sortAttrReaderMap.find(attrName);
    if (it == _sortAttrReaderMap.end()) {
        return nullptr;
    }
    return it->second;
}

}} // namespace indexlibv2::table
