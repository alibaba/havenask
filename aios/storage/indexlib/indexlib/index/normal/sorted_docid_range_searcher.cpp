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
#include "indexlib/index/normal/sorted_docid_range_searcher.h"

#include "indexlib/index/normal/attribute/accessor/attribute_reader.h"
#include "indexlib/index/normal/attribute/accessor/multi_field_attribute_reader.h"
#include "indexlib/util/Exception.h"

using namespace std;
using namespace indexlib::index_base;

namespace indexlib { namespace index {
IE_LOG_SETUP(index, SortedDocidRangeSearcher);

struct DocIdRangeCompare {
    bool operator()(const DocIdRange& i, const DocIdRange& j) { return i.first < j.first; }
};

SortedDocidRangeSearcher::SortedDocidRangeSearcher() {}

SortedDocidRangeSearcher::~SortedDocidRangeSearcher() {}

void SortedDocidRangeSearcher::Init(const MultiFieldAttributeReaderPtr& attrReaders, const PartitionMeta& partitionMeta,
                                    const vector<string>& sortAttributes)
{
    mPartMeta = partitionMeta;
    for (const string& sortFieldName : sortAttributes) {
        AttributeReaderPtr sortAttrReader = attrReaders->GetAttributeReader(sortFieldName);
        if (!sortAttrReader) {
            INDEXLIB_FATAL_ERROR(NonExist,
                                 "GetAttributeReader "
                                 "for sort attribute field[%s] failed!",
                                 sortFieldName.c_str());
        }
        mSortAttrReaderMap.insert(std::make_pair(sortFieldName, sortAttrReader));
        IE_LOG(INFO, "sort range search init attribute [%s]", sortFieldName.c_str());
    }
}

bool SortedDocidRangeSearcher::GetSortedDocIdRanges(const table::DimensionDescriptionVector& dimensions,
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

bool SortedDocidRangeSearcher::RecurGetSortedDocIdRanges(const table::DimensionDescriptionVector& dimensions,
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

bool SortedDocidRangeSearcher::GetSortedDocIdRanges(const std::shared_ptr<table::DimensionDescription>& dimension,
                                                    const DocIdRange& rangeLimit, DocIdRangeVector& resultRanges) const
{
    const AttributeReaderPtr& attrReader = GetAttributeReader(dimension->name);
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
        table::DimensionDescription::Range rangeDescription(dimension->values[i], dimension->values[i]);
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

bool SortedDocidRangeSearcher::ValidateDimensions(const table::DimensionDescriptionVector& dimensions) const
{
    size_t count = dimensions.size();
    const indexlibv2::config::SortDescriptions& sortDescs = mPartMeta.GetSortDescriptions();
    if (count > sortDescs.size() || dimensions[count - 1]->name != sortDescs[count - 1].GetSortFieldName()) {
        IE_LOG(WARN, "dimensions are inconsistent with partMeta");
        return false;
    }

    for (size_t i = 0; i + 1 < count; ++i) {
        if (dimensions[i]->name != sortDescs[i].GetSortFieldName()) {
            IE_LOG(WARN, "dimensions inconsistent with partMeta");
            return false;
        }
        if (!dimensions[i]->ranges.empty()) {
            IE_LOG(WARN, "high dimension %s can't have range content", dimensions[i]->name.c_str());
            return false;
        }
    }
    return true;
}

void SortedDocidRangeSearcher::AppendRanges(const DocIdRangeVector& src, DocIdRangeVector& dest) const
{
    dest.insert(dest.end(), src.begin(), src.end());
}

const AttributeReaderPtr& SortedDocidRangeSearcher::GetAttributeReader(const string& attrName) const
{
    AttributeReaderMap::const_iterator it = mSortAttrReaderMap.find(attrName);
    if (it == mSortAttrReaderMap.end()) {
        static AttributeReaderPtr nullReader;
        return nullReader;
    }
    return it->second;
}
}} // namespace indexlib::index
