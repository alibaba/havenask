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

#include "indexlib/common_define.h"
#include "indexlib/index_base/index_meta/partition_meta.h"
#include "indexlib/indexlib.h"
#include "indexlib/table/normal_table/DimensionDescription.h"

DECLARE_REFERENCE_CLASS(index, MultiFieldAttributeReader);
DECLARE_REFERENCE_CLASS(index, AttributeReader);

namespace indexlib { namespace index {

class SortedDocidRangeSearcher
{
public:
    SortedDocidRangeSearcher();
    ~SortedDocidRangeSearcher();

public:
    void Init(const MultiFieldAttributeReaderPtr& attrReaders, const index_base::PartitionMeta& partitionMeta,
              const std::vector<std::string>& sortAttributes);

    bool GetSortedDocIdRanges(const table::DimensionDescriptionVector& dimensions, const DocIdRange& rangeLimit,
                              DocIdRangeVector& resultRanges) const;

private:
    bool RecurGetSortedDocIdRanges(const table::DimensionDescriptionVector& dimensions, DocIdRangeVector& rangeLimits,
                                   DocIdRangeVector& resultRanges, size_t dimensionIdx) const;

    bool GetSortedDocIdRanges(const std::shared_ptr<table::DimensionDescription>& dimension,
                              const DocIdRange& rangeLimit, DocIdRangeVector& resultRanges) const;
    void AppendRanges(const DocIdRangeVector& src, DocIdRangeVector& dest) const;

    bool ValidateDimensions(const table::DimensionDescriptionVector& dimensions) const;
    const AttributeReaderPtr& GetAttributeReader(const std::string& attrName) const;

private:
    typedef std::unordered_map<std::string, AttributeReaderPtr> AttributeReaderMap;

    AttributeReaderMap mSortAttrReaderMap;
    index_base::PartitionMeta mPartMeta;

private:
    IE_LOG_DECLARE();
    friend class SortedDocidRangeSearcherTest;
};

DEFINE_SHARED_PTR(SortedDocidRangeSearcher);
}} // namespace indexlib::index
