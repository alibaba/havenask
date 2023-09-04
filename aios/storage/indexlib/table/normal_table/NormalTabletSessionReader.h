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

#include "indexlib/framework/Version.h"
#include "indexlib/framework/mem_reclaimer/EpochBasedMemReclaimer.h"
#include "indexlib/framework/mem_reclaimer/IIndexMemoryReclaimer.h"
#include "indexlib/index/IIndexReader.h"
#include "indexlib/index/attribute/AttributeReader.h"
#include "indexlib/index/deletionmap/DeletionMapIndexReader.h"
#include "indexlib/index/inverted_index/InvertedIndexReader.h"
#include "indexlib/index/primary_key/PrimaryKeyReader.h"
#include "indexlib/index/summary/SummaryReader.h"
#include "indexlib/table/common/CommonTabletSessionReader.h"
#include "indexlib/table/normal_table/DimensionDescription.h"
#include "indexlib/table/normal_table/NormalTabletInfo.h"
#include "indexlib/table/normal_table/NormalTabletReader.h"

namespace indexlibv2::table {

class NormalTabletSessionReader : public CommonTabletSessionReader<NormalTabletReader>
{
public:
    NormalTabletSessionReader(const std::shared_ptr<NormalTabletReader>& normalTabletReader,
                              const std::shared_ptr<framework::IIndexMemoryReclaimer>& memReclaimer);
    ~NormalTabletSessionReader();

public:
    const framework::Version& GetVersion() const { return _impl->GetVersion(); }
    std::shared_ptr<NormalTabletInfo> GetNormalTabletInfo() const { return _impl->GetNormalTabletInfo(); }

    std::shared_ptr<indexlib::index::InvertedIndexReader> GetMultiFieldIndexReader() const
    {
        return _impl->GetMultiFieldIndexReader();
    }

    const std::shared_ptr<index::DeletionMapIndexReader>& GetDeletionMapReader() const
    {
        return _impl->GetDeletionMapReader();
    }
    const std::shared_ptr<indexlib::index::PrimaryKeyIndexReader>& GetPrimaryKeyReader() const
    {
        return _impl->GetPrimaryKeyReader();
    }
    std::shared_ptr<index::SummaryReader> GetSummaryReader() const { return _impl->GetSummaryReader(); }
    std::shared_ptr<index::AttributeReader> GetAttributeReader(const std::string& attrName) const
    {
        return _impl->GetAttributeReader(attrName);
    }
    std::shared_ptr<index::PackAttributeReader> GetPackAttributeReader(const std::string& packName) const
    {
        return _impl->GetPackAttributeReader(packName);
    }

    bool GetSortedDocIdRanges(const std::vector<std::shared_ptr<indexlib::table::DimensionDescription>>& dimensions,
                              const DocIdRange& rangeLimits, DocIdRangeVector& resultRanges) const
    {
        return _impl->GetSortedDocIdRanges(dimensions, rangeLimits, resultRanges);
    }

    bool GetPartedDocIdRanges(const DocIdRangeVector& rangeHint, size_t totalWayCount, size_t wayIdx,
                              DocIdRangeVector& ranges) const
    {
        return _impl->GetPartedDocIdRanges(rangeHint, totalWayCount, wayIdx, ranges);
    }
    bool GetPartedDocIdRanges(const DocIdRangeVector& rangeHint, size_t totalWayCount,
                              std::vector<DocIdRangeVector>& rangeVectors) const
    {
        return _impl->GetPartedDocIdRanges(rangeHint, totalWayCount, rangeVectors);
    }

    const NormalTabletReader::AccessCounterMap& GetInvertedAccessCounter() const;
    const NormalTabletReader::AccessCounterMap& GetAttributeAccessCounter() const;

public:
    Status QueryIndex(const base::PartitionQuery& query, base::PartitionResponse& partitionResponse) const;
    Status QueryDocIds(const base::PartitionQuery& query, base::PartitionResponse& partitionResponse,
                       std::vector<docid_t>& docids) const;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::table
