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
#include "indexlib/base/Status.h"
#include "indexlib/base/Types.h"
#include "indexlib/base/proto/query.pb.h"
#include "indexlib/index/inverted_index/Types.h"

namespace indexlib::index {
class Term;
class InvertedIndexReader;
} // namespace indexlib::index

namespace indexlibv2::index {
class DeletionMapIndexReader;
} // namespace indexlibv2::index

namespace indexlibv2::config {
class ITabletSchema;
class InvertedIndexConfig;
} // namespace indexlibv2::config

namespace indexlibv2::table {
class NormalTabletReader;

// a part of NormalTabletReader
class NormalTabletSearcher
{
public:
    NormalTabletSearcher(const NormalTabletReader* normalTabletReader);
    virtual ~NormalTabletSearcher();

public:
    /* protobuf query.proto:PartitionQuery to json
     json format:
     {
        "docid" : [ 0, 1, ...],
        "pk" : [ "key1", "key2", ...],
        "condition" : {
            "indexName" : "index1",
            "truncateName" : "trunc1",
            "values" : [ "value1", "value2", ...]
        },
        "ignoreDeletionMap" : false,
        "truncateName" : "__bitmap__",
        "limit" : 10,
        "attrs" : ["attr1", "attr2", ...],
        "pkNumber" : [5435621, 45938278, ...],
        "needSectionInfo" : false,
        "summarys" : ["summary1", "summary2", ...]
     }
     */
    Status Search(const std::string& jsonQuery, std::string& result) const;
    virtual Status QueryIndex(const base::PartitionQuery& query, base::PartitionResponse& partitionResponse) const;
    Status QueryDocIds(const base::PartitionQuery& query, base::PartitionResponse& partitionResponse,
                       std::vector<docid_t>& docids) const;

protected:
    enum class IndexTableQueryType { Unknown, ByDocid, ByRawPk, ByPkHash, ByCondition };
    static Status ValidateQuery(const base::PartitionQuery& query, IndexTableQueryType& queryType);

private:
    static Status ValidateDocIds(const std::shared_ptr<index::DeletionMapIndexReader>& deletionMapReader,
                                 const std::vector<docid_t>& docids);

    static std::vector<std::string> ValidateAttrs(const std::shared_ptr<config::ITabletSchema>& tabletSchema,
                                                  const std::vector<std::string>& attrs);
    static std::vector<std::string> ValidateSummarys(const std::shared_ptr<config::ITabletSchema>& tabletSchema,
                                                     const std::vector<std::string>& summarys);

    static Status QueryDocIdsByTerm(const std::shared_ptr<indexlib::index::InvertedIndexReader>& indexReader,
                                    const std::shared_ptr<index::DeletionMapIndexReader>& deletionMapReader,
                                    const indexlib::index::Term& term, const int64_t limit, PostingType postingType,
                                    std::vector<docid_t>& docIds, indexlibv2::base::IndexTermMeta& indexTermMeta);

    static std::pair<Status, std::shared_ptr<indexlib::index::Term>>
    CreateTerm(const std::shared_ptr<indexlibv2::config::InvertedIndexConfig>& indexConfig,
               const std::string& indexValue, const std::string& truncateName);

    Status QueryDocIdsByCondition(const base::AttrCondition& condition, const bool ignoreDeletionMap,
                                  const bool needSectionMeta, const int64_t limit, const std::string& truncateName,
                                  std::vector<docid_t>& docIds, base::PartitionResponse& partitionResponse) const;

    Status QueryDocIdsByRawPks(const std::shared_ptr<indexlibv2::index::DeletionMapIndexReader>& deletionMapReader,
                               const std::vector<std::string>& pkList, const int64_t limit,
                               std::vector<docid_t>& docIds) const;

    // only support 64-bits hash
    Status QueryDocIdsByPkHashs(const std::shared_ptr<index::DeletionMapIndexReader>& deletionMapReader,
                                const std::vector<uint64_t>& pkList, const int64_t limit,
                                std::vector<docid_t>& docIds) const;

    Status QueryRowAttrByDocId(const docid_t docid, const std::vector<std::string>& attrs, base::Row& row) const;
    Status QueryRowSummaryByDocId(const docid_t docid, const std::vector<std::string>& summarys, base::Row& row) const;

    Status QueryIndexByDocId(const std::vector<std::string>& attrs, const std::vector<std::string>& summarys,
                             const std::vector<docid_t>& docids, const int64_t limit,
                             base::PartitionResponse& partitionResponse) const;

    static bool ParseRangeQueryStr(const std::shared_ptr<indexlibv2::config::InvertedIndexConfig>& indexConfig,
                                   const std::string& range, int64_t& leftTimestamp, int64_t& rightTimestamp);

    static bool ParseTimestamp(const std::shared_ptr<indexlibv2::config::InvertedIndexConfig>& indexConfig,
                               const std::string& str, int64_t& value);

protected:
    const NormalTabletReader* _normalTabletReader = nullptr;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::table
