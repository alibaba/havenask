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
#include "indexlib/base/proto/query.pb.h"
#include "indexlib/common_define.h"
#include "indexlib/index/common/Term.h"
#include "indexlib/partition/index_partition.h"

namespace indexlibv2::tools {
class IndexTableExecutorV2;
class OrcTableExecutor;
} // namespace indexlibv2::tools

namespace indexlib::index {
class DeletionMapReader;
} // namespace indexlib::index

namespace indexlib::index {
class InvertedIndexReader;
class PrimaryKeyIndexReader;
} // namespace indexlib::index

namespace indexlib::config {
class IndexConfig;
}

namespace indexlib::tools {

class IndexTableExecutor
{
    typedef indexlib::partition::IndexPartitionPtr IndexPartitionPtr;
    typedef indexlib::partition::IndexPartitionReaderPtr IndexPartitionReaderPtr;

    typedef indexlibv2::base::PartitionQuery PartitionQuery;
    typedef indexlibv2::base::PartitionResponse PartitionResponse;
    typedef indexlibv2::base::AttrCondition AttrCondition;
    typedef indexlibv2::base::IndexTermMeta IndexTermMeta;
    typedef indexlibv2::base::Row Row;

    enum class IndexTableQueryType { Unknown, ByDocid, ByPk, ByCondition };

public:
    static const std::string BITMAP_TRUNCATE_NAME;
    static const std::string NULL_VALUE;

    static Status QueryIndex(const IndexPartitionReaderPtr& indexPartitionReaderPtr, const PartitionQuery& query,
                             PartitionResponse& partitionResponse);

    static Status QueryDocIdsByCondition(const IndexPartitionReaderPtr& indexPartitionReader,
                                         const AttrCondition& condition, const bool ignoreDeletionMap,
                                         const int64_t limit, const std::string& truncateName,
                                         std::vector<docid_t>& docIds, IndexTermMeta& indexTermMeta);

    static Status QueryDocIdsByPks(const IndexPartitionReaderPtr& indexPartitionReaderPtr,
                                   const std::vector<std::string>& pkList, const bool ignoreDeletionMap,
                                   const int64_t limit, std::vector<docid_t>& docIds);

    static Status QueryDocIdsByPks(const std::shared_ptr<indexlib::index::PrimaryKeyIndexReader>& pkReader,
                                   const std::shared_ptr<indexlib::index::DeletionMapReader>& deletionMapReader,
                                   const std::vector<std::string>& pkList, const int64_t limit,
                                   std::vector<docid_t>& docIds);

private:
    static Status ValidateQuery(const PartitionQuery& query, IndexTableQueryType& queryType);

    static Status ValidateDocIds(const std::shared_ptr<indexlib::index::DeletionMapReader>& deletionMapReader,
                                 const std::vector<docid_t>& docids);

    static std::vector<std::string> ValidateAttrs(const std::shared_ptr<config::IndexPartitionSchema>& legacySchema,
                                                  const std::vector<std::string>& attrs);
    static std::vector<std::string> ValidateSummarys(const std::shared_ptr<config::IndexPartitionSchema>& legacySchema,
                                                     const std::vector<std::string>& summarys);

    static Status QueryRowAttrByDocId(const IndexPartitionReaderPtr& indexPartitionReader, const docid_t docid,
                                      const std::vector<std::string>& attrs, Row& row);
    static Status QueryRowSummaryByDocId(const IndexPartitionReaderPtr& indexPartitionReader, const docid_t docid,
                                         const std::vector<std::string>& summarys, Row& row);

    static Status QueryIndexByDocId(const IndexPartitionReaderPtr& indexPartitionReader,
                                    const std::vector<std::string>& attrs, const std::vector<std::string>& summarys,
                                    const std::vector<docid_t>& docids, const int64_t limit,
                                    PartitionResponse& partitionResponse);

    static Status QueryDocIdsByTerm(const std::shared_ptr<index::InvertedIndexReader>& indexReader,
                                    const std::shared_ptr<indexlib::index::DeletionMapReader>& deletionMapReader,
                                    const index::Term& term, const int64_t limit, PostingType postingType,
                                    std::vector<docid_t>& docIds, IndexTermMeta& indexTermMeta);

    static std::pair<Status, std::shared_ptr<index::Term>>
    CreateTerm(const std::shared_ptr<config::IndexConfig>& indexConfig, const std::string& indexValue,
               const std::string& truncateName);

    friend class indexlibv2::tools::IndexTableExecutorV2;
    friend class indexlibv2::tools::OrcTableExecutor;
};

} // namespace indexlib::tools
