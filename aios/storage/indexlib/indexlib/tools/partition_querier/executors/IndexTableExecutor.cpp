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
#include "tools/partition_querier/executors/IndexTableExecutor.h"

#include "indexlib/document/normal/SearchSummaryDocument.h"
#include "indexlib/index/common/NumberTerm.h"
#include "indexlib/index/inverted_index/InvertedIndexReader.h"
#include "indexlib/index/inverted_index/PostingIterator.h"
#include "indexlib/index/inverted_index/format/TermMeta.h"
#include "indexlib/index/normal/attribute/accessor/attribute_iterator_typed.h"
#include "indexlib/index/normal/attribute/accessor/attribute_reader.h"
#include "indexlib/index/normal/attribute/accessor/pack_attribute_reader.h"
#include "indexlib/index/normal/deletionmap/deletion_map_reader.h"
#include "indexlib/index/normal/summary/summary_reader.h"
#include "indexlib/index/partition_info.h"
#include "indexlib/index/primary_key/PrimaryKeyIndexReader.h"
#include "indexlib/indexlib.h"
#include "indexlib/partition/index_partition_reader.h"
#include "indexlib/test/range_query_parser.h"
#include "tools/partition_querier/executors/AttrHelper.h"

using namespace std;
using namespace autil;
using namespace indexlib;
using namespace indexlib::common;
using namespace indexlib::partition;
using namespace indexlib::index;
using namespace indexlib::config;
using namespace google::protobuf;

namespace indexlib::tools {

template <typename T>
bool AttrHelper::FetchTypeValue(AttributeIteratorBase* reader, docid_t docid, T& value)
{
    typedef indexlib::index::AttributeIteratorTyped<T> IteratorType;
    IteratorType* iter = static_cast<IteratorType*>(reader);
    return iter->Seek(docid, value);
}

const std::string IndexTableExecutor::BITMAP_TRUNCATE_NAME = "__bitmap__";
const std::string IndexTableExecutor::NULL_VALUE = "__NULL__";

Status IndexTableExecutor::ValidateQuery(const PartitionQuery& query, IndexTableQueryType& queryType)
{
    const bool queryByDocid = query.docid_size() > 0;
    const bool queryByPk = query.pk_size() > 0;
    const bool queryByCondition = query.has_condition();

    if (!(queryByDocid || queryByPk || queryByCondition)) {
        return Status::InvalidArgs("PartitionQuery should contain one of docids, pks or condition.");
    }

    if ((queryByDocid && queryByPk) || (queryByPk && queryByCondition) || (queryByCondition && queryByDocid)) {
        return Status::InvalidArgs("PartitionQuery can only contain one of docids, pks and condition");
    }

    if (queryByDocid) {
        queryType = IndexTableQueryType::ByDocid;
    } else if (queryByPk) {
        queryType = IndexTableQueryType::ByPk;
    } else if (queryByCondition) {
        queryType = IndexTableQueryType::ByCondition;
    }

    return Status::OK();
}

vector<string> IndexTableExecutor::ValidateAttrs(const std::shared_ptr<config::IndexPartitionSchema>& legacySchema,
                                                 const vector<string>& attrs)
{
    const auto& attrSchema = legacySchema->GetAttributeSchema();
    vector<string> indexAttrs;
    if (!attrSchema) {
        return indexAttrs;
    }
    for (size_t i = 0; i < attrSchema->GetAttributeCount(); i++) {
        const auto& attrConfig = attrSchema->GetAttributeConfigs()[i];
        if (attrConfig->IsDisabled() || attrConfig->IsDeleted()) {
            continue;
        }
        indexAttrs.push_back(attrConfig->GetAttrName());
    }
    return AttrHelper::ValidateAttrs(indexAttrs, attrs);
}

vector<string> IndexTableExecutor::ValidateSummarys(const std::shared_ptr<config::IndexPartitionSchema>& legacySchema,
                                                    const vector<string>& summarys)
{
    vector<string> indexSummarys;
    const auto& summarySchema = legacySchema->GetSummarySchema();
    if (!summarySchema) {
        return indexSummarys;
    }
    if (summarys.empty()) {
        for (auto iter = summarySchema->Begin(); iter != summarySchema->End(); ++iter) {
            indexSummarys.push_back((*iter)->GetSummaryName());
        }
        return indexSummarys;
    }
    for (const auto& summary : summarys) {
        auto fieldConfig = legacySchema->GetFieldConfig(summary);
        if (!fieldConfig) {
            continue;
        }
        if (!summarySchema->GetSummaryConfig(fieldConfig->GetFieldId())) {
            continue;
        }
        indexSummarys.push_back(summary);
    }
    return indexSummarys;
}

Status IndexTableExecutor::ValidateDocIds(const std::shared_ptr<indexlib::index::DeletionMapReader>& deletionMapReader,
                                          const vector<docid_t>& docids)
{
    if (deletionMapReader == nullptr) {
        return Status::OK();
    }
    for (const auto& docId : docids) {
        if (deletionMapReader->IsDeleted(docId)) {
            return Status::Expired("find deleted docid [%d]", docId);
        }
    }

    return Status::OK();
}

Status
IndexTableExecutor::QueryDocIdsByTerm(const std::shared_ptr<InvertedIndexReader>& indexReader,
                                      const std::shared_ptr<indexlib::index::DeletionMapReader>& deletionMapReader,
                                      const Term& term, const int64_t limit, PostingType postingType,
                                      std::vector<docid_t>& docIds, IndexTermMeta& indexTermMeta)
{
    autil::mem_pool::Pool pool;
    auto postingIter = indexReader->Lookup(term, limit, postingType, &pool).ValueOrThrow();
    if (!postingIter) {
        return Status::NotFound("create index iterator for index name [%s] failed", term.GetIndexName().c_str());
    }
    auto termMeta = postingIter->GetTermMeta();
    if (termMeta) {
        indexTermMeta.set_docfreq(termMeta->GetDocFreq());
        indexTermMeta.set_totaltermfreq(termMeta->GetTotalTermFreq());
        indexTermMeta.set_payload(termMeta->GetPayload());
    }
    docid_t docId = INVALID_DOCID;
    while (docIds.size() < limit && (docId = postingIter->SeekDoc(docId)) != INVALID_DOCID) {
        if ((nullptr == deletionMapReader) || !deletionMapReader->IsDeleted(docId)) {
            docIds.push_back(docId);
        }
    }
    IE_POOL_COMPATIBLE_DELETE_CLASS(&pool, postingIter);
    return Status::OK();
}

std::pair<Status, std::shared_ptr<Term>> IndexTableExecutor::CreateTerm(const std::shared_ptr<IndexConfig>& indexConfig,
                                                                        const std::string& indexValue,
                                                                        const std::string& truncateName)
{
    std::string indexName = indexConfig->GetIndexName();
    if (indexValue == IndexTableExecutor::NULL_VALUE) {
        return {Status::OK(), std::make_shared<Term>(indexName)};
    } else if (indexConfig->GetInvertedIndexType() == it_date || indexConfig->GetInvertedIndexType() == it_range) {
        int64_t leftNumber, rightNumber;
        if (!test::RangeQueryParser::ParseQueryStr(indexConfig, indexValue, leftNumber, rightNumber)) {
            auto status = Status::InvalidArgs(
                "illegal query format, expected \"(xxx, xxx]\" instead of \"%s\" on index type [%s]",
                indexValue.c_str(), config::IndexConfig::InvertedIndexTypeToStr(indexConfig->GetInvertedIndexType()));
            return {status, nullptr};
        }
        auto term = std::make_shared<index::Int64Term>(leftNumber, true, rightNumber, true, indexName);
        return {Status::OK(), term};
    } else {
        auto term = std::make_shared<Term>(indexName);
        term->SetWord(indexValue);
        term->SetTruncateName(truncateName);
        return {Status::OK(), term};
    }
}

Status IndexTableExecutor::QueryDocIdsByCondition(const IndexPartitionReaderPtr& indexPartitionReader,
                                                  const AttrCondition& condition, const bool ignoreDeletionMap,
                                                  const int64_t limit, const string& truncateName,
                                                  vector<docid_t>& docIds, IndexTermMeta& indexTermMeta)
{
    const auto indexPartitionSchemaPtr = indexPartitionReader->GetSchema();
    const string indexName = condition.indexname();
    const auto indexConfig = indexPartitionSchemaPtr->GetIndexSchema()->GetIndexConfig(indexName);
    const auto postingType =
        (truncateName == IndexTableExecutor::BITMAP_TRUNCATE_NAME) ? PostingType::pt_bitmap : PostingType::pt_normal;

    if (indexConfig == nullptr) {
        return Status::InvalidArgs("can not find index [%s] in schema", indexName.c_str());
    }
    const auto indexReader = indexPartitionReader->GetInvertedIndexReader();
    std::shared_ptr<indexlib::index::DeletionMapReader> deletionMapReader;
    if (!ignoreDeletionMap) {
        deletionMapReader = indexPartitionReader->GetDeletionMapReader();
    }

    for (const auto& indexValue : condition.values()) {
        auto [status, term] = CreateTerm(indexConfig, indexValue, truncateName);
        if (!status.IsOK()) {
            return status;
        }
        status = QueryDocIdsByTerm(indexReader, deletionMapReader, *term, limit, postingType, docIds, indexTermMeta);
        if (!status.IsOK()) {
            return status;
        }
    }
    if (docIds.empty()) {
        return Status::NotFound("no docid matching search condition found");
    }
    return Status::OK();
}

Status IndexTableExecutor::QueryDocIdsByPks(const IndexPartitionReaderPtr& indexPartitionReaderPtr,
                                            const std::vector<std::string>& pkList, const bool ignoreDeletionMap,
                                            const int64_t limit, std::vector<docid_t>& docIds)
{
    std::shared_ptr<indexlib::index::DeletionMapReader> deletionMapReader;
    if (!ignoreDeletionMap) {
        deletionMapReader = indexPartitionReaderPtr->GetDeletionMapReader();
    }

    return QueryDocIdsByPks(indexPartitionReaderPtr->GetPrimaryKeyReader(), deletionMapReader, pkList, limit, docIds);
}

Status
IndexTableExecutor::QueryDocIdsByPks(const std::shared_ptr<indexlib::index::PrimaryKeyIndexReader>& pkReader,
                                     const std::shared_ptr<indexlib::index::DeletionMapReader>& deletionMapReader,
                                     const vector<string>& pkList, const int64_t limit, vector<docid_t>& docIds)
{
    if (nullptr == pkReader) {
        return Status::InvalidArgs("table can not get primary key reader.");
    }

    for (const auto& pk : pkList) {
        auto docId = pkReader->Lookup(pk);
        if (INVALID_DOCID == docId) {
            return Status::NotFound("can't find docid with pk [%s]", pk.c_str());
        }
        if ((nullptr != deletionMapReader) && deletionMapReader->IsDeleted(docId)) {
            return Status::Expired("find deleted docid [%d] with pk[%s]", docId, pk.c_str());
        }
        if (docIds.size() < limit) {
            docIds.push_back(docId);
        }
    }
    return Status::OK();
}

Status IndexTableExecutor::QueryRowAttrByDocId(const IndexPartitionReaderPtr& indexPartitionReader, const docid_t docid,
                                               const vector<string>& attrs, Row& row)
{
    const auto& schema = indexPartitionReader->GetSchema();
    const auto& attrSchema = schema->GetAttributeSchema();

    autil::mem_pool::Pool pool;

    for (const auto& attr : attrs) {
        const auto& attrConfig = attrSchema->GetAttributeConfig(attr);
        if (attrConfig->IsDisabled() || attrConfig->IsDeleted()) {
            continue;
        }
        auto attrValue = row.add_attrvalues();
        const auto& packAttrConfig = attrConfig->GetPackAttributeConfig();
        if (packAttrConfig) {
            const auto& packName = packAttrConfig->GetPackName();
            const auto& packAttrReader = indexPartitionReader->GetPackAttributeReader(packName);
            auto attrRef = packAttrReader->GetSubAttributeReference(attr);
            const char* value = packAttrReader->GetBaseAddress(docid, &pool);
            if (value == nullptr) {
                return Status::NotFound("can not find attr[%s] with docid = %d", attr.c_str(), docid);
            }
            AttrHelper::GetAttributeValue(attrRef, value, attrConfig, *attrValue);
        } else {
            const auto& attrReader = indexPartitionReader->GetAttributeReader(attr);
            auto iterator = attrReader->CreateIterator(&pool);
            AttrHelper::GetAttributeValue(iterator, docid, attrConfig, *attrValue);
            POOL_COMPATIBLE_DELETE_CLASS(&pool, iterator);
        }
    }
    return Status::OK();
}

Status IndexTableExecutor::QueryRowSummaryByDocId(const IndexPartitionReaderPtr& indexPartitionReader,
                                                  const docid_t docid, const vector<string>& summarys, Row& row)
{
    if (summarys.empty()) {
        return Status::OK();
    }
    index::SummaryReaderPtr summaryReader = indexPartitionReader->GetSummaryReader();
    if (!summaryReader) {
        return Status::InternalError("get summary reader failed!");
    }
    auto schema = indexPartitionReader->GetSchema();
    config::SummarySchemaPtr summarySchema = schema->GetSummarySchema();
    if (!summarySchema) {
        return Status::ConfigError("get summary schema failed!");
    }
    auto doc = std::make_unique<indexlib::document::SearchSummaryDocument>(nullptr, summarySchema->GetSummaryCount());
    if (!summaryReader->GetDocument(docid, doc.get())) {
        return Status::InternalError("read summary failed");
    }
    for (const auto& summary : summarys) {
        fieldid_t fieldId = schema->GetFieldId(summary);
        index::summaryfieldid_t summaryFieldId = summarySchema->GetSummaryFieldId(fieldId);
        const autil::StringView* fieldValue = doc->GetFieldValue(summaryFieldId);
        std::string value;
        if (fieldValue != NULL && fieldValue->size() > 0) {
            value.assign(fieldValue->data(), fieldValue->size());
        }
        auto summaryValue = row.add_summaryvalues();
        summaryValue->set_fieldname(summary);
        summaryValue->set_value(value);
    }
    return Status::OK();
}

Status IndexTableExecutor::QueryIndexByDocId(const IndexPartitionReaderPtr& indexPartitionReader,
                                             const vector<string>& attrs, const vector<string>& summarys,
                                             const vector<docid_t>& docids, const int64_t limit,
                                             PartitionResponse& partitionResponse)
{
    for (int docCount = 0; docCount < min(docids.size(), size_t(limit)); ++docCount) {
        auto docid = docids[docCount];
        auto row = partitionResponse.add_rows();
        row->set_docid(docid);
        RETURN_STATUS_DIRECTLY_IF_ERROR(QueryRowAttrByDocId(indexPartitionReader, docid, attrs, *row));
        RETURN_STATUS_DIRECTLY_IF_ERROR(QueryRowSummaryByDocId(indexPartitionReader, docid, summarys, *row));
    }
    if (partitionResponse.rows_size() > 0) {
        const auto& attrValues = partitionResponse.rows(0).attrvalues();
        auto attrInfo = partitionResponse.mutable_attrinfo();
        for (int i = 0; i < attrValues.size(); ++i) {
            auto field = attrInfo->add_fields();
            field->set_type(attrValues[i].type());
            field->set_attrname(attrs[i]);
        }
    }
    return Status::OK();
}

Status IndexTableExecutor::QueryIndex(const IndexPartitionReaderPtr& indexPartitionReader, const PartitionQuery& query,
                                      PartitionResponse& partitionResponse)
{
    IndexTableQueryType queryType = IndexTableQueryType::Unknown;
    RETURN_STATUS_DIRECTLY_IF_ERROR(ValidateQuery(query, queryType));

    const int64_t limit = query.has_limit() ? query.limit() : 10;
    const string truncateName = query.truncatename();
    vector<docid_t> docids;
    const bool ignoreDeletionMap = query.has_ignoredeletionmap() ? query.ignoredeletionmap() : true;
    std::shared_ptr<indexlib::index::DeletionMapReader> deletionMapReader;
    if (!ignoreDeletionMap) {
        deletionMapReader = indexPartitionReader->GetDeletionMapReader();
    }

    switch (queryType) {
    case IndexTableQueryType::ByCondition:
        RETURN_STATUS_DIRECTLY_IF_ERROR(QueryDocIdsByCondition(indexPartitionReader, query.condition(),
                                                               ignoreDeletionMap, limit, truncateName, docids,
                                                               *(partitionResponse.mutable_termmeta())));
        break;
    case IndexTableQueryType::ByPk: {
        RETURN_STATUS_DIRECTLY_IF_ERROR(QueryDocIdsByPks(indexPartitionReader->GetPrimaryKeyReader(), deletionMapReader,
                                                         {query.pk().begin(), query.pk().end()}, limit, docids));
        break;
    }
    case IndexTableQueryType::ByDocid:
        docids = {query.docid().begin(), query.docid().end()};
        break;
    default:
        return Status::InvalidArgs("unknown query type code", (int)queryType);
        break;
    }

    if (!ignoreDeletionMap) {
        RETURN_STATUS_DIRECTLY_IF_ERROR(ValidateDocIds(deletionMapReader, docids));
    }

    auto attrs =
        ValidateAttrs(indexPartitionReader->GetSchema(), vector<string> {query.attrs().begin(), query.attrs().end()});
    auto summarys = ValidateSummarys(indexPartitionReader->GetSchema(),
                                     vector<string> {query.summarys().begin(), query.summarys().end()});
    RETURN_STATUS_DIRECTLY_IF_ERROR(
        QueryIndexByDocId(indexPartitionReader, attrs, summarys, docids, limit, partitionResponse));

    if (IndexTableQueryType::ByPk == queryType) {
        for (int i = 0; i < query.pk().size(); ++i) {
            partitionResponse.mutable_rows(i)->set_pk(query.pk(i));
        }
    }

    return Status::OK();
}

} // namespace indexlib::tools
