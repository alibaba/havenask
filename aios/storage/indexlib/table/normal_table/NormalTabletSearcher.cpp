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
#include "indexlib/table/normal_table/NormalTabletSearcher.h"

#include "autil/memory.h"
#include "indexlib/config/TabletSchema.h"
#include "indexlib/document/normal/SearchSummaryDocument.h"
#include "indexlib/document/normal/SourceDocument.h"
#include "indexlib/index/attribute/AttrHelper.h"
#include "indexlib/index/attribute/AttributeIteratorBase.h"
#include "indexlib/index/attribute/AttributeIteratorTyped.h"
#include "indexlib/index/attribute/AttributeReader.h"
#include "indexlib/index/attribute/Common.h"
#include "indexlib/index/common/NumberTerm.h"
#include "indexlib/index/deletionmap/DeletionMapIndexReader.h"
#include "indexlib/index/field_meta/Common.h"
#include "indexlib/index/field_meta/FieldMetaReader.h"
#include "indexlib/index/inverted_index/Common.h"
#include "indexlib/index/inverted_index/InDocSectionMeta.h"
#include "indexlib/index/inverted_index/MultiFieldIndexReader.h"
#include "indexlib/index/inverted_index/PostingIterator.h"
#include "indexlib/index/inverted_index/SectionAttributeReader.h"
#include "indexlib/index/inverted_index/config/DateIndexConfig.h"
#include "indexlib/index/inverted_index/config/PackageIndexConfig.h"
#include "indexlib/index/inverted_index/format/TermMeta.h"
#include "indexlib/index/primary_key/PrimaryKeyIndexReader.h"
#include "indexlib/index/source/SourceReader.h"
#include "indexlib/index/summary/SummaryReader.h"
#include "indexlib/index/summary/config/SummaryConfig.h"
#include "indexlib/index/summary/config/SummaryIndexConfig.h"
#include "indexlib/table/BuiltinDefine.h"
#include "indexlib/table/common/SearchUtil.h"
#include "indexlib/table/normal_table/Common.h"
#include "indexlib/table/normal_table/NormalTabletReader.h"
#include "indexlib/util/ProtoJsonizer.h"
#include "indexlib/util/TimestampUtil.h"

namespace indexlib::index {

template <typename T>
bool AttrHelper::FetchTypeValue(indexlibv2::index::AttributeIteratorBase* reader, docid_t docid, T& value)
{
    typedef indexlibv2::index::AttributeIteratorTyped<T> IteratorType;
    IteratorType* iter = static_cast<IteratorType*>(reader);
    return iter->Seek(docid, value);
}

} // namespace indexlib::index

namespace indexlibv2::table {
AUTIL_LOG_SETUP(indexlib.table, NormalTabletSearcher);

static const std::string BITMAP_TRUNCATE_NAME = "__bitmap__";
static const std::string NULL_VALUE = "__NULL__";

NormalTabletSearcher::NormalTabletSearcher(const NormalTabletReader* normalTabletReader)
    : _normalTabletReader(normalTabletReader)
{
    assert(_normalTabletReader);
}

NormalTabletSearcher::~NormalTabletSearcher() {}

/*protobuf query.proto:PartitionQuery to json
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
Status NormalTabletSearcher::Search(const std::string& jsonQuery, std::string& result) const
{
    base::PartitionQuery query;
    RETURN_STATUS_DIRECTLY_IF_ERROR(indexlib::util::ProtoJsonizer::FromJson(jsonQuery, &query));
    base::PartitionResponse partitionResponse;
    RETURN_STATUS_DIRECTLY_IF_ERROR(QueryIndex(query, partitionResponse));
    RETURN_STATUS_DIRECTLY_IF_ERROR(indexlib::util::ProtoJsonizer::ToJson(partitionResponse, &result));
    return Status::OK();
}

Status NormalTabletSearcher::QueryIndex(const base::PartitionQuery& query,
                                        base::PartitionResponse& partitionResponse) const
{
    if (_normalTabletReader->GetSchema()->GetTableType() != indexlib::table::TABLE_TYPE_NORMAL) {
        return Status::Unimplement("not support queryIndex for orc tablet");
    }

    IndexTableQueryType queryType = IndexTableQueryType::Unknown;
    RETURN_STATUS_DIRECTLY_IF_ERROR(ValidateQuery(query, queryType));

    if (queryType == IndexTableQueryType::ByFieldMeta) {
        return QueryFieldMeta(query, partitionResponse);
    }

    std::vector<docid_t> docids;
    auto ret = QueryDocIds(query, partitionResponse, docids);
    if (!ret.IsOK()) {
        return ret;
    }

    const int64_t limit = query.has_limit() ? query.limit() : 10;

    auto attrs = ValidateAttrs(_normalTabletReader->GetSchema(),
                               std::vector<std::string> {query.attrs().begin(), query.attrs().end()});
    auto summarys = ValidateSummarys(_normalTabletReader->GetSchema(),
                                     std::vector<std::string> {query.summarys().begin(), query.summarys().end()});
    auto sources = std::vector<std::string> {query.sources().begin(), query.sources().end()};
    RETURN_STATUS_DIRECTLY_IF_ERROR(QueryIndexByDocId(attrs, summarys, sources, docids, limit, partitionResponse));
    if (IndexTableQueryType::ByRawPk == queryType) {
        for (int i = 0; i < query.pk().size(); ++i) {
            partitionResponse.mutable_rows(i)->set_pk(query.pk(i));
        }
    }

    return Status::OK();
}

Status NormalTabletSearcher::QueryFieldMeta(const base::PartitionQuery& query,
                                            base::PartitionResponse& partitionResponse) const
{
    if (!query.has_fieldmetaquery() && !query.has_fieldtokencountquery()) {
        return Status::OK();
    }
    if (query.has_fieldmetaquery()) {
        auto queryMeta = query.fieldmetaquery();
        auto indexName = queryMeta.indexname();
        auto type = queryMeta.fieldmetatype();
        auto metaReader = _normalTabletReader->GetIndexReader<indexlib::index::FieldMetaReader>(
            indexlib::index::FIELD_META_INDEX_TYPE_STR, indexName);
        if (!metaReader) {
            return Status::InvalidArgs("cannot get field meta for index [%s], type [%s]", indexName.c_str(),
                                       type.c_str());
        }
        auto fieldMeta = metaReader->GetTableFieldMeta(type);
        if (!fieldMeta) {
            return Status::InvalidArgs("cannot get field meta for index [%s], type [%s]", indexName.c_str(),
                                       type.c_str());
        }
        partitionResponse.mutable_metaresult()->set_indexname(indexName);
        partitionResponse.mutable_metaresult()->set_fieldmetatype(type);
        partitionResponse.mutable_metaresult()->set_metainfo(autil::legacy::ToJsonString(fieldMeta));
    }
    if (query.has_fieldtokencountquery()) {
        auto queryMeta = query.fieldtokencountquery();
        auto indexName = queryMeta.indexname();
        auto docId = queryMeta.docid();
        auto metaReader = _normalTabletReader->GetIndexReader<indexlib::index::FieldMetaReader>(
            indexlib::index::FIELD_META_INDEX_TYPE_STR, indexName);
        if (!metaReader) {
            return Status::InvalidArgs("cannot get field meta for index [%s]", indexName.c_str());
        }
        uint64_t tokenCount = 0;
        autil::mem_pool::Pool pool;
        if (!metaReader->GetFieldTokenCount(docId, &pool, tokenCount)) {
            return Status::InvalidArgs("cannot get field meta len for index [%s]", indexName.c_str());
        }
        auto row = partitionResponse.add_rows();
        row->set_docid(docId);
        auto fieldTokenCountRes = row->mutable_fieldtokencountres();
        fieldTokenCountRes->set_indexname(indexName);
        fieldTokenCountRes->set_fieldtokencount(tokenCount);
    }
    return Status::OK();
}

Status NormalTabletSearcher::QueryDocIds(const base::PartitionQuery& query, base::PartitionResponse& partitionResponse,
                                         std::vector<docid_t>& docids) const
{
    IndexTableQueryType queryType = IndexTableQueryType::Unknown;
    RETURN_STATUS_DIRECTLY_IF_ERROR(ValidateQuery(query, queryType));

    const int64_t limit = query.has_limit() ? query.limit() : 10;
    const std::string truncateName = query.truncatename();
    const bool ignoreDeletionMap = query.has_ignoredeletionmap() ? query.ignoredeletionmap() : true;
    const bool needSectionMeta = query.has_needsectioninfo() ? query.needsectioninfo() : false;

    std::shared_ptr<index::DeletionMapIndexReader> deletionMapReader;
    if (!ignoreDeletionMap) {
        deletionMapReader = _normalTabletReader->GetDeletionMapReader();
    }

    switch (queryType) {
    case IndexTableQueryType::ByCondition:
        RETURN_STATUS_DIRECTLY_IF_ERROR(QueryDocIdsByCondition(query.condition(), ignoreDeletionMap, needSectionMeta,
                                                               limit, truncateName, docids, partitionResponse));
        break;
    case IndexTableQueryType::ByRawPk:
        RETURN_STATUS_DIRECTLY_IF_ERROR(
            QueryDocIdsByRawPks(deletionMapReader, {query.pk().begin(), query.pk().end()}, limit, docids));
        break;
    case IndexTableQueryType::ByDocid:
        docids = {query.docid().begin(), query.docid().end()};
        break;
    case IndexTableQueryType::ByPkHash:
        RETURN_STATUS_DIRECTLY_IF_ERROR(
            QueryDocIdsByPkHashs(deletionMapReader, {query.pknumber().begin(), query.pknumber().end()}, limit, docids));
        break;
    default:
        return Status::InvalidArgs("unknown query type code", (int)queryType);
        break;
    }

    if (!ignoreDeletionMap) {
        RETURN_STATUS_DIRECTLY_IF_ERROR(ValidateDocIds(deletionMapReader, docids));
    }
    return Status::OK();
}

Status NormalTabletSearcher::ValidateQuery(const base::PartitionQuery& query, IndexTableQueryType& queryType)
{
    const bool queryByDocid = query.docid_size() > 0;
    const bool queryByRawPk = query.pk_size() > 0;
    const bool queryByCondition = query.has_condition();
    const bool queryByPkHash = query.pknumber_size() > 0;
    const bool queryForFieldMeta = query.has_fieldmetaquery() || query.has_fieldtokencountquery();

    size_t queryCounts = (queryByDocid ? 1 : 0) + (queryByRawPk ? 1 : 0) + (queryByCondition ? 1 : 0) +
                         (queryByPkHash ? 1 : 0) + (queryForFieldMeta ? 1 : 0);
    if (queryCounts != 1) {
        return Status::InvalidArgs(
            "PartitionQuery must contain exactly one of docids, pks, pknumbers , condition and field meta");
    }

    if (queryByDocid) {
        queryType = IndexTableQueryType::ByDocid;
    } else if (queryByRawPk) {
        queryType = IndexTableQueryType::ByRawPk;
    } else if (queryByCondition) {
        queryType = IndexTableQueryType::ByCondition;
    } else if (queryByPkHash) {
        queryType = IndexTableQueryType::ByPkHash;
    } else if (queryForFieldMeta) {
        queryType = IndexTableQueryType::ByFieldMeta;
    }

    return Status::OK();
}

Status NormalTabletSearcher::ValidateDocIds(const std::shared_ptr<index::DeletionMapIndexReader>& deletionMapReader,
                                            const std::vector<docid_t>& docids)
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

std::vector<std::string> NormalTabletSearcher::ValidateAttrs(const std::shared_ptr<config::ITabletSchema>& tabletSchema,
                                                             const std::vector<std::string>& attrs)
{
    std::vector<std::string> indexAttrs;
    const auto& indexConfigs = tabletSchema->GetIndexConfigs(index::ATTRIBUTE_INDEX_TYPE_STR);
    for (const auto& indexConfig : indexConfigs) {
        auto attrConfig = dynamic_cast<const index::AttributeConfig*>(indexConfig.get());
        if (attrConfig->IsDisabled()) {
            continue;
        }
        indexAttrs.push_back(attrConfig->GetAttrName());
    }
    return indexlib::index::AttrHelper::ValidateAttrs(indexAttrs, attrs);
}

std::vector<std::string>
NormalTabletSearcher::ValidateSummarys(const std::shared_ptr<config::ITabletSchema>& tabletSchema,
                                       const std::vector<std::string>& summarys)
{
    std::vector<std::string> indexSummarys;
    const auto& config =
        tabletSchema->GetIndexConfig(indexlib::index::SUMMARY_INDEX_TYPE_STR, indexlib::index::SUMMARY_INDEX_NAME);
    if (!config) {
        return indexSummarys;
    }
    const auto& summaryConfig = std::dynamic_pointer_cast<indexlibv2::config::SummaryIndexConfig>(config);
    assert(summaryConfig);
    if (summarys.empty()) {
        for (const auto& singleConfig : *summaryConfig) {
            indexSummarys.push_back(singleConfig->GetSummaryName());
        }
        return indexSummarys;
    }
    for (const auto& summary : summarys) {
        auto fieldConfig = tabletSchema->GetFieldConfig(summary);
        if (!fieldConfig) {
            continue;
        }
        if (!summaryConfig->GetSummaryConfig(fieldConfig->GetFieldId())) {
            continue;
        }
        indexSummarys.push_back(summary);
    }
    return indexSummarys;
}

Status
NormalTabletSearcher::QueryDocIdsByPkHashs(const std::shared_ptr<index::DeletionMapIndexReader>& deletionMapReader,
                                           const std::vector<uint64_t>& pkList, const int64_t limit,
                                           std::vector<docid_t>& docIds) const
{
    const auto& pkReader = _normalTabletReader->GetPrimaryKeyReader();
    if (nullptr == pkReader) {
        return Status::InvalidArgs("table can not get primary key reader.");
    }

    for (const auto& pk : pkList) {
        auto docId = pkReader->LookupWithPKHash(autil::uint128_t(pk));
        if (INVALID_DOCID == docId) {
            return Status::NotFound("can't find docid with pk [%llu]", pk);
        }
        if ((nullptr != deletionMapReader) && deletionMapReader->IsDeleted(docId)) {
            return Status::Expired("find deleted docid [%d] with pk[%llu]", docId, pk);
        }
        if (docIds.size() < limit) {
            docIds.push_back(docId);
        }
    }
    return Status::OK();
}

Status
NormalTabletSearcher::QueryDocIdsByRawPks(const std::shared_ptr<index::DeletionMapIndexReader>& deletionMapReader,
                                          const std::vector<std::string>& pkList, const int64_t limit,
                                          std::vector<docid_t>& docIds) const
{
    const auto& pkReader = _normalTabletReader->GetPrimaryKeyReader();
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

Status NormalTabletSearcher::QueryDocIdsByCondition(const indexlibv2::base::AttrCondition& condition,
                                                    const bool ignoreDeletionMap, const bool needSectionMeta,
                                                    const int64_t limit, const std::string& truncateName,
                                                    std::vector<docid_t>& docIds,
                                                    base::PartitionResponse& partitionResponse) const
{
    const auto tabletSchema = _normalTabletReader->GetSchema();
    const std::string indexName = condition.indexname();
    const auto config = tabletSchema->GetIndexConfig(indexlib::index::GENERAL_INVERTED_INDEX_TYPE_STR, indexName);
    const auto indexConfig = std::dynamic_pointer_cast<indexlibv2::config::InvertedIndexConfig>(config);
    if (indexConfig == nullptr) {
        return Status::InvalidArgs("fail to cast [%s] index config in schema", indexName.c_str());
    }

    const auto postingType = (truncateName == BITMAP_TRUNCATE_NAME) ? PostingType::pt_bitmap : PostingType::pt_normal;

    const auto indexReader = _normalTabletReader->GetMultiFieldIndexReader();
    std::shared_ptr<indexlibv2::index::DeletionMapIndexReader> deletionMapReader;
    if (!ignoreDeletionMap) {
        deletionMapReader = _normalTabletReader->GetDeletionMapReader();
    }

    for (const auto& indexValue : condition.values()) {
        auto [status, term] = CreateTerm(indexConfig, indexValue, truncateName);
        if (!status.IsOK()) {
            return status;
        }
        status = QueryDocIdsByTerm(indexReader, deletionMapReader, *term, limit, postingType, docIds,
                                   *(partitionResponse.mutable_termmeta()));
        if (!status.IsOK()) {
            return status;
        }
    }
    if (docIds.empty()) {
        return Status::NotFound("no docid matching search condition found");
    }
    if (needSectionMeta) {
        auto packageIndexConfig = std::dynamic_pointer_cast<config::PackageIndexConfig>(indexConfig);
        if (!packageIndexConfig || !packageIndexConfig->HasSectionAttribute()) {
            return Status::InvalidArgs("fail to get index [%s] section info", indexName.c_str());
        }
        auto sectionReader = indexReader->GetSectionReader(indexName);
        for (auto docId : docIds) {
            auto meta = sectionReader->GetSection(docId);
            uint32_t sectionCount = meta->GetSectionCount();
            for (uint32_t i = 0; i < sectionCount; ++i) {
                section_weight_t sectionWeight = 0;
                fieldid_t fieldId = 0;
                section_len_t sectionLength = 0;
                meta->GetSectionMeta(i, sectionWeight, fieldId, sectionLength);
                auto sectionMeta = partitionResponse.add_sectionmetas();
                sectionMeta->set_fieldid(fieldId);
                sectionMeta->set_sectionweight(sectionWeight);
                sectionMeta->set_sectionlen(sectionLength);
            }
        }
    }
    return Status::OK();
}

Status NormalTabletSearcher::QueryRowAttrByDocId(const docid_t docid, const std::vector<std::string>& attrs,
                                                 base::Row& row) const
{
    const auto& schema = _normalTabletReader->GetSchema();
    autil::mem_pool::Pool pool;
    for (const auto& attr : attrs) {
        auto indexConfig = schema->GetIndexConfig(index::ATTRIBUTE_INDEX_TYPE_STR, /*indexName*/ attr);
        assert(indexConfig);
        auto attrConfig = std::dynamic_pointer_cast<index::AttributeConfig>(indexConfig);
        if (attrConfig->IsDisabled()) {
            continue;
        }
        auto attrValue = row.add_attrvalues();
        const auto& packAttrConfig = attrConfig->GetPackAttributeConfig();
        if (packAttrConfig) {
            // TODO(yonghao.fyh) : support pack attr
            assert(false);
            return Status::Unimplement("not support pack attr yet");
        } else {
            const auto& attrReader = _normalTabletReader->GetAttributeReader(attr);
            if (!attrReader) {
                return Status::NotFound();
            }

            auto iterator = attrReader->CreateIterator(&pool);
            bool found = indexlib::index::AttrHelper::GetAttributeValue(iterator, docid, attrConfig, *attrValue);
            POOL_COMPATIBLE_DELETE_CLASS(&pool, iterator);
            if (!found) {
                return Status::NotFound();
            }
        }
    }
    return Status::OK();
}

Status NormalTabletSearcher::QueryRowSummaryByDocId(const docid_t docid, const std::vector<std::string>& summarys,
                                                    base::Row& row) const
{
    if (summarys.empty()) {
        return Status::OK();
    }
    std::shared_ptr<index::SummaryReader> summaryReader = _normalTabletReader->GetSummaryReader();
    if (!summaryReader) {
        return Status::InternalError("get summary reader failed!");
    }
    const auto& schema = _normalTabletReader->GetSchema();
    autil::mem_pool::Pool pool;
    const auto& config =
        schema->GetIndexConfig(indexlib::index::SUMMARY_INDEX_TYPE_STR, indexlib::index::SUMMARY_INDEX_NAME);
    if (!config) {
        return Status::ConfigError("get summary config failed");
    }
    const auto& summaryConfig = std::dynamic_pointer_cast<indexlibv2::config::SummaryIndexConfig>(config);
    assert(summaryConfig);

    auto doc = std::make_unique<indexlib::document::SearchSummaryDocument>(nullptr, summaryConfig->GetSummaryCount());
    auto [status, ret] = summaryReader->GetDocument(docid, doc.get());
    RETURN_STATUS_DIRECTLY_IF_ERROR(status);
    if (!ret) {
        return Status::InternalError("get document for docid [%d] failed", docid);
    }
    for (const auto& summary : summarys) {
        fieldid_t fieldId = schema->GetFieldId(summary);
        index::summaryfieldid_t summaryFieldId = summaryConfig->GetSummaryFieldId(fieldId);
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

Status NormalTabletSearcher::QueryRowSourceByDocId(const docid_t docid, const std::vector<std::string>& sources,
                                                   base::Row& row) const
{
    // 如果@sources为空，则以实际取出来的为准
    auto sourceReader = _normalTabletReader->GetSourceReader();
    if (!sourceReader) {
        if (sources.empty()) {
            return Status::OK();
        }
        return Status::InternalError("get source reader failed!");
    }

    indexlib::document::SourceDocument sourceDoc(nullptr);
    auto status = sourceReader->GetDocument(docid, &sourceDoc);
    RETURN_IF_STATUS_ERROR(status, "get document for docid [%d] failed", docid);
    std::vector<std::string> fieldNames;
    std::vector<std::string> fieldValues;
    sourceDoc.ExtractFields(fieldNames, fieldValues);

    for (size_t i = 0; i < fieldNames.size(); ++i) {
        if (sources.empty() || std::find(sources.begin(), sources.end(), fieldNames[i]) != sources.end()) {
            auto sourceValue = row.add_sourcevalues();
            sourceValue->set_fieldname(fieldNames[i]);
            sourceValue->set_value(fieldValues[i]);
        }
    }
    return Status::OK();
}

Status NormalTabletSearcher::QueryIndexByDocId(const std::vector<std::string>& attrs,
                                               const std::vector<std::string>& summarys,
                                               const std::vector<std::string>& sources,
                                               const std::vector<docid_t>& docids, const int64_t limit,
                                               base::PartitionResponse& partitionResponse) const
{
    for (int docCount = 0; docCount < std::min(docids.size(), size_t(limit)); ++docCount) {
        auto row = partitionResponse.add_rows();
        auto docid = docids[docCount];
        row->set_docid(docid);
        RETURN_STATUS_DIRECTLY_IF_ERROR(QueryRowAttrByDocId(docid, attrs, *row));
        RETURN_STATUS_DIRECTLY_IF_ERROR(QueryRowSummaryByDocId(docid, summarys, *row));
        RETURN_STATUS_DIRECTLY_IF_ERROR(QueryRowSourceByDocId(docid, sources, *row));
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

Status NormalTabletSearcher::QueryDocIdsByTerm(
    const std::shared_ptr<indexlib::index::InvertedIndexReader>& indexReader,
    const std::shared_ptr<indexlibv2::index::DeletionMapIndexReader>& deletionMapReader,
    const indexlib::index::Term& term, const int64_t limit, PostingType postingType, std::vector<docid_t>& docIds,
    base::IndexTermMeta& indexTermMeta)
{
    autil::mem_pool::Pool pool;
    auto postingIter =
        autil::shared_ptr_pool_deallocated(&pool, indexReader->Lookup(term, limit, postingType, &pool).ValueOrThrow());
    if (!postingIter) {
        return Status::NotFound("create index posting iterator for index name [%s] failed",
                                term.GetIndexName().c_str());
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
    return Status::OK();
}

std::pair<Status, std::shared_ptr<indexlib::index::Term>>
NormalTabletSearcher::CreateTerm(const std::shared_ptr<indexlibv2::config::InvertedIndexConfig>& indexConfig,
                                 const std::string& indexValue, const std::string& truncateName)
{
    std::string indexName = indexConfig->GetIndexName();
    if (indexValue == NULL_VALUE) {
        return {Status::OK(), std::make_shared<indexlib::index::Term>(indexName)};
    } else if (indexConfig->GetInvertedIndexType() == it_date || indexConfig->GetInvertedIndexType() == it_range) {
        int64_t leftNumber, rightNumber;
        if (!ParseRangeQueryStr(indexConfig, indexValue, leftNumber, rightNumber)) {
            auto status = Status::InvalidArgs(
                "illegal query format, expected \"(xxx, xxx]\" instead of \"%s\" on index type [%s]",
                indexValue.c_str(),
                indexlibv2::config::InvertedIndexConfig::InvertedIndexTypeToStr(indexConfig->GetInvertedIndexType()));
            return {status, nullptr};
        }
        auto term = std::make_shared<indexlib::index::Int64Term>(leftNumber, true, rightNumber, true, indexName);
        return {Status::OK(), term};
    } else {
        auto term = std::make_shared<indexlib::index::Term>(indexName);
        term->SetWord(indexValue);
        term->SetTruncateName(truncateName);
        return {Status::OK(), term};
    }
}

bool NormalTabletSearcher::ParseRangeQueryStr(
    const std::shared_ptr<indexlibv2::config::InvertedIndexConfig>& indexConfig, const std::string& range,
    int64_t& leftTimestamp, int64_t& rightTimestamp)
{
    if (range.empty()) {
        return false;
    }

    std::vector<std::string> strVec = autil::StringUtil::split(range, ",");
    if ((int)strVec.size() != 2 || strVec[0].length() < 1 || strVec[1].length() < 1) {
        return false;
    }

    if (strVec[0][0] != '(' && strVec[0][0] != '[') {
        return false;
    }

    if (strVec[1].back() != ')' && strVec[1].back() != ']') {
        return false;
    }
    int rightNumberLength = (int)strVec[1].length() - 1;
    bool isLeftClose = strVec[0][0] == '[';
    bool isRightClose = strVec[1].back() == ']';
    if (strVec[0].length() == 1 && strVec[0][0] == '(') {
        leftTimestamp = std::numeric_limits<int64_t>::min();
        isLeftClose = true;
    } else if (!ParseTimestamp(indexConfig, strVec[0].substr(1), leftTimestamp)) {
        return false;
    }
    if (strVec[1].length() == 1 && strVec[1][0] == ')') {
        rightTimestamp = std::numeric_limits<int64_t>::max();
        isRightClose = true;
    } else if (!ParseTimestamp(indexConfig, strVec[1].substr(0, rightNumberLength), rightTimestamp)) {
        return false;
    }
    if (!isLeftClose)
        leftTimestamp++;
    if (!isRightClose)
        rightTimestamp--;

    return leftTimestamp <= rightTimestamp;
}

bool NormalTabletSearcher::ParseTimestamp(const std::shared_ptr<indexlibv2::config::InvertedIndexConfig>& indexConfig,
                                          const std::string& str, int64_t& value)
{
    indexlib::InvertedIndexType type = indexConfig->GetInvertedIndexType();
    if (it_range == type) {
        return autil::StringUtil::numberFromString(str, value);
    }

    assert(it_datetime == indexConfig->GetInvertedIndexType());
    auto dateIndexConfig = std::dynamic_pointer_cast<indexlibv2::config::DateIndexConfig>(indexConfig);
    assert(dateIndexConfig);
    auto fieldConfig = dateIndexConfig->GetFieldConfig();
    uint64_t tmp;
    if (!indexlib::util::TimestampUtil::ConvertToTimestamp(fieldConfig->GetFieldType(), autil::StringView(str), tmp,
                                                           fieldConfig->GetDefaultTimeZoneDelta()) &&
        !indexlib::util::TimestampUtil::ConvertToTimestamp(ft_uint64, autil::StringView(str), tmp, 0)) {
        return false;
    }
    value = tmp;
    return true;
}

} // namespace indexlibv2::table
