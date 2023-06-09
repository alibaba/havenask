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
#include "indexlib/index/attribute/AttrHelper.h"
#include "indexlib/index/attribute/AttributeIteratorBase.h"
#include "indexlib/index/attribute/AttributeIteratorTyped.h"
#include "indexlib/index/attribute/AttributeReader.h"
#include "indexlib/index/attribute/Common.h"
#include "indexlib/index/common/NumberTerm.h"
#include "indexlib/index/deletionmap/DeletionMapIndexReader.h"
#include "indexlib/index/inverted_index/Common.h"
#include "indexlib/index/inverted_index/InDocSectionMeta.h"
#include "indexlib/index/inverted_index/MultiFieldIndexReader.h"
#include "indexlib/index/inverted_index/PostingIterator.h"
#include "indexlib/index/inverted_index/SectionAttributeReader.h"
#include "indexlib/index/inverted_index/config/DateIndexConfig.h"
#include "indexlib/index/inverted_index/config/PackageIndexConfig.h"
#include "indexlib/index/inverted_index/format/TermMeta.h"
#include "indexlib/index/primary_key/PrimaryKeyIndexReader.h"
#include "indexlib/table/BuiltinDefine.h"
#include "indexlib/table/common/SearchUtil.h"
#include "indexlib/table/normal_table/Common.h"
#include "indexlib/table/normal_table/NormalTabletReader.h"
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

/* json format:
   {
   "docid_list" : [ 0, 1, ...],
   "pk_list" : [ "key1", "key2", ...],
   "condition" : {
       "index_name" : "index1",
       "values" : [ "value1", "value2", ...]
   },
   "ignore_deletionmap" : false,
   "truncate_name" : "__bitmap__",
   "limit" : 10,
   "attributes" : ["attr1", "attr2", ...]
   }
*/
Status NormalTabletSearcher::Search(const std::string& jsonQuery, std::string& result) const
{
    std::vector<int64_t> docidVec;
    std::vector<std::string> pkList;
    std::vector<std::string> attributes;

    base::PartitionQuery query;
    try {
        autil::legacy::Any a = autil::legacy::json::ParseJson(jsonQuery);
        autil::legacy::json::JsonMap am = autil::legacy::AnyCast<autil::legacy::json::JsonMap>(a);
        auto iter = am.find("ignore_deletionmap");
        if (iter != am.end()) {
            query.set_ignoredeletionmap(autil::legacy::AnyCast<bool>(iter->second));
        }
        iter = am.find("truncate_name");
        if (iter != am.end()) {
            query.set_truncatename(autil::legacy::AnyCast<std::string>(iter->second));
        }
        iter = am.find("pk_list");
        if (iter != am.end()) {
            autil::legacy::FromJson(pkList, iter->second);
        }
        iter = am.find("docid_list");
        if (iter != am.end()) {
            autil::legacy::FromJson(docidVec, iter->second);
        }
        iter = am.find("attributes");
        if (iter != am.end()) {
            autil::legacy::FromJson(attributes, iter->second);
        }
        iter = am.find("limit");
        if (iter != am.end()) {
            query.set_limit(autil::legacy::AnyCast<int64_t>(iter->second));
        }
        iter = am.find("condition");
        if (iter != am.end()) {
            auto conditionMap = autil::legacy::AnyCast<autil::legacy::json::JsonMap>(iter->second);
            auto condition = query.mutable_condition();
            auto it = conditionMap.find("index_name");
            if (it != conditionMap.end()) {
                condition->set_indexname(autil::legacy::AnyCast<std::string>(it->second));
            }

            std::vector<std::string> values;
            it = conditionMap.find("values");
            if (it != conditionMap.end()) {
                autil::legacy::FromJson(values, it->second);
            }
            condition->mutable_values()->CopyFrom({values.begin(), values.end()});
        }
    } catch (const std::exception& e) {
        return Status::InvalidArgs("invalid query [%s]", jsonQuery.c_str());
    }

    if (!pkList.empty()) {
        query.mutable_pk()->CopyFrom({pkList.begin(), pkList.end()});
    }
    if (!docidVec.empty()) {
        query.mutable_docid()->CopyFrom({docidVec.begin(), docidVec.end()});
    }
    if (!attributes.empty()) {
        query.mutable_attrs()->CopyFrom({attributes.begin(), attributes.end()});
    }

    base::PartitionResponse partitionResponse;
    auto ret = QueryIndex(query, partitionResponse);
    if (ret.IsOK()) {
        SearchUtil::ConvertResponseToStringFormat(partitionResponse, result);
    }
    return ret;
}

Status NormalTabletSearcher::QueryIndex(const base::PartitionQuery& query,
                                        base::PartitionResponse& partitionResponse) const
{
    if (_normalTabletReader->GetSchema()->GetTableType() != indexlib::table::TABLE_TYPE_NORMAL) {
        return Status::Unimplement("not support queryIndex for orc tablet");
    }
    std::vector<docid_t> docids;
    auto ret = QueryDocIds(query, partitionResponse, docids);
    if (!ret.IsOK()) {
        return ret;
    }

    IndexTableQueryType queryType = IndexTableQueryType::Unknown;
    RETURN_STATUS_DIRECTLY_IF_ERROR(ValidateQuery(query, queryType));
    const int64_t limit = query.has_limit() ? query.limit() : 10;

    auto attrs = ValidateAttrs(_normalTabletReader->GetSchema(),
                               std::vector<std::string> {query.attrs().begin(), query.attrs().end()});
    RETURN_STATUS_DIRECTLY_IF_ERROR(QueryIndexByDocId(attrs, docids, limit, partitionResponse));
    if (IndexTableQueryType::ByRawPk == queryType) {
        for (int i = 0; i < query.pk().size(); ++i) {
            partitionResponse.mutable_rows(i)->set_pk(query.pk(i));
        }
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

    size_t queryCounts =
        (queryByDocid ? 1 : 0) + (queryByRawPk ? 1 : 0) + (queryByCondition ? 1 : 0) + (queryByPkHash ? 1 : 0);
    if (queryCounts != 1) {
        return Status::InvalidArgs("PartitionQuery must contain exactly one of docids, pks, pknumbers and condition");
    }

    if (queryByDocid) {
        queryType = IndexTableQueryType::ByDocid;
    } else if (queryByRawPk) {
        queryType = IndexTableQueryType::ByRawPk;
    } else if (queryByCondition) {
        queryType = IndexTableQueryType::ByCondition;
    } else if (queryByPkHash) {
        queryType = IndexTableQueryType::ByPkHash;
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

std::vector<std::string> NormalTabletSearcher::ValidateAttrs(const std::shared_ptr<config::TabletSchema>& tabletSchema,
                                                             const std::vector<std::string>& attrs)
{
    auto legacySchema = tabletSchema->GetLegacySchema();
    std::vector<std::string> indexAttrs;
    const auto& indexConfigs = tabletSchema->GetIndexConfigs(index::ATTRIBUTE_INDEX_TYPE_STR);
    for (const auto& indexConfig : indexConfigs) {
        auto attrConfig = dynamic_cast<const config::AttributeConfig*>(indexConfig.get());
        if (attrConfig->IsDisable()) {
            continue;
        }
        indexAttrs.push_back(attrConfig->GetAttrName());
    }
    return indexlib::index::AttrHelper::ValidateAttrs(indexAttrs, attrs);
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
    const auto config = tabletSchema->GetIndexConfig(indexlib::index::INVERTED_INDEX_TYPE_STR, indexName);
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

Status NormalTabletSearcher::QueryRowByDocId(const docid_t docid, const std::vector<std::string>& attrs,
                                             base::Row& row) const
{
    const auto& schema = _normalTabletReader->GetSchema();
    autil::mem_pool::Pool pool;
    row.set_docid(docid);
    for (const auto& attr : attrs) {
        auto indexConfig = schema->GetIndexConfig(index::ATTRIBUTE_INDEX_TYPE_STR, /*indexName*/ attr);
        assert(indexConfig);
        auto attrConfig = std::dynamic_pointer_cast<config::AttributeConfig>(indexConfig);
        if (attrConfig->IsDisable()) {
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

Status NormalTabletSearcher::QueryIndexByDocId(const std::vector<std::string>& attrs,
                                               const std::vector<docid_t>& docids, const int64_t limit,
                                               base::PartitionResponse& partitionResponse) const
{
    for (int docCount = 0; docCount < std::min(docids.size(), size_t(limit)); ++docCount) {
        RETURN_STATUS_DIRECTLY_IF_ERROR(QueryRowByDocId(docids[docCount], attrs, *partitionResponse.add_rows()));
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
        return Status::NotFound("create index iterator for index name", term.GetIndexName().c_str(), "failed.");
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
