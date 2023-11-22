#include "indexlib/table/normal_table/test/NormalTableTestSearcher.h"

#include "autil/mem_pool/Pool.h"
#include "indexlib/config/TabletSchema.h"
#include "indexlib/document/normal/SearchSummaryDocument.h"
#include "indexlib/document/normal/SourceDocument.h"
#include "indexlib/index/ann/ANNIndexConfig.h"
#include "indexlib/index/ann/aitheta2/AithetaIndexReader.h"
#include "indexlib/index/attribute/AttributeReader.h"
#include "indexlib/index/attribute/Common.h"
#include "indexlib/index/common/field_format/range/RangeFieldEncoder.h"
#include "indexlib/index/deletionmap/Common.h"
#include "indexlib/index/inverted_index/Common.h"
#include "indexlib/index/inverted_index/InvertedIndexReader.h"
#include "indexlib/index/inverted_index/PostingIterator.h"
#include "indexlib/index/inverted_index/config/SingleFieldIndexConfig.h"
#include "indexlib/index/pack_attribute/Common.h"
#include "indexlib/index/pack_attribute/PackAttributeReader.h"
#include "indexlib/index/primary_key/Common.h"
#include "indexlib/index/primary_key/PrimaryKeyIndexReader.h"
#include "indexlib/index/primary_key/PrimaryKeyReader.h"
#include "indexlib/index/source/Common.h"
#include "indexlib/index/source/SourceReader.h"
#include "indexlib/index/source/config/SourceIndexConfig.h"
#include "indexlib/index/summary/Common.h"
#include "indexlib/index/summary/SummaryReader.h"
#include "indexlib/index/summary/config/SummaryIndexConfig.h"
#include "indexlib/table/normal_table/NormalTabletSessionReader.h"
#include "indexlib/table/normal_table/test/NormalTableTestHelper.h"
#include "indexlib/table/normal_table/test/Query.h"
#include "indexlib/table/normal_table/test/RangeQueryParser.h"
#include "indexlib/table/test/DocumentParser.h"
#include "indexlib/table/test/RawDocument.h"
#include "indexlib/table/test/Result.h"

namespace indexlibv2 { namespace table {
AUTIL_LOG_SETUP(indexlib.table, NormalTableTestSearcher);

PostingType GetPostingTypeAndMaybeAdjustTerm(indexlib::index::Term* term)
{
    static const std::map<std::string, PostingType> postingTypeMap = {
        {"pt_bitmap", pt_bitmap}, {"pt_normal", pt_normal}, {"pt_default", pt_default}};
    PostingType postingType = pt_default;
    auto pos = term->GetWord().find(NormalTableTestHelper::POSTING_TYPE_SEPARATOR);
    if (pos != std::string::npos) {
        std::string word = term->GetWord().substr(0, pos);
        std::string postingTypeStr = term->GetWord().substr(pos + 1);
        postingType = postingTypeMap.at(postingTypeStr);
        term->SetWord(word);
    }
    return postingType;
}

std::shared_ptr<Query> CreateTermQuery(const std::string& indexName, const indexlib::index::Term& term,
                                       PostingType postingType,
                                       std::shared_ptr<indexlib::index::InvertedIndexReader> reader,
                                       autil::mem_pool::Pool* pool)
{
    indexlib::index::PostingIterator* iter = reader->Lookup(term, 1000, postingType, pool).Value();
    if (!iter) {
        indexlib::IE_POOL_COMPATIBLE_DELETE_CLASS(pool, iter);
        return nullptr;
    }
    auto query = std::make_shared<TermQuery>();
    query->Init(iter, pool, indexName);
    return query;
}

static std::shared_ptr<Query> CreateDocidQuery(const std::string& indexName, const std::string& termString)
{
    auto query = std::make_shared<DocidQuery>();
    query->Init(termString);
    return query;
}

static std::shared_ptr<Query>
CreatePrimaryKeyQuery(const std::shared_ptr<indexlib::index::PrimaryKeyIndexReader>& pkReader, const std::string& pkStr)
{
    auto docId = pkReader->Lookup(pkStr);
    if (docId == INVALID_DOCID) {
        return nullptr;
    }
    auto query = std::make_shared<PrimaryKeyQuery>();
    query->Init(docId);
    return query;
}

NormalTableTestSearcher::NormalTableTestSearcher(const std::shared_ptr<framework::ITabletReader>& tabletReader,
                                                 const std::shared_ptr<config::ITabletSchema>& tabletSchema)
    : _tabletReader(tabletReader)
    , _schema(tabletSchema)
{
    _primaryKeyIndexName = "";
    auto config = _schema->GetPrimaryKeyIndexConfig();
    if (config) {
        auto pkConfig = std::dynamic_pointer_cast<indexlibv2::index::PrimaryKeyIndexConfig>(config);
        if (pkConfig) {
            _primaryKeyIndexName = pkConfig->GetIndexName();
        }
    }
}

std::shared_ptr<Query> NormalTableTestSearcher::ParseQuery(const std::string& queryStr, autil::mem_pool::Pool* pool)
{
    if (queryStr.empty()) {
        return nullptr;
    }
    auto pos = queryStr.find(NormalTableTestHelper::INDEX_TERM_SEPARATOR);
    assert(pos != std::string::npos);
    std::string indexName = queryStr.substr(0, pos);
    std::string termString = queryStr.substr(pos + 1);

    bool isSubReader = false;
    if ((!isSubReader && indexName == NormalTableTestHelper::DOCID) ||
        (isSubReader && indexName == NormalTableTestHelper::SUBDOCID)) {
        return CreateDocidQuery(indexName, termString);
    }
    assert(_tabletReader);
    if (indexName == _primaryKeyIndexName) {
        auto reader = std::dynamic_pointer_cast<indexlib::index::PrimaryKeyIndexReader>(
            _tabletReader->GetIndexReader(index::PRIMARY_KEY_INDEX_TYPE_STR, indexName));
        if (reader != nullptr) {
            return CreatePrimaryKeyQuery(reader, termString);
        }
        return nullptr;
    }

    auto invertedReader = std::dynamic_pointer_cast<indexlib::index::InvertedIndexReader>(
        _tabletReader->GetIndexReader(indexlib::index::INVERTED_INDEX_TYPE_STR, indexName));
    if (invertedReader) {
        auto invertedConfig = std::dynamic_pointer_cast<indexlibv2::config::InvertedIndexConfig>(
            _tabletReader->GetSchema()->GetIndexConfig(indexlib::index::GENERAL_INVERTED_INDEX_TYPE_STR, indexName));
        if (invertedConfig && !invertedConfig->IsNormal()) {
            return nullptr;
        }
        if (termString == NormalTableTestHelper::NULL_TERM ||
            (invertedConfig && invertedConfig->SupportNull() &&
             invertedConfig->GetNullTermLiteralString() == termString)) {
            // lookup null term
            indexlib::index::Term term(indexName);
            PostingType postingType = GetPostingTypeAndMaybeAdjustTerm(&term);
            return CreateTermQuery(indexName, term, postingType, invertedReader, pool);
        }

        if (invertedConfig && (it_datetime == invertedConfig->GetInvertedIndexType() ||
                               it_range == invertedConfig->GetInvertedIndexType())) {
            int64_t leftNumber, rightNumber;
            if (!RangeQueryParser::ParseQueryStr(invertedConfig, termString, leftNumber, rightNumber)) {
                return std::shared_ptr<Query>();
            }
            auto singleIndexConfig =
                std::dynamic_pointer_cast<indexlibv2::config::SingleFieldIndexConfig>(invertedConfig);
            assert(singleIndexConfig);
            if (indexlib::index::RangeFieldEncoder::UINT == indexlib::index::RangeFieldEncoder::GetRangeFieldType(
                                                                singleIndexConfig->GetFieldConfig()->GetFieldType()) &&
                leftNumber < 0) {
                leftNumber = 0;
            }
            indexlib::index::Int64Term term(leftNumber, true, rightNumber, true, indexName);
            PostingType postingType = GetPostingTypeAndMaybeAdjustTerm(&term);
            return CreateTermQuery(indexName, term, postingType, invertedReader, pool);
        }

        indexlib::index::Term term(termString, indexName);
        PostingType postingType = GetPostingTypeAndMaybeAdjustTerm(&term);
        return CreateTermQuery(indexName, term, postingType, invertedReader, pool);
    }
    return nullptr;
}

std::shared_ptr<Query>
NormalTableTestSearcher::ParseANNQuery(const std::string& queryStr, autil::mem_pool::Pool* pool,
                                       std::shared_ptr<index::ann::AithetaAuxSearchInfoBase> searchInfo)
{
    if (queryStr.empty()) {
        return nullptr;
    }
    auto pos = queryStr.find(NormalTableTestHelper::INDEX_TERM_SEPARATOR);
    assert(pos != std::string::npos);
    std::string indexName = queryStr.substr(0, pos);
    std::string termString = queryStr.substr(pos + 1);

    assert(_tabletReader);

    auto aithetaReader = std::dynamic_pointer_cast<indexlibv2::index::ann::AithetaIndexReader>(
        _tabletReader->GetIndexReader(indexlibv2::index::ANN_INDEX_TYPE_STR, indexName));
    if (aithetaReader) {
        std::string output;
        _tabletReader->GetSchema()->Serialize(false, &output);
        auto aithetaConfig = std::dynamic_pointer_cast<indexlibv2::config::ANNIndexConfig>(
            _tabletReader->GetSchema()->GetIndexConfig(indexlibv2::index::ANN_INDEX_TYPE_STR, indexName));
        if (!aithetaConfig) {
            AUTIL_SLOG(ERROR) << "ann no config " << indexName;
            return nullptr;
        }

        indexlibv2::index::ann::AithetaTerm term;
        term.SetAithetaSearchInfo(searchInfo);
        term.SetWord(termString);
        PostingType postingType = GetPostingTypeAndMaybeAdjustTerm(&term);
        return CreateTermQuery(indexName, term, postingType, aithetaReader, pool);
    }
    return nullptr;
}

std::string NormalTableTestSearcher::GetAttributeValue(const std::string& attributeName, docid_t docId)
{
    assert(_tabletReader);
    auto attributeReader = std::dynamic_pointer_cast<index::AttributeReader>(
        _tabletReader->GetIndexReader(index::ATTRIBUTE_INDEX_TYPE_STR, attributeName));
    if (!attributeReader) {
        AUTIL_LOG(ERROR, "cast attribute [%s] failed", attributeName.c_str());
        return "";
    }
    autil::mem_pool::Pool pool;
    std::string value;
    if (!attributeReader->Read(docId, value, &pool)) {
        AUTIL_LOG(WARN, "doc [%d] get attribute [%s] failed", docId, attributeName.c_str());
    }
    return value;
}

std::string NormalTableTestSearcher::GetPackAttributeValue(const std::string& packName,
                                                           const std::string& subAttributeName, docid_t docId)
{
    assert(_tabletReader);
    auto packAttributeReader = std::dynamic_pointer_cast<index::PackAttributeReader>(
        _tabletReader->GetIndexReader(index::PACK_ATTRIBUTE_INDEX_TYPE_STR, packName));
    if (!packAttributeReader) {
        AUTIL_LOG(ERROR, "cast pack attribute [%s] failed", packName.c_str());
        return "";
    }
    autil::mem_pool::Pool pool;
    std::string value;
    if (!packAttributeReader->Read(docId, subAttributeName, value, &pool)) {
        AUTIL_LOG(WARN, "doc [%d] get pack attribute [%s:%s] failed", docId, packName.c_str(),
                  subAttributeName.c_str());
    }
    return value;
}

void NormalTableTestSearcher::GetSourceValue(docid_t docId, std::shared_ptr<RawDocument>& rawDoc)
{
    auto sourceReader = std::dynamic_pointer_cast<index::SourceReader>(
        _tabletReader->GetIndexReader(index::SOURCE_INDEX_TYPE_STR, index::SOURCE_INDEX_NAME));
    if (nullptr == sourceReader) {
        return;
    }
    indexlib::document::SourceDocument sourceDoc(nullptr);
    auto status = sourceReader->GetDocument(docId, &sourceDoc);
    if (!status.IsOK()) {
        return;
    }
    std::vector<std::string> fieldNames;
    std::vector<std::string> fieldValues;
    sourceDoc.ExtractFields(fieldNames, fieldValues);
    for (size_t i = 0; i < fieldNames.size(); ++i) {
        rawDoc->SetField(fieldNames[i], fieldValues[i]);
    }
}

void NormalTableTestSearcher::GetSummaryValue(docid_t docId, std::shared_ptr<RawDocument>& rawDoc)
{
    auto summaryReader = std::dynamic_pointer_cast<index::SummaryReader>(
        _tabletReader->GetIndexReader(index::SUMMARY_INDEX_TYPE_STR, index::SUMMARY_INDEX_NAME));
    if (nullptr == summaryReader) {
        return;
    }
    indexlib::document::SearchSummaryDocument summaryDoc(nullptr, 4096);
    auto [status, ret] = summaryReader->GetDocument(docId, &summaryDoc);
    if (!status.IsOK() || !ret) {
        return;
    }
    auto schema = _tabletReader->GetSchema();
    auto summaryIndexConfig = std::dynamic_pointer_cast<config::SummaryIndexConfig>(
        schema->GetIndexConfig(index::SUMMARY_INDEX_TYPE_STR, index::SUMMARY_INDEX_NAME));
    assert(summaryIndexConfig);
    for (const auto& fieldConfig : schema->GetFieldConfigs()) {
        auto fieldId = fieldConfig->GetFieldId();
        if (summaryIndexConfig->IsInSummary(fieldId)) {
            std::string fieldName = fieldConfig->GetFieldName();
            auto summaryFieldId = summaryIndexConfig->GetSummaryFieldId(fieldId);
            const autil::StringView* fieldValue = summaryDoc.GetFieldValue(summaryFieldId);
            if (!fieldValue) {
                continue;
            }
            std::string fieldValueStr = fieldValue->to_string();
            if (fieldConfig->IsMultiValue()) {
                fieldValueStr = DocumentParser::ParseMultiValueResult(fieldValueStr);
            }
            rawDoc->SetField(fieldName, fieldValueStr);
        }
    }
}

void NormalTableTestSearcher::FillResult(std::shared_ptr<Query> query, docid_t docId, std::shared_ptr<Result> result,
                                         bool isDeleted)
{
    auto rawDoc = std::make_shared<RawDocument>();
    rawDoc->SetField(FIELD_DOCID, autil::StringUtil::toString<docid_t>(docId));
    rawDoc->SetDocId(docId);
    rawDoc->SetField(FIELD_DELETION_MAP, isDeleted ? "1" : "0");
    // FillPosition(query, rawDoc);
    const auto& schema = _tabletReader->GetSchema();

    // fill source
    auto sourceIndexConfig = std::dynamic_pointer_cast<config::SourceIndexConfig>(
        schema->GetIndexConfig(index::SOURCE_INDEX_TYPE_STR, index::SOURCE_INDEX_NAME));
    if (sourceIndexConfig) {
        GetSourceValue(docId, rawDoc);
    }

    // fill summary
    auto summaryIndexConfig = std::dynamic_pointer_cast<config::SummaryIndexConfig>(
        schema->GetIndexConfig(index::SUMMARY_INDEX_TYPE_STR, index::SUMMARY_INDEX_NAME));
    if (summaryIndexConfig) {
        GetSummaryValue(docId, rawDoc);
    }
    // fill attribute
    auto attributeIndexConfigs = schema->GetIndexConfigs(index::ATTRIBUTE_INDEX_TYPE_STR);
    for (const auto& indexConfig : attributeIndexConfigs) {
        auto attributeConfig = std::dynamic_pointer_cast<indexlibv2::index::AttributeConfig>(indexConfig);
        assert(attributeConfig);
        auto fieldId = attributeConfig->GetFieldId();
        if (summaryIndexConfig && summaryIndexConfig->IsInSummary(fieldId)) {
            continue;
        }
        std::string fieldName = attributeConfig->GetIndexName();
        std::string fieldValue = GetAttributeValue(fieldName, docId);
        if (attributeConfig->IsMultiValue()) {
            fieldValue = DocumentParser::ParseMultiValueResult(fieldValue);
        }
        rawDoc->SetField(fieldName, fieldValue);
    }
    // fill pack attribute
    auto packAttributeIndexConfigs = schema->GetIndexConfigs(index::PACK_ATTRIBUTE_INDEX_TYPE_STR);
    for (const auto& indexConfig : packAttributeIndexConfigs) {
        auto packAttributeConfig = std::dynamic_pointer_cast<indexlibv2::index::PackAttributeConfig>(indexConfig);
        assert(packAttributeConfig);
        const auto& attributeConfigs = packAttributeConfig->GetAttributeConfigVec();
        const auto& packName = packAttributeConfig->GetPackName();
        for (const auto& attributeConfig : attributeConfigs) {
            auto fieldId = attributeConfig->GetFieldId();
            if (summaryIndexConfig && summaryIndexConfig->IsInSummary(fieldId)) {
                continue;
            }
            std::string fieldName = attributeConfig->GetIndexName();
            std::string fieldValue = GetPackAttributeValue(packName, fieldName, docId);
            if (attributeConfig->IsMultiValue()) {
                fieldValue = DocumentParser::ParseMultiValueResult(fieldValue);
            }
            rawDoc->SetField(fieldName, fieldValue);
        }
    }
    result->AddDoc(rawDoc);
}

std::shared_ptr<Result> NormalTableTestSearcher::Search(std::shared_ptr<Query> query, bool ignoreDeletionMap)
{
    std::shared_ptr<index::DeletionMapIndexReader> deletionMapReader;
    if (!ignoreDeletionMap) {
        deletionMapReader = std::dynamic_pointer_cast<index::DeletionMapIndexReader>(
            _tabletReader->GetIndexReader(index::DELETION_MAP_INDEX_TYPE_STR, "deletionmap"));
    }

    auto result = std::make_shared<Result>();
    docid_t docid = INVALID_DOCID;
    do {
        docid_t nextDocid = query->Seek(docid);
        assert(nextDocid == INVALID_DOCID || nextDocid > docid);
        docid = nextDocid;
        if (docid != INVALID_DOCID) {
            bool isDeleted = (deletionMapReader && deletionMapReader->IsDeleted(docid));
            if (isDeleted) {
                continue;
            }
            FillResult(query, docid, result, isDeleted);
        }
    } while (docid != INVALID_DOCID);
    return result;
}

std::shared_ptr<Result> NormalTableTestSearcher::Search(const std::string& queryStr, bool ignoreDeletionMap)
{
    autil::mem_pool::Pool pool;
    auto query = ParseQuery(queryStr, &pool);
    if (!query) {
        AUTIL_LOG(INFO, "query [%s] get null result", queryStr.c_str());
        return nullptr;
    }
    return Search(query, ignoreDeletionMap);
}

std::shared_ptr<Result>
NormalTableTestSearcher::SearchANN(const std::string& queryStr, bool ignoreDeletionMap,
                                   std::shared_ptr<index::ann::AithetaAuxSearchInfoBase> searchInfo)
{
    autil::mem_pool::Pool pool;
    auto query = ParseANNQuery(queryStr, &pool, searchInfo);
    if (!query) {
        AUTIL_LOG(INFO, "query [%s] get null result", queryStr.c_str());
        return nullptr;
    }
    return Search(query, ignoreDeletionMap);
}

}} // namespace indexlibv2::table
