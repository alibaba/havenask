#pragma once

#include <ha3/sql/ops/scan/ScanBase.h>
#include <ha3/sql/ops/calc/CalcTable.h>
#include <ha3/proto/BasicDefs.pb.h>
#include <indexlib/document/index_document/normal_document/search_summary_document.h>
#include <indexlib/index/normal/summary/summary_reader.h>

BEGIN_HA3_NAMESPACE(sql);

class SummaryScan : public ScanBase {
private:
    typedef std::vector<IE_NAMESPACE(document)::SearchSummaryDocument> SearchSummaryDocVecType;
public:
    SummaryScan(bool requirePk = true);
    virtual ~SummaryScan();
private:
    SummaryScan(const SummaryScan&);
    SummaryScan& operator=(const SummaryScan &);
public:
    bool init(const ScanInitParam &param) override;
    bool doBatchScan(TablePtr &table, bool &eof) override;
    bool updateScanQuery(const StreamQueryPtr &inputQuery) override;

private:
    void initSummary();
    void initExtraSummary(SqlBizResource *bizResource, SqlQueryResource *queryResource);
    bool genDocIdFromQuery(const common::QueryPtr &query);
    bool genRawDocIdFromQuery(const common::QueryPtr &query, std::vector<std::string> &rawPks);
    bool parseQuery(std::vector<std::string> &pks, bool &needFilter);
    bool prepareFields();
    bool convertPK2DocId(const std::vector<primarykey_t> &pks);
    template <typename PKType>
    bool convertPK2DocId(
            const std::vector<primarykey_t> &pks,
            const std::vector<IE_NAMESPACE(index)::IndexReader *> &primaryKeyReaders);
    template <typename PKType>
    static PKType toPrimaryKeyType(primarykey_t pk);

    bool prepareSummaryReader();
    void  filterDocIdByRange(const std::vector<std::string> &rawPks,
                             const proto::Range &range,
                             std::vector<primarykey_t> &pks);
    primarykey_t calculatePrimaryKey(const std::string &rawPk);
    bool getSummaryDocs(const suez::turing::SummaryInfo *summaryInfo,
                        const std::vector<matchdoc::MatchDoc> &matchDocs,
                        SearchSummaryDocVecType &summaryDocs);
    template <typename T>
    bool fillSummaryDocs(const matchdoc::Reference<T> *ref,
                         const SearchSummaryDocVecType &summaryDocs,
                         const std::vector<matchdoc::MatchDoc> &matchDocs,
                         int32_t summaryFieldId);
    bool fillSummary(const std::vector<matchdoc::MatchDoc> &matchDocs);
    bool fillAttributes(const std::vector<matchdoc::MatchDoc> &matchDocs);
    TablePtr createTable(std::vector<matchdoc::MatchDoc> &matchDocVec,
                         const matchdoc::MatchDocAllocatorPtr &matchDocAllocator,
                         bool reuseMatchDocAllocator);
    void adjustUsedFields();
private:
    std::vector<search::IndexPartitionReaderWrapperPtr> _indexPartitionReaderWrappers;
    std::vector<suez::turing::AttributeExpressionCreatorPtr> _attributeExpressionCreators;
    bool _requirePk;
    std::vector<docid_t> _docIds;
    std::vector<int> _tableIdx;
    std::vector<std::vector<suez::turing::AttributeExpression *>> _tableAttributes;
    proto::Range _range;
    autil::HashFunctionBasePtr _hashFuncPtr;
    std::string _tableMeta;
    CalcTablePtr _calcTable;
    std::vector<IE_NAMESPACE(index)::SummaryReaderPtr> _summaryReaders;
private:
    HA3_LOG_DECLARE();
};


template <typename PKType>
inline PKType SummaryScan::toPrimaryKeyType(primarykey_t pk) {
    return pk;
}

template <>
inline uint64_t SummaryScan::toPrimaryKeyType<uint64_t>(primarykey_t pk) {
    return pk.value[1];
}

template <typename PKType>
bool SummaryScan::convertPK2DocId(
        const std::vector<primarykey_t> &pks,
        const std::vector<IE_NAMESPACE(index)::IndexReader *> &primaryKeyReaders)
{
    std::vector<IE_NAMESPACE(index)::PrimaryKeyIndexReaderTyped<PKType>*> pkIndexReaders;
    for (auto &pkReader : primaryKeyReaders) {
        auto pkIndexReader = dynamic_cast<
            IE_NAMESPACE(index)::PrimaryKeyIndexReaderTyped<PKType>*>(pkReader);
        if (!pkIndexReader) {
            SQL_LOG(ERROR, "can not get primaryKeyIndexReader");
            return false;
        }
        pkIndexReaders.emplace_back(pkIndexReader);
    }

    int tableSize = pkIndexReaders.size();
    _docIds.clear();
    _tableIdx.clear();
    for (const auto &pk : pks) {
        PKType pkTyped = toPrimaryKeyType<PKType>(pk);
        docid_t lastDocId = INVALID_DOCID;
        for (int i = 0; i < tableSize; ++i) {
            docid_t docId = pkIndexReaders[i]->Lookup(pkTyped, lastDocId);
            if (docId != INVALID_DOCID) {
                _docIds.emplace_back(docId);
                _tableIdx.emplace_back(i);
                break;
            }
        }
    }
    return true;
}

HA3_TYPEDEF_PTR(SummaryScan);

END_HA3_NAMESPACE(sql);
