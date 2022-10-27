#ifndef ISEARCH_SEARCH_TERMSCORER_H_
#define ISEARCH_SEARCH_TERMSCORER_H_
#include <ha3/common.h>
#include <ha3/search/QueryExecutor.h>
#include <ha3/search/IndexPartitionReaderWrapper.h>
#include <indexlib/index/normal/inverted_index/accessor/posting_iterator.h>
#include <ha3/rank/MatchData.h>
#include <ha3/rank/MetaInfo.h>

BEGIN_HA3_NAMESPACE(search);

class TermQueryExecutor : public QueryExecutor
{
public:
    TermQueryExecutor(IE_NAMESPACE(index)::PostingIterator *iter,
                      const common::Term &term);
    virtual ~TermQueryExecutor();
private:
    TermQueryExecutor(const TermQueryExecutor &);
    TermQueryExecutor& operator=(const TermQueryExecutor &);
public:
    const std::string getName() const override {
        return "TermQueryExecutor";
    }
    df_t getDF(GetDFType type) const override;
    void reset() override;
    void moveToEnd() override {
        setDocId(END_DOCID);
    }

    IE_NAMESPACE(common)::ErrorCode seekSubDoc(
        docid_t docId, docid_t subDocId,
        docid_t subDocEnd, bool needSubMatchdata, docid_t& result) override;
    
    bool isMainDocHit(docid_t docId) const override {
        return true;
    }

    IE_NAMESPACE(common)::ErrorCode seekPosition(pos_t pos, pos_t& result) {
        assert(_iter);
        return _iter->SeekPositionWithErrorCode(pos, result);
    }

    bool hasPosition() const {
        if (_iter) {
            return _iter->HasPosition();
        }
        return false;
    }

    std::string getIndexName() const {
        return _term.getIndexName();
    }

    void setMainChainDF(df_t df) {
        _mainChainDF = df;
    }

    void setIndexPartitionReaderWrapper(HA3_NS(search)::IndexPartitionReaderWrapper *indexPartWrapper) {
        _indexPartReaderWrapper = indexPartWrapper;
    }

    IE_NAMESPACE(index)::PostingIterator *getPostingIterator() {
        return _iter;
    }

public:
    IE_NAMESPACE(common)::ErrorCode unpackMatchData(rank::TermMatchData &tmd) override {
        assert(_iter);
        return _iter->UnpackWithErrorCode(tmd.getTermMatchData());
    }
    void getMetaInfo(rank::MetaInfo *metaInfo) const override;

    matchvalue_t getMatchValue() {
        return _iter->GetMatchValue();
    }
public:
    std::string toString() const override;
protected:
    IE_NAMESPACE(common)::ErrorCode doSeek(docid_t id, docid_t& result) override;
protected:
    IE_NAMESPACE(index)::PostingIterator *_iter;
    IE_NAMESPACE(index)::PostingIterator *_backupIter;
    common::Term _term;
    df_t _mainChainDF;
private:
    HA3_NS(search)::IndexPartitionReaderWrapper *_indexPartReaderWrapper;
private:
    HA3_LOG_DECLARE();
};
HA3_TYPEDEF_PTR(TermQueryExecutor);

END_HA3_NAMESPACE(search);
#endif
