#ifndef ISEARCH_SEARCH_QUERYEXECUTOR_H_
#define ISEARCH_SEARCH_QUERYEXECUTOR_H_

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <stdlib.h>
#include <string>
#include <assert.h>
#include <ha3/rank/MetaInfo.h>
#include <ha3/rank/TermMatchData.h>
#include <indexlib/index/normal/inverted_index/accessor/doc_value_filter.h>
#include <indexlib/common/error_code.h>
// TODO: remove following line after refator
#include <assert.h>

BEGIN_HA3_NAMESPACE(search);

class QueryExecutor;
typedef std::vector<QueryExecutor*> SingleLayerExecutors;
typedef std::vector<QueryExecutor*> SingleLayerSeekExecutors;

class QueryExecutor
{
protected:
    static constexpr IE_NAMESPACE(common)::ErrorCode IE_OK = IE_NAMESPACE(common)::ErrorCode::OK;
public:
    class DFCompare {
    public:
        DFCompare() {
        }
    public:
        bool operator () (QueryExecutor *lft, QueryExecutor *rht) {
            return lft->getCurrentDF() < rht->getCurrentDF();
        }
    };

public:
    QueryExecutor() {
        _current = INVALID_DOCID;
        _currentSub = INVALID_DOCID;
        _seekDocCount = 0;
        _hasSubDocExecutor = true;
    }

    virtual ~QueryExecutor() {
    }

protected:
    enum GetDFType {
        GDT_MAIN_CHAIN,
        GDT_CURRENT_CHAIN
    };
public:
    virtual const std::string getName() const = 0;
    virtual IE_NAMESPACE(index)::DocValueFilter* stealFilter()
    { return NULL; }
    /**
     * seek starting from the *NEXT* docid in the posting list to find the
     * first docid which is not less than id. If there is no such a docid,
     * INVALID_DOCID will be returned.
     *
     * @param id the document id  to be found
     */
    IE_NAMESPACE(common)::ErrorCode seek(docid_t id, docid_t& result)
    {
        if (id > _current) {
            auto ec = doSeek(id, _current);
            IE_RETURN_CODE_IF_ERROR(ec);
        }
        result = _current;
        return IE_OK;
    }

    IE_NAMESPACE(common)::ErrorCode seekWithoutCheck(docid_t id, docid_t& result)
    {
        assert(id > _current);
        auto ret = doSeek(id, _current);
        result = _current;
        return ret;
    }
    IE_NAMESPACE(common)::ErrorCode seekSub(docid_t docId, docid_t subDocId,
                    docid_t subDocEnd, bool needSubMatchdata, docid_t& result)
    {
        auto ec = seekSubDoc(docId, subDocId, subDocEnd, needSubMatchdata, _currentSub);
        result = _currentSub;
        return ec;
    }

    virtual IE_NAMESPACE(common)::ErrorCode seekSubDoc(docid_t docId, docid_t subDocId,
                               docid_t subDocEnd, bool needSubMatchdata, docid_t& result) = 0;
    virtual void reset() {
        _current = INVALID_DOCID;
    }
    virtual bool isMainDocHit(docid_t docId) const = 0;
    bool hasSubDocExecutor() {return _hasSubDocExecutor; }
    virtual df_t getDF(GetDFType type) const = 0;
    virtual void moveToEnd() { _current = END_DOCID; }
    virtual void setCurrSub(docid_t docid) { _currentSub = docid; }
    virtual uint32_t getSeekDocCount() { return _seekDocCount; }
public:
    //only for test
    virtual std::string toString() const = 0;

public:
    docid_t legacySeek(docid_t id)
    {
        docid_t ret = INVALID_DOCID;
        auto ec = seek(id, ret);
        IE_NAMESPACE(common)::ThrowIfError(ec);
        return ret;
    }
    docid_t legacySeekSub(docid_t docId, docid_t subDocId,
                          docid_t subDocEnd, bool needSubMatchdata = false)
    {
        docid_t ret = INVALID_DOCID;
        auto ec = seekSub(docId, subDocId, subDocEnd, needSubMatchdata, ret);
        IE_NAMESPACE(common)::ThrowIfError(ec);
        return ret;
    }

public:
    inline docid_t getDocId() const { return _current; }
    inline docid_t getSubDocId() const { return _currentSub; }
    inline void setDocId(docid_t docid) { _current = docid; }
    inline df_t getCurrentDF() const { return getDF(GDT_CURRENT_CHAIN); }
    inline df_t getMainChainDF() const { return getDF(GDT_MAIN_CHAIN); }
    inline bool isEmpty() const { return _current == END_DOCID; }
    inline void setQueryLabel(const std::string& label) { _queryLabel = label; }
    inline const std::string& getQueryLabel() { return _queryLabel; }

    /**
     * seek next sub doc id in specified doc.
     * @param docId, specified doc to seek sub doc.
     * @param subDocId, sub docid to seek
     * @param subDocEnd, [subDocBegin, subDocEnd), for AndNotQueryExecutor.
     */
    inline docid_t seekSubDoc(docid_t docId, docid_t subDocId,
                               docid_t subDocEnd, bool needSubMatchdata)
    {
        docid_t tmpid = INVALID_DOCID;
        auto ec = seekSubDoc(docId, subDocId, subDocEnd, needSubMatchdata, tmpid);
        IE_NAMESPACE(common)::ThrowIfError(ec);
        return tmpid;
    }

private:
    virtual IE_NAMESPACE(common)::ErrorCode doSeek(docid_t id, docid_t& result) = 0;
public:
    virtual IE_NAMESPACE(common)::ErrorCode unpackMatchData(rank::TermMatchData &tmd) {
        tmd.setMatched(true);
        return IE_OK;
    }
    virtual void getMetaInfo(rank::MetaInfo *metaInfo) const {
        rank::TermMetaInfo termMetaInfo;
        termMetaInfo.setMatchDataLevel(MDL_SUB_QUERY);
        termMetaInfo.setQueryLabel(_queryLabel);
        metaInfo->pushBack(termMetaInfo);
    }

    virtual matchvalue_t getMatchValue() {
        return matchvalue_t();
    }
protected:
    docid_t _current;
    docid_t _currentSub;
    uint32_t _seekDocCount;
    bool _hasSubDocExecutor;
    std::string _queryLabel;
};

HA3_TYPEDEF_PTR(QueryExecutor);

END_HA3_NAMESPACE(search);
#endif
