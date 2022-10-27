#ifndef ISEARCH_QUERYEXECUTORMOCK_H
#define ISEARCH_QUERYEXECUTORMOCK_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/search/QueryExecutor.h>

BEGIN_HA3_NAMESPACE(search);

class QueryExecutorMock : public QueryExecutor
{
public:
    QueryExecutorMock(const std::string &docIdStr);
    QueryExecutorMock(const std::vector<docid_t> &docIds);
    ~QueryExecutorMock();
private:
    QueryExecutorMock(const QueryExecutorMock &);
    QueryExecutorMock& operator=(const QueryExecutorMock &);
public:
    const std::string getName() const override { return std::string("QueryExecutorMock"); }
    void reset() override;
    df_t getDF(GetDFType type) const override { return _docIds.size(); }
    docid_t seekSubDoc(
            docid_t docId, docid_t subDocId, docid_t subDocEnd, bool needSubMatchdata);
    IE_NAMESPACE(common)::ErrorCode seekSubDoc(
        docid_t docId, docid_t subDocId,
        docid_t subDocEnd, bool needSubMatchdata, docid_t& result) override
    {
        result = seekSubDoc(docId, subDocId, subDocEnd, needSubMatchdata);
        return IE_NAMESPACE(common)::ErrorCode::OK;
    }
    
    /* override */ bool isMainDocHit(docid_t docId) const override {return true;}; 


public:
    /* override */ std::string toString() const override;
private:
    IE_NAMESPACE(common)::ErrorCode doSeek(docid_t id, docid_t& result) override;

private:
    size_t  _cursor;
    std::vector<docid_t> _docIds;
private:
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(QueryExecutorMock);

END_HA3_NAMESPACE(search);

#endif //ISEARCH_QUERYEXECUTORMOCK_H
