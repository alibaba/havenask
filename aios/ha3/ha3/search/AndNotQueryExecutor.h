#ifndef ISEARCH_ANDNOTQUERYEXECUTOR_H
#define ISEARCH_ANDNOTQUERYEXECUTOR_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/search/OrQueryExecutor.h>
#include <ha3/rank/MatchData.h>
#include <string>
#include <vector>
#include <list>
#include <assert.h>

BEGIN_HA3_NAMESPACE(search);

class AndNotQueryExecutor : public MultiQueryExecutor
{
public:
    AndNotQueryExecutor();
    ~AndNotQueryExecutor();
public:
    const std::string getName() const override {
        return "AndNotQueryExecutor";
    }
    df_t getDF(GetDFType type) const override;
    void addQueryExecutors(const std::vector<QueryExecutor*> &queryExecutors) override {
        assert(false);
    }
    IE_NAMESPACE(common)::ErrorCode seekSubDoc(docid_t docId, docid_t subDocId,
                       docid_t subDocEnd, bool needSubMatchdata, docid_t& result) override;
    bool isMainDocHit(docid_t docId) const override;
    void setCurrSub(docid_t docid) override;
    uint32_t getSeekDocCount() override {
        return MultiQueryExecutor::getSeekDocCount() + _testDocCount;
    }
public:
    void addQueryExecutors(QueryExecutor* leftExecutor,
                           QueryExecutor* rightExecutor);
public:
    std::string toString() const override;
private:
    IE_NAMESPACE(common)::ErrorCode doSeek(docid_t docId, docid_t& result) override;
private:
    QueryExecutor *_leftQueryExecutor;
    QueryExecutor *_rightQueryExecutor;
    IE_NAMESPACE(index)::DocValueFilter* _rightFilter;
    int64_t _testDocCount;
    bool _rightExecutorHasSub;
private:
    HA3_LOG_DECLARE();
};

END_HA3_NAMESPACE(search);

#endif //ISEARCH_ANDNOTQUERYEXECUTOR_H
