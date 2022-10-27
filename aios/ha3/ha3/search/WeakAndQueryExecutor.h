#ifndef ISEARCH_WEAKANDQUERYEXECUTOR_H
#define ISEARCH_WEAKANDQUERYEXECUTOR_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/search/MultiQueryExecutor.h>
#include <ha3/search/QueryExecutorHeap.h>
#include <ha3/rank/MatchData.h>
#include <string>
#include <vector>
#include <assert.h>

BEGIN_HA3_NAMESPACE(search);

class WeakAndQueryExecutor : public MultiQueryExecutor
{
public:
    WeakAndQueryExecutor(uint32_t minShouldMatch);
    ~WeakAndQueryExecutor();
public:
    const std::string getName() const override {
        return "WeakAndQueryExecutor";
    }
    IE_NAMESPACE(common)::ErrorCode doSeek(docid_t id, docid_t& result) override;
    IE_NAMESPACE(common)::ErrorCode seekSubDoc(docid_t docId, docid_t subDocId,
                       docid_t subDocEnd, bool needSubMatchdata, docid_t& result) override;
    df_t getDF(GetDFType type) const override;
    bool isMainDocHit(docid_t docId) const override;
    void addQueryExecutors(const std::vector<QueryExecutor*> &queryExecutors) override;
    void reset() override;
    std::string toString() const override;
private:
    void doNthElement(std::vector<QueryExecutorEntry>& heap);
private:
    std::vector<QueryExecutorEntry> _sortNHeap;
    std::vector<QueryExecutorEntry> _sortNHeapSub;
    uint32_t _count;
    uint32_t _minShouldMatch;
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(WeakAndQueryExecutor);

END_HA3_NAMESPACE(search);

#endif //ISEARCH_WEAKANDQUERYEXECUTOR_H
