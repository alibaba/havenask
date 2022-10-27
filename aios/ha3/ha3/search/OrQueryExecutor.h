#ifndef ISEARCH_ORQUERYEXECUTOR_H
#define ISEARCH_ORQUERYEXECUTOR_H

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

class OrQueryExecutor : public MultiQueryExecutor
{
public:
    OrQueryExecutor();
    virtual ~OrQueryExecutor();

public:
    const std::string getName() const override {
        return "OrQueryExecutor";
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
    std::vector<QueryExecutorEntry> _qeMinHeap;
    uint32_t _count;
private:
    HA3_LOG_DECLARE();
};

END_HA3_NAMESPACE(search);

#endif //ISEARCH_ORQUERYEXECUTOR_H
