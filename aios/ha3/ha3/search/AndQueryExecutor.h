#ifndef ISEARCH_ANDQUERYEXECUTOR_H
#define ISEARCH_ANDQUERYEXECUTOR_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/search/MultiQueryExecutor.h>

BEGIN_HA3_NAMESPACE(search);

class AndQueryExecutor : public MultiQueryExecutor
{
public:
    AndQueryExecutor();
    virtual ~AndQueryExecutor();

public:
    const std::string getName() const override;
    df_t getDF(GetDFType type) const override;
    IE_NAMESPACE(common)::ErrorCode doSeek(docid_t id, docid_t& result) override;
    IE_NAMESPACE(common)::ErrorCode seekSubDoc(docid_t docId, docid_t subDocId,
                       docid_t subDocEnd, bool needSubMatchdata, docid_t& result) override;
    bool isMainDocHit(docid_t docId) const override;
    void addQueryExecutors(const std::vector<QueryExecutor*> &queryExecutor) override;
public:
    std::string toString() const override;
    uint32_t getSeekDocCount() override {
        return MultiQueryExecutor::getSeekDocCount() + _testDocCount;
    }
private:
    bool testCurrentDoc(docid_t docid);

protected:
    QueryExecutorVector _sortedQueryExecutors;
    std::vector<IE_NAMESPACE(index)::DocValueFilter*> _filters;
    QueryExecutor **_firstExecutor;
    int64_t _testDocCount;
private:
    HA3_LOG_DECLARE();
};

inline bool AndQueryExecutor::testCurrentDoc(docid_t docid) {
    size_t filterSize = _filters.size();
    for (size_t i = 0; i < filterSize; i++) {
        _testDocCount++;
        if (!_filters[i]->Test(docid)) {
            return false;
        }
    }
    return true;
}

END_HA3_NAMESPACE(search);

#endif //ISEARCH_ANDQUERYEXECUTOR_H
