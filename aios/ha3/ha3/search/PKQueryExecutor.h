#ifndef ISEARCH_PKQUERYEXECUTOR_H
#define ISEARCH_PKQUERYEXECUTOR_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/search/QueryExecutor.h>
#include <indexlib/partition/index_partition_reader.h>

BEGIN_HA3_NAMESPACE(search);

class PKQueryExecutor : public QueryExecutor
{
public:
    PKQueryExecutor(QueryExecutor* queryExecutor,
                    docid_t docId);
    ~PKQueryExecutor();
public:
    const std::string getName() const override {
        return "PKQueryExecutor";
    }
    df_t getDF(GetDFType type) const override {
        return 1;
    }
    void reset() override;
    IE_NAMESPACE(common)::ErrorCode seekSubDoc(docid_t docId, docid_t subDocId,
                       docid_t subDocEnd, bool needSubMatchdata, docid_t& result) override;
    bool isMainDocHit(docid_t docId) const override;
public:
    std::string toString() const override;
private:
    IE_NAMESPACE(common)::ErrorCode doSeek(docid_t id, docid_t &result) override;
private:
    QueryExecutor *_queryExecutor;
    docid_t _docId;
    int32_t _count;
private:
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(PKQueryExecutor);

END_HA3_NAMESPACE(search);

#endif //ISEARCH_PKQUERYEXECUTOR_H
