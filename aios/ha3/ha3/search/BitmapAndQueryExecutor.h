#ifndef ISEARCH_BITMAPANDQUERYEXECUTOR_H
#define ISEARCH_BITMAPANDQUERYEXECUTOR_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/search/MultiQueryExecutor.h>
#include <ha3/search/BitmapTermQueryExecutor.h>

BEGIN_HA3_NAMESPACE(search);

class BitmapAndQueryExecutor : public MultiQueryExecutor
{
public:
    BitmapAndQueryExecutor(autil::mem_pool::Pool *pool);
    ~BitmapAndQueryExecutor();
private:
    BitmapAndQueryExecutor(const BitmapAndQueryExecutor &);
    BitmapAndQueryExecutor& operator=(const BitmapAndQueryExecutor &);
public:
    void addQueryExecutors(const std::vector<QueryExecutor *> &queryExecutors) override;
    const std::string getName() const override {
        return "BitmapAndQueryExecutor";
    }
    df_t getDF(GetDFType type) const override;
    bool isMainDocHit(docid_t docId) const override;

    IE_NAMESPACE(common)::ErrorCode seekSubDoc(docid_t docId, docid_t subDocId,
                       docid_t subDocEnd, bool needSubMatchdata, docid_t& result) override;    
public:
    std::string toString() const override;
private:
    IE_NAMESPACE(common)::ErrorCode doSeek(docid_t docId, docid_t& result) override;

private:
    autil::mem_pool::Pool *_pool;
    QueryExecutor *_seekQueryExecutor;
    std::vector<BitmapTermQueryExecutor*> _bitmapTermExecutors;
    BitmapTermQueryExecutor** _firstExecutor;
private:
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(BitmapAndQueryExecutor);

END_HA3_NAMESPACE(search);

#endif //ISEARCH_BITMAPANDQUERYEXECUTOR_H
