#ifndef ISEARCH_RESTRICTPHRASEQUERYEXECUTOR_H
#define ISEARCH_RESTRICTPHRASEQUERYEXECUTOR_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/search/PhraseQueryExecutor.h>
#include <ha3/search/QueryExecutorRestrictor.h>
BEGIN_HA3_NAMESPACE(search);

class RestrictPhraseQueryExecutor: public PhraseQueryExecutor
{
public:
    RestrictPhraseQueryExecutor(QueryExecutorRestrictor *restrictor);
    ~RestrictPhraseQueryExecutor() override;
private:
    RestrictPhraseQueryExecutor(const RestrictPhraseQueryExecutor &);
    RestrictPhraseQueryExecutor& operator=(const RestrictPhraseQueryExecutor &);
public:
    const std::string getName() const override {
        return "RestrictorPhraseQueryExecutor";
    }
    IE_NAMESPACE(common)::ErrorCode doSeek(docid_t id, docid_t& result) override;
private:
    QueryExecutorRestrictor *_restrictor;
private:
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(RestrictPhraseQueryExecutor);

END_HA3_NAMESPACE(search);

#endif //ISEARCH_RESTRICTPHRASEQUERYEXECUTOR_H
