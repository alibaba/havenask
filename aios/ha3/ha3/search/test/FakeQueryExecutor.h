#ifndef ISEARCH_FAKEQUERYEXECUTOR_H
#define ISEARCH_FAKEQUERYEXECUTOR_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/search/QueryExecutor.h>

BEGIN_HA3_NAMESPACE(search);

class FakeQueryExecutor : public QueryExecutor
{
public:
    FakeQueryExecutor();
    ~FakeQueryExecutor();
public:
    /* override */ const std::string getName() const;

    /* override */ docid_t getDocId();

    /* override */ docid_t seek(docid_t id);
    /* override */ void reset() {}
    /* override */ df_t getDF(GetDFType type) const { return 0; }
public:
    /* override */ std::string toString() const;
private:
    HA3_LOG_DECLARE();
};

END_HA3_NAMESPACE(search);

#endif //ISEARCH_FAKEQUERYEXECUTOR_H
