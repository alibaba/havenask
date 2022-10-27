#ifndef ISEARCH_MULTITERMANDQUERYEXECUTOR_H
#define ISEARCH_MULTITERMANDQUERYEXECUTOR_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/search/AndQueryExecutor.h>

BEGIN_HA3_NAMESPACE(search);

class MultiTermAndQueryExecutor : public AndQueryExecutor
{
public:
    MultiTermAndQueryExecutor();
    ~MultiTermAndQueryExecutor();

public:
    const std::string getName() const {return "MultiTermAndQueryExecutor";}
    std::string toString() const {
        return "MultiTermAnd" + MultiQueryExecutor::toString();
    }
private:
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(MultiTermAndQueryExecutor);

END_HA3_NAMESPACE(search);

#endif //ISEARCH_MULTITERMANDQUERYEXECUTOR_H
