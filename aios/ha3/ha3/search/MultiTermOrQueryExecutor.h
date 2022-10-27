#ifndef ISEARCH_MULTITERMORQUERYEXECUTOR_H
#define ISEARCH_MULTITERMORQUERYEXECUTOR_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/search/OrQueryExecutor.h>

BEGIN_HA3_NAMESPACE(search);

class MultiTermOrQueryExecutor : public OrQueryExecutor
{
public:
    MultiTermOrQueryExecutor();
    ~MultiTermOrQueryExecutor();

public:
    const std::string getName() const override {
        return "MultiTermOrQueryExecutor";
    }
    std::string toString() const override;
private:
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(MultiTermOrQueryExecutor);

END_HA3_NAMESPACE(search);

#endif //ISEARCH_MULTITERMORQUERYEXECUTOR_H
