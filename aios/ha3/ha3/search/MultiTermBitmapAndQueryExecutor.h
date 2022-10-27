#ifndef ISEARCH_MULTITERMBITMAPANDQUERYEXECUTOR_H
#define ISEARCH_MULTITERMBITMAPANDQUERYEXECUTOR_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/search/BitmapAndQueryExecutor.h>

BEGIN_HA3_NAMESPACE(search);

class MultiTermBitmapAndQueryExecutor : public BitmapAndQueryExecutor
{
public:
    MultiTermBitmapAndQueryExecutor(autil::mem_pool::Pool *pool):BitmapAndQueryExecutor(pool) {}
    ~MultiTermBitmapAndQueryExecutor();

public:
    const std::string getName() const {return "MultiTermBitmapAndQueryExecutor";}
    std::string toString() const {
        return "MultiTerm" + BitmapAndQueryExecutor::toString();
    }
private:
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(MultiTermBitmapAndQueryExecutor);

END_HA3_NAMESPACE(search);

#endif //ISEARCH_MULTITERMBITMAPANDQUERYEXECUTOR_H
