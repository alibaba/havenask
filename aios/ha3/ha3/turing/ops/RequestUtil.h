#ifndef ISEARCH_REQUESTUTIL_H
#define ISEARCH_REQUESTUTIL_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/common/Request.h>

BEGIN_HA3_NAMESPACE(turing);

class RequestUtil
{
public:
    RequestUtil();
    ~RequestUtil();
private:
    RequestUtil(const RequestUtil &);
    RequestUtil& operator=(const RequestUtil &);
public:
    static bool defaultSorterBeginRequest(const common::RequestPtr &request);
    static bool needScoreInSort(const HA3_NS(common)::RequestPtr &request);
    static bool validateSortInfo(const HA3_NS(common)::RequestPtr &request);

private:
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(RequestUtil);

END_HA3_NAMESPACE(ops);

#endif //ISEARCH_REQUESTUTIL_H
