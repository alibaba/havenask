#ifndef ISEARCH_REQUESTCREATOR_H
#define ISEARCH_REQUESTCREATOR_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/queryparser/RequestParser.h>

BEGIN_HA3_NAMESPACE(qrs);

class RequestCreator
{
public:
    RequestCreator();
    ~RequestCreator();
private:
    RequestCreator(const RequestCreator &);
    RequestCreator& operator=(const RequestCreator &);
public:
    static common::RequestPtr prepareRequest(const std::string &query, 
            const std::string &defaultIndexName = "default0",
            bool multiTermFlag = false);
private:
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(RequestCreator);

END_HA3_NAMESPACE(qrs);

#endif //ISEARCH_REQUESTCREATOR_H
