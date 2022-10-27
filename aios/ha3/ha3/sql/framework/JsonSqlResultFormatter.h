#ifndef ISEARCH_JSON_SQLRESULT_FORMATTER_H
#define ISEARCH_JSON_SQLRESULT_FORMATTER_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/common/ResultFormatter.h>
#include <sstream>

BEGIN_HA3_NAMESPACE(common);

class JsonSqlResultFormatter : public ResultFormatter
{
public:
    JsonSqlResultFormatter();
    ~JsonSqlResultFormatter();
public:
   static void format(QrsSessionSqlResult& sqlResult);
private:
    HA3_LOG_DECLARE();
};

END_HA3_NAMESPACE(common);


#endif // ISEARCH_JSON_SQLRESULT_FORMATTER_H
