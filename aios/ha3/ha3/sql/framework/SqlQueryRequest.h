#ifndef ISEARCH_SQLQUERYREQUEST_H
#define ISEARCH_SQLQUERYREQUEST_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <unordered_map>

BEGIN_HA3_NAMESPACE(sql);

class SqlQueryRequest {
public:
    SqlQueryRequest() {
    }
public:
    bool init(const std::string& queryStr);
    bool getValue(const std::string &name, std::string &value) const;
    const std::string &getValueWithDefault(const std::string &name, const std::string &value = "") const;
    const std::string &getSqlQuery() const { return _sqlStr; }
    std::unordered_map<std::string, std::string> &getKvPair() { return _kvPair; }
    const std::string &getSourceSpec();
private:
    void parseKvPair(const std::string &value);
    bool validate();
private:
    std::string _sqlStr;
    std::unordered_map<std::string, std::string> _kvPair;
private:
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(SqlQueryRequest);

END_HA3_NAMESPACE(sql);

#endif //ISEARCH_SQLQUERYREQUEST_H
