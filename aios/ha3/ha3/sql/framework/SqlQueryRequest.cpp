#include <ha3/sql/framework/SqlQueryRequest.h>
#include <ha3/sql/common/common.h>
#include <autil/StringUtil.h>
using namespace std;
using namespace autil;
BEGIN_HA3_NAMESPACE(sql);
HA3_LOG_SETUP(sql, SqlQueryRequest);

static const string SOURCE_SPEC = "exec.source.spec";

bool SqlQueryRequest::init(const string& queryStr) {
    const vector<string> &clauses = StringUtil::split(queryStr, SQL_CLAUSE_SPLIT, true);
    if (clauses.empty()) {
        return false;
    }
    for (auto clause: clauses) {
        if (clause.find(SQL_KVPAIR_CLAUSE) == 0) {
            parseKvPair(clause.substr(strlen(SQL_KVPAIR_CLAUSE)));
        } else {
            if (clause.find(SQL_QUERY_CLAUSE) == 0) {
                _sqlStr = clause.substr(strlen(SQL_QUERY_CLAUSE));
            }else {
                _sqlStr = clause;
            }
        }
    }
    if (!validate()) {
        return false;
    }
    return true;
}

const std::string &SqlQueryRequest::getSourceSpec() {
    static const string defaultSource = SQL_SOURCE_SPEC_EMPTY;
    return getValueWithDefault(SOURCE_SPEC, defaultSource);
}

void SqlQueryRequest::parseKvPair(const string &value) {
    const vector<string> &pairs = StringUtil::split(value, ";", true);
    for (auto pair : pairs) {
        size_t pos = pair.find(':');
        if (pos != string::npos) {
            _kvPair[pair.substr(0, pos)] = pair.substr(pos + 1);
        }
    }
}

bool SqlQueryRequest::getValue(const string &name, string &value) const {
    auto iter = _kvPair.find(name);
    if (iter == _kvPair.end()) {
        return false;
    }
    value = iter->second;
    return true;
}

const string &SqlQueryRequest::getValueWithDefault(const string &name, const string &value) const {
    auto iter = _kvPair.find(name);
    if (iter == _kvPair.end()) {
        return value;
    }
    return iter->second;
}

bool SqlQueryRequest::validate() {
    return !_sqlStr.empty();
}

END_HA3_NAMESPACE(sql);
