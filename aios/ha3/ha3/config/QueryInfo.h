#ifndef ISEARCH_QUERYINFO_H
#define ISEARCH_QUERYINFO_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <autil/legacy/jsonizable.h>

BEGIN_HA3_NAMESPACE(config);

class QueryInfo : public autil::legacy::Jsonizable
{
public:
    QueryInfo();
    QueryInfo(const std::string &defaultIndexName,
              QueryOperator defaultOP = OP_AND, bool flag = false);
    ~QueryInfo();
public:
    const std::string& getDefaultIndexName() const {
        return _defaultIndexName;
    }
    void setDefaultIndexName(const std::string &indexName) {
        _defaultIndexName = indexName;
    }

    QueryOperator getDefaultOperator() const {
        return _defaultOP;
    }
    void setDefaultOperator(QueryOperator defaultOP) {
        _defaultOP = defaultOP;
    }
    const bool getDefaultMultiTermOptimizeFlag() const {
        return _useMultiTermOptimize;
    }
    void setMultiTermOptimizeFlag(bool flag) {
        _useMultiTermOptimize = flag;
    }

    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json);
private:
    std::string _defaultIndexName;
    QueryOperator _defaultOP;
    bool _useMultiTermOptimize;
private:
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(QueryInfo);

END_HA3_NAMESPACE(config);

#endif //ISEARCH_QUERYINFO_H
