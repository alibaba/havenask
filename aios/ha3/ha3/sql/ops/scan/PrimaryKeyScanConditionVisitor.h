#ifndef ISEARCH_KEYVALUESCANCONDITIONVISITOR_H
#define ISEARCH_KEYVALUESCANCONDITIONVISITOR_H

#include <ha3/util/Log.h>
#include <ha3/sql/common/common.h>
#include <ha3/sql/ops/condition/ConditionVisitor.h>
#include <suez/turing/expression/util/TableInfo.h>

BEGIN_HA3_NAMESPACE(sql);


class PrimaryKeyScanConditionVisitor : public ConditionVisitor
{
public:
    PrimaryKeyScanConditionVisitor(const std::string& keyFieldName, const std::string& keyIndexName);
    ~PrimaryKeyScanConditionVisitor();
public:
    void visitAndCondition(AndCondition *condition) override;
    void visitOrCondition(OrCondition *condition) override;
    void visitNotCondition(NotCondition *condition) override;
    void visitLeafCondition(LeafCondition *condition) override;
    bool stealHasQuery() {
        bool tmp = _hasQuery;
        _hasQuery = false;
        return tmp;
    }
    bool needFilter() { return _needFilter; }
    std::vector<std::string> getRawKeyVec() const {
        return _rawKeyVec;
    }
protected:
    bool parseUdf(const autil_rapidjson::SimpleValue &condition);
    bool parseKey(const autil_rapidjson::SimpleValue &condition);
    bool parseIn(const autil_rapidjson::SimpleValue &condition);
    bool parseEqual(
            const autil_rapidjson::SimpleValue &left,
            const autil_rapidjson::SimpleValue &right);
    bool doParseEqual(
            const autil_rapidjson::SimpleValue &left,
            const autil_rapidjson::SimpleValue &right);
    bool tryAddKeyValue(const autil_rapidjson::SimpleValue &attr,
                        const autil_rapidjson::SimpleValue &value);
protected:
    suez::turing::TableInfoPtr _tableInfo;
    std::string _keyFieldName;
    std::string _keyIndexName;
    bool _hasQuery;
    bool _needFilter;
    std::vector<std::string> _rawKeyVec;
    std::set<std::string> _rawKeySet;
private:
    HA3_LOG_DECLARE();
};
HA3_TYPEDEF_PTR(PrimaryKeyScanConditionVisitor);

typedef PrimaryKeyScanConditionVisitor SummaryScanConditionVisitor;
typedef PrimaryKeyScanConditionVisitor KVScanConditionVisitor;
typedef PrimaryKeyScanConditionVisitor KKVScanConditionVisitor;
HA3_TYPEDEF_PTR(SummaryScanConditionVisitor);
HA3_TYPEDEF_PTR(KVScanConditionVisitor);
HA3_TYPEDEF_PTR(KKVScanConditionVisitor);

/**
class SummaryScanConditionVisitor : public PrimaryKeyScanConditionVisitor
{
public:
    SummaryScanConditionVisitor(const std::string& keyFieldName, const std::string& keyIndexName)
        : PrimaryKeyScanConditionVisitor(keyFieldName, keyIndexName)
    {}
    ~SummaryScanConditionVisitor()
    {}

private:
    HA3_LOG_DECLARE();
};
HA3_TYPEDEF_PTR(SummaryScanConditionVisitor);


class KVScanConditionVisitor : public PrimaryKeyScanConditionVisitor
{
public:
    KVScanConditionVisitor(const std::string& keyFieldName, const std::string& keyIndexName)
        : PrimaryKeyScanConditionVisitor(keyFieldName, keyIndexName)
    {}
    ~KVScanConditionVisitor()
    {}

private:
    HA3_LOG_DECLARE();
};
HA3_TYPEDEF_PTR(KVScanConditionVisitor);


class KKVScanConditionVisitor : public PrimaryKeyScanConditionVisitor
{
public:
    KKVScanConditionVisitor(const std::string& keyFieldName, const std::string& keyIndexName)
        : PrimaryKeyScanConditionVisitor(keyFieldName, keyIndexName)
    {}
    ~KKVScanConditionVisitor()
    {}

private:
    HA3_LOG_DECLARE();
};
HA3_TYPEDEF_PTR(KKVScanConditionVisitor);
*/

END_HA3_NAMESPACE(sql);

#endif //ISEARCH_KEYVALUESCANCONDITIONVISITOR_H
