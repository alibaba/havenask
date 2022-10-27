#pragma once

#include <ha3/util/Log.h>
#include <ha3/sql/common/common.h>
#include <ha3/sql/ops/condition/ConditionVisitor.h>
#include <ha3/sql/attr/IndexInfo.h>
#include <khronos_table_interface/TimeRange.h>
#include <khronos_table_interface/SearchInterface.h>

BEGIN_HA3_NAMESPACE(sql);

class KhronosMetaConditionVisitor : public ConditionVisitor
{
public:
    KhronosMetaConditionVisitor(const IndexInfoMapType &indexInfos);
    ~KhronosMetaConditionVisitor();
public:
    void visitAndCondition(AndCondition *condition) override;
    void visitOrCondition(OrCondition *condition) override;
    void visitNotCondition(NotCondition *condition) override;
    void visitLeafCondition(LeafCondition *condition) override;
public:
    auto &getMetricVec() { return _metricVec; }
    auto &getTagkVec() { return _tagkVec; }
    auto &getTagvVec() { return _tagvVec; }
    auto &getTsRange() { return _tsRange; }
private:
    bool isIndexColumn(const autil_rapidjson::SimpleValue& param);
    void parseLeaf(const autil_rapidjson::SimpleValue &condition);
    void handleParams(const autil_rapidjson::SimpleValue &param,
                      std::string &op,
                      const autil_rapidjson::SimpleValue **left,
                      const autil_rapidjson::SimpleValue **right);
    void parseAsMetric(const std::string &op,
                       const std::string &rawOp,
                       const autil_rapidjson::SimpleValue &right);
    void parseMetricFromUDFWithEqualOp(const autil_rapidjson::SimpleValue &value);
    void parseTagkTagvFromString(const std::string &op,
                                 const std::string &rawOp,
                                 const std::string &leftValue,
                                 const std::string &rightValue);
private:
    IndexInfoMapType _indexInfos;
    // assert(size <= 1)
    std::vector<khronos::search::SearchInterface::MetricMatchInfo> _metricVec;
    // where metric = 'sys'
    // where metric = 'sys*'; where metric LIKE 'sys*'
    // where metric=iwildcard('*sys*')
    std::vector<std::string> _tagkVec; // where tagk = 'host'
    std::vector<std::string> _tagvVec; // where tagv = '1.1*'
    khronos::search::TimeRange _tsRange;
private:
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(KhronosMetaConditionVisitor);

END_HA3_NAMESPACE(sql);
