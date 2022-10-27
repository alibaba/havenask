#pragma once

#include <ha3/util/Log.h>
#include <ha3/sql/common/common.h>
#include <ha3/sql/ops/condition/ConditionVisitor.h>
#include <ha3/sql/attr/IndexInfo.h>
#include <khronos_table_interface/TimeRange.h>
#include <khronos_table_interface/SearchInterface.h>
#include <autil/mem_pool/Pool.h>

BEGIN_HA3_NAMESPACE(sql);

class KhronosDataConditionVisitor : public ConditionVisitor
{
public:
    KhronosDataConditionVisitor(autil::mem_pool::Pool *pool,
                                const IndexInfoMapType &indexInfos);
    ~KhronosDataConditionVisitor();
public:
    void visitAndCondition(AndCondition *condition) override;
    void visitOrCondition(OrCondition *condition) override;
    void visitNotCondition(NotCondition *condition) override;
public:
    auto &getTagKVInfos() { return _tagKVInfos; }
    auto &getTsRange() { return _tsRange; }
protected:
    bool isIndexColumn(const autil_rapidjson::SimpleValue& param);
    void parseAsItem(const autil_rapidjson::SimpleValue &leftIn,
                     const autil_rapidjson::SimpleValue &rightIn,
                     const std::string &op);
private:
    void parseTagsFromUDF(const std::string &tagk,
                          const autil_rapidjson::SimpleValue &value);

protected:
    autil::mem_pool::Pool *_pool;
    IndexInfoMapType _indexInfos;
    //tags['host'] = 'a' and tags['mem'] = 'b'
    khronos::search::SearchInterface::TagKVInfos _tagKVInfos;
    khronos::search::TimeRange _tsRange;
private:
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(KhronosDataConditionVisitor);

END_HA3_NAMESPACE(sql);
