#pragma once

#include <ha3/util/Log.h>
#include <ha3/sql/common/common.h>
#include <ha3/sql/ops/condition/ExprVisitor.h>
#include <ha3/sql/attr/IndexInfo.h>
#include <autil/mem_pool/Pool.h>


BEGIN_HA3_NAMESPACE(sql);

class OutputFieldsVisitor: public ExprVisitor
{
public:
    OutputFieldsVisitor();
    ~OutputFieldsVisitor();
public:
    auto &getUsedFieldsItemSet() { return _usedFieldsItemSet; }
    auto &getUsedFieldsColumnSet() { return _usedFieldsColumnSet; }
protected:
    void visitCastUdf(const autil_rapidjson::SimpleValue &value) override;
    void visitMultiCastUdf(const autil_rapidjson::SimpleValue &value) override;
    void visitNormalUdf(const autil_rapidjson::SimpleValue &value) override;
    void visitOtherOp(const autil_rapidjson::SimpleValue &value) override;
    void visitItemOp(const autil_rapidjson::SimpleValue &value) override;
    void visitColumn(const autil_rapidjson::SimpleValue &value) override;
    void visitInt64(const autil_rapidjson::SimpleValue &value) override {}
    void visitDouble(const autil_rapidjson::SimpleValue &value) override {}
    void visitString(const autil_rapidjson::SimpleValue &value) override {}
private:
    void visitParams(const autil_rapidjson::SimpleValue &value);
private:
    // select tags['host'], tags['mem']
    std::set<std::pair<std::string, std::string> > _usedFieldsItemSet;
    // select a, b
    std::set<std::string> _usedFieldsColumnSet;
private:
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(OutputFieldsVisitor);

END_HA3_NAMESPACE(sql);
