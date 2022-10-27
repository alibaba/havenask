#pragma once

#include <ha3/util/Log.h>
#include <ha3/sql/common/common.h>
#include <ha3/sql/ops/condition/ExprVisitor.h>

BEGIN_HA3_NAMESPACE(sql);

class ExprGenerateVisitor: public ExprVisitor
{
public:
    ExprGenerateVisitor();
    ~ExprGenerateVisitor();
public:
    auto &getExpr() { return _expr; }
    auto &getRenameMap() { return _renameMap; }
protected:
    void visitCastUdf(const autil_rapidjson::SimpleValue &value) override;
    void visitMultiCastUdf(const autil_rapidjson::SimpleValue &value) override;
    void visitNormalUdf(const autil_rapidjson::SimpleValue &value) override;
    void visitInOp(const autil_rapidjson::SimpleValue &value) override;
    void visitNotInOp(const autil_rapidjson::SimpleValue &value) override;
    void visitCaseOp(const autil_rapidjson::SimpleValue &value) override;
    void visitOtherOp(const autil_rapidjson::SimpleValue &value) override;
    void visitInt64(const autil_rapidjson::SimpleValue &value) override;
    void visitDouble(const autil_rapidjson::SimpleValue &value) override;
    void visitBool(const autil_rapidjson::SimpleValue &value) override;    
    void visitItemOp(const autil_rapidjson::SimpleValue &value) override;
    void visitColumn(const autil_rapidjson::SimpleValue &value) override;
    void visitString(const autil_rapidjson::SimpleValue &value) override;
private:
    void parseInOrNotIn(const autil_rapidjson::SimpleValue &value, const std::string &op);
private:
    std::string _expr;
    std::map<std::string, std::string> _renameMap;
private:
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(ExprGenerateVisitor);

END_HA3_NAMESPACE(sql);
