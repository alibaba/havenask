#pragma once

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <autil/legacy/RapidJsonCommon.h>

BEGIN_HA3_NAMESPACE(sql);

class ExprVisitor
{
public:
    ExprVisitor();
    virtual ~ExprVisitor();
    ExprVisitor(const ExprVisitor &) = delete;
    ExprVisitor& operator=(const ExprVisitor &) = delete;
public:
    void visit(const autil_rapidjson::SimpleValue &value);
    bool isError() const {
        return _hasError;
    }
    const std::string &errorInfo() const {
        return _errorInfo;
    }
    void setErrorInfo(const char* fmt, ...);
protected:
    virtual void visitCastUdf(const autil_rapidjson::SimpleValue &value);
    virtual void visitMultiCastUdf(const autil_rapidjson::SimpleValue &value);
    virtual void visitNormalUdf(const autil_rapidjson::SimpleValue &value);
    virtual void visitInOp(const autil_rapidjson::SimpleValue &value);
    virtual void visitNotInOp(const autil_rapidjson::SimpleValue &value);
    virtual void visitCaseOp(const autil_rapidjson::SimpleValue &value);
    virtual void visitOtherOp(const autil_rapidjson::SimpleValue &value);
    virtual void visitInt64(const autil_rapidjson::SimpleValue &value);
    virtual void visitDouble(const autil_rapidjson::SimpleValue &value);
    virtual void visitBool(const autil_rapidjson::SimpleValue &value);    
    virtual void visitItemOp(const autil_rapidjson::SimpleValue &value);
    virtual void visitColumn(const autil_rapidjson::SimpleValue &value);
    virtual void visitString(const autil_rapidjson::SimpleValue &value);
private:
    bool _hasError;
    std::string _errorInfo;
private:
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(ExprVisitor);
END_HA3_NAMESPACE(sql);
