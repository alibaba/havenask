#ifndef ISEARCH_CONDITIONVISITOR_H
#define ISEARCH_CONDITIONVISITOR_H

#include <ha3/common.h>
#include <ha3/util/Log.h>
#include <ha3/sql/ops/condition/Condition.h>

BEGIN_HA3_NAMESPACE(sql);

class ConditionVisitor
{
public:
    ConditionVisitor();
    virtual ~ConditionVisitor();
public:
    virtual void visitAndCondition(AndCondition *condition) = 0;
    virtual void visitOrCondition(OrCondition *condition) = 0;
    virtual void visitNotCondition(NotCondition *condition) = 0;
    virtual void visitLeafCondition(LeafCondition *condition) = 0;
public:
    bool isError() const {
        return _hasError;
    }
    const std::string &errorInfo() const {
        return _errorInfo;
    }
    void setErrorInfo(const char* fmt, ...)
    {
        const size_t maxMessageLength = 1024;
        char buffer[maxMessageLength];
        va_list ap;
        va_start(ap, fmt);
        vsnprintf(buffer, maxMessageLength, fmt, ap);
        va_end(ap);

        setErrorInfo(std::string(buffer));
    }
    void setErrorInfo(const std::string &errorInfo) {
        assert(!_hasError);
        _hasError = true;
        _errorInfo = errorInfo;
    }
protected:
    bool _hasError;
    std::string _errorInfo;

    HA3_LOG_DECLARE();
};


#define CHECK_VISITOR_ERROR(visitor)                                    \
    do {                                                                \
        if (unlikely(visitor.isError())) {                              \
            SQL_LOG(ERROR, "visitor has error: %s",                     \
                    visitor.errorInfo().c_str());                       \
            return false;                                               \
        }                                                               \
    } while (false)

END_HA3_NAMESPACE(sql);

#endif //ISEARCH_CONDITIONVISITOR_H
