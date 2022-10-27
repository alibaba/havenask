#ifndef ISEARCH_AGGFUNDESCRIPTION_H
#define ISEARCH_AGGFUNDESCRIPTION_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/util/Serialize.h>
#include <suez/turing/expression/syntax/SyntaxExpr.h>

BEGIN_HA3_NAMESPACE(common);

class AggFunDescription
{
public:
    AggFunDescription();
    AggFunDescription(const std::string &functionName,
                      suez::turing::SyntaxExpr *syntaxExpr);
    ~AggFunDescription();
public:
    void setFunctionName(const std::string&);
    const std::string& getFunctionName() const;

    void setSyntaxExpr(suez::turing::SyntaxExpr*);
    suez::turing::SyntaxExpr* getSyntaxExpr() const;

    HA3_SERIALIZE_DECLARE();

    std::string toString() const;
private:
    std::string _functionName;
    suez::turing::SyntaxExpr* _syntaxExpr;
private:
    HA3_LOG_DECLARE();
};

END_HA3_NAMESPACE(common);

#endif //ISEARCH_AGGFUNDESCRIPTION_H
