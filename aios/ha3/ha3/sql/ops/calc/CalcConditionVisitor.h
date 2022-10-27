#ifndef ISEARCH_HA3CALCCONDITIONVISITOR_H
#define ISEARCH_HA3CALCCONDITIONVISITOR_H

#include <ha3/sql/common/common.h>
#include <ha3/sql/ops/condition/ConditionVisitor.h>
#include <suez/turing/expression/framework/MatchDocsExpressionCreator.h>

BEGIN_HA3_NAMESPACE(sql);

class CalcConditionVisitor : public ConditionVisitor
{
public:
    CalcConditionVisitor(suez::turing::MatchDocsExpressionCreator *attrExprCreator,
                         autil::mem_pool::Pool *pool);
    ~CalcConditionVisitor();
public:
    void visitAndCondition(AndCondition *condition) override;
    void visitOrCondition(OrCondition *condition) override;
    void visitNotCondition(NotCondition *condition) override;
    void visitLeafCondition(LeafCondition *condition) override;
public:
    suez::turing::AttributeExpression* stealAttributeExpression();
    const suez::turing::AttributeExpression* getAttributeExpresson() const;
private:
    suez::turing::AttributeExpression* _attrExpr;
    suez::turing::MatchDocsExpressionCreator *_attrExprCreator;
    autil::mem_pool::Pool *_pool;
};
    
HA3_TYPEDEF_PTR(CalcConditionVisitor);

END_HA3_NAMESPACE(sql);

#endif //ISEARCH_HA3SCANCONDITIONVISITOR_H
