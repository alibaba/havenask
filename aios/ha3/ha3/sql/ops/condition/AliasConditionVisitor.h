#ifndef ISEARCH_HA3ALIASCONDITIONVISITOR_H
#define ISEARCH_HA3ALIASCONDITIONVISITOR_H

#include <ha3/sql/common/common.h>
#include <ha3/sql/ops/condition/ConditionVisitor.h>

BEGIN_HA3_NAMESPACE(sql);

class AliasConditionVisitor : public ConditionVisitor
{
public:
    AliasConditionVisitor();
    ~AliasConditionVisitor();
public:
    void visitAndCondition(AndCondition *condition) override;
    void visitOrCondition(OrCondition *condition) override;
    void visitNotCondition(NotCondition *condition) override;
    void visitLeafCondition(LeafCondition *condition) override;
public:
    std::map<std::string, std::string> &getAliasMap() {
        return _aliasMap;
    }
private:
    std::map<std::string, std::string> _aliasMap;

};
    
HA3_TYPEDEF_PTR(AliasConditionVisitor);

END_HA3_NAMESPACE(sql);

#endif //ISEARCH_HA3SCANCONDITIONVISITOR_H
