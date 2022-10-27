#ifndef ISEARCH_CONDITION_H
#define ISEARCH_CONDITION_H


#include <ha3/sql/common/common.h>
#include <suez/turing/expression/syntax/SyntaxExpr.h>
#include <autil/legacy/RapidJsonCommon.h>

BEGIN_HA3_NAMESPACE(sql);

class ConditionVisitor;
class Condition;
HA3_TYPEDEF_PTR(Condition);

enum ConditionType {
    AND_CONDITION = 0,
    OR_CONDITION = 1,
    NOT_CONDITION = 2,
    LEAF_CONDITION = 3,
};

class Condition
{
public:
    typedef std::vector<ConditionPtr> ConditionVector;

public:
    Condition();
    virtual ~Condition();
public:
    virtual ConditionType getType() const = 0;
    virtual void accept(ConditionVisitor *visitor) = 0;
    virtual std::string toString() = 0;
public:
    void addCondition(const ConditionPtr &conditionPtr) {
        _children.push_back(conditionPtr);
    }
    const std::vector<ConditionPtr>& getChildCondition() const {
        return _children;
    }
    const std::vector<ConditionPtr>& getChildCondition() {
        return _children;
    }
    void setTopJsonDoc(autil_rapidjson::AutilPoolAllocator *allocator,
                    autil_rapidjson::SimpleDocument *topDoc);
public:
    static Condition* createCondition(ConditionType type);
    static Condition* createLeafCondition(const autil_rapidjson::SimpleValue &simpleVal);

protected:
    ConditionVector _children;
    autil_rapidjson::AutilPoolAllocator *_allocator;
    autil_rapidjson::SimpleDocument *_topDoc;

};


class AndCondition : public Condition {
public:
    void accept(ConditionVisitor *visitor) override;
    std::string toString() override;
    ConditionType getType() const override {
        return AND_CONDITION;
    }
};

class OrCondition : public Condition {
public:
    void accept(ConditionVisitor *visitor) override;
    std::string toString() override;
    ConditionType getType() const override {
        return OR_CONDITION;
    }
};

class NotCondition : public Condition {
public:
    void accept(ConditionVisitor *visitor) override;
    std::string toString() override;
    ConditionType getType() const override {
        return NOT_CONDITION;
    }
};

class LeafCondition : public Condition {
public:
    LeafCondition(const autil_rapidjson::SimpleValue& simpleVal);
public:
    void accept(ConditionVisitor *visitor) override;
    std::string toString() override;
    ConditionType getType() const override {
        return LEAF_CONDITION;
    }
public:
    const autil_rapidjson::SimpleValue& getCondition() {
        return _condition;
    }
private:
    const autil_rapidjson::SimpleValue& _condition;
};
    
END_HA3_NAMESPACE(sql);

#endif //ISEARCH_SCANCONDITION_H
