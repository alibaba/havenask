#include <ha3/sql/ops/condition/Condition.h>
#include <ha3/sql/ops/condition/ConditionVisitor.h>
#include <suez/turing/expression/syntax/SyntaxParser.h>
#include <autil/legacy/RapidJsonHelper.h>

using namespace std;
using namespace autil_rapidjson;

BEGIN_HA3_NAMESPACE(sql);

Condition::Condition()
    : _allocator(NULL)
    , _topDoc(NULL)
 
{}

Condition::~Condition() { 
    DELETE_AND_SET_NULL(_topDoc);
    DELETE_AND_SET_NULL(_allocator);
}

void Condition::setTopJsonDoc(autil_rapidjson::AutilPoolAllocator *allocator,
                                  autil_rapidjson::SimpleDocument *topDoc) {
    if (_topDoc) {
        DELETE_AND_SET_NULL(_topDoc);
    }
    _topDoc = topDoc;
    if (_allocator) {
        DELETE_AND_SET_NULL(_allocator);
    }
    _allocator = allocator;
}

LeafCondition::LeafCondition(const autil_rapidjson::SimpleValue& simpleVal)
    : _condition(simpleVal)
{
}

Condition* Condition::createCondition(ConditionType type) {
    Condition *scanCond = NULL;
    switch (type) {
    case AND_CONDITION:
        scanCond = new AndCondition();
        break;
    case OR_CONDITION:
        scanCond = new OrCondition();
        break;
    case NOT_CONDITION:
        scanCond = new NotCondition();
        break;
    default:
        assert(false);
    }
    return scanCond;
}

Condition* Condition::createLeafCondition(const autil_rapidjson::SimpleValue &simpleVal) {
    return new LeafCondition(simpleVal);
}

void AndCondition::accept(ConditionVisitor *visitor) {
    visitor->visitAndCondition(this);
}

void OrCondition::accept(ConditionVisitor *visitor) {
    visitor->visitOrCondition(this);
}

void NotCondition::accept(ConditionVisitor *visitor) {
    visitor->visitNotCondition(this);
}

void LeafCondition::accept(ConditionVisitor *visitor) {
    visitor->visitLeafCondition(this);
}

string AndCondition::toString() {
    if (_children.size() == 0) {
        return "";
    } else if (_children.size() == 1) {
        return _children[0]->toString();
    }
    string conditionStr;
    for (size_t i = 0; i < _children.size() - 1; i++) {
        conditionStr += _children[i]->toString() + " AND ";
    }
    conditionStr += _children[_children.size() - 1]->toString();
    conditionStr = "(" + conditionStr + ")";
    return conditionStr;
}

string OrCondition::toString() {
    if (_children.size() == 0) {
        return "";
    } else if (_children.size() == 1) {
        return _children[0]->toString();
    }
    string conditionStr;
    for (size_t i = 0; i < _children.size() - 1; i++) {
        conditionStr += _children[i]->toString() + " OR ";
    }
    conditionStr += _children[_children.size() - 1]->toString();
    conditionStr = "(" + conditionStr + ")";
    return conditionStr;
}

string NotCondition::toString() {
    if (_children.size() == 0) {
        return "";
    }
    string conditionStr = _children[0]->toString();
    conditionStr = "NOT " + conditionStr;
    return conditionStr;
}

string LeafCondition::toString() {
    return RapidJsonHelper::SimpleValue2Str(_condition);
}


END_HA3_NAMESPACE(sql);
