#include <ha3/sql/ops/condition/ConditionParser.h>
#include <ha3/sql/common/common.h>
#include <autil/legacy/RapidJsonCommon.h>
#include <autil/legacy/RapidJsonHelper.h>

using namespace std;
using namespace navi;
using namespace autil_rapidjson;

BEGIN_HA3_NAMESPACE(sql);
HA3_LOG_SETUP(sql, ConditionParser);
ConditionParser::ConditionParser(autil::mem_pool::Pool *pool)
    : _pool(pool) {
}

ConditionParser::~ConditionParser() {
}

bool ConditionParser::parseCondition(const std::string &jsonStr, ConditionPtr &condition) {
    if (jsonStr.empty()) {
        return true;
    }
    AutilPoolAllocator *allocator = new AutilPoolAllocator(_pool);
    SimpleDocument *simpleDoc = new SimpleDocument(allocator);
    simpleDoc->Parse(jsonStr.c_str());
    if (simpleDoc->HasParseError()) {
        DELETE_AND_SET_NULL(simpleDoc);
        DELETE_AND_SET_NULL(allocator);
        SQL_LOG(WARN, "parse simple document failed, [%s]", jsonStr.c_str());
        return false;
    }
    ConditionPtr tmpCondition;
    if (!buildCondition(*simpleDoc, tmpCondition) || !validateCondition(tmpCondition)) {
        DELETE_AND_SET_NULL(simpleDoc);
        DELETE_AND_SET_NULL(allocator);
        SQL_LOG(WARN, "build & valid condition failed.");
        return false;
    }
    condition = tmpCondition;
    condition->setTopJsonDoc(allocator, simpleDoc);
    return true;
}

bool ConditionParser::buildCondition(const SimpleValue &simpleVal,
        ConditionPtr &condition)
{
    if (!simpleVal.IsObject() || !simpleVal.HasMember(SQL_CONDITION_OPERATOR)
        || !simpleVal.HasMember(SQL_CONDITION_PARAMETER))
    {
        condition.reset(Condition::createLeafCondition(simpleVal));
        return condition != nullptr;
    }
    string ops(simpleVal[SQL_CONDITION_OPERATOR].GetString());
    if (ops == SQL_AND_OP) {
        condition.reset(Condition::createCondition(AND_CONDITION));
    } else if (ops == SQL_OR_OP) {
        condition.reset(Condition::createCondition(OR_CONDITION));
    } else if (ops == SQL_NOT_OP) {
        condition.reset(Condition::createCondition(NOT_CONDITION));
    } else {
        condition.reset(Condition::createLeafCondition(simpleVal));
        return condition != nullptr;
    }
    const SimpleValue &opArray = simpleVal[SQL_CONDITION_PARAMETER];
    if (!opArray.IsArray()) {
        SQL_LOG(WARN, "cond param is not array");
        return false;
    }
    for (SizeType i = 0; i < opArray.Size(); i++) {
        const SimpleValue &param = opArray[i];
        if (param.IsObject()) {
            ConditionPtr child;
            if (!buildCondition(param, child)) {
                SQL_LOG(WARN, "build cond [%s] failed",
                        RapidJsonHelper::SimpleValue2Str(param).c_str());
                return false;
            }
            condition->addCondition(child);
        } else if (param.IsString()) {
            ConditionPtr child;
            child.reset(new LeafCondition(param));
            condition->addCondition(child);
        } else {
            SQL_LOG(WARN, "unexpected param [%s]",
                    RapidJsonHelper::SimpleValue2Str(param).c_str());
            return false;
        }
    }
    return true;
}

END_HA3_NAMESPACE(sql);
