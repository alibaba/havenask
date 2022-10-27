#ifndef ISEARCH_CONDITIONPARSER_H
#define ISEARCH_CONDITIONPARSER_H

#include <ha3/sql/common/common.h>
#include <ha3/util/Log.h>
#include <ha3/sql/ops/condition/Condition.h>

BEGIN_HA3_NAMESPACE(sql);

class ConditionParser
{
public:
    ConditionParser(autil::mem_pool::Pool *pool = NULL);
    ~ConditionParser();
private:
    ConditionParser(const ConditionParser &);
    ConditionParser& operator=(const ConditionParser &);
public:
    bool parseCondition(const std::string &jsonStr, ConditionPtr &condition);
private:
    bool buildCondition(const autil_rapidjson::SimpleValue &jsonObject,
                        ConditionPtr &condition);
    bool validateCondition(const ConditionPtr &condition) const { return true; }
private:
    autil::mem_pool::Pool *_pool;
private:
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(ConditionParser);

END_HA3_NAMESPACE(sql);

#endif //ISEARCH_CONDITIONPARSER_H
