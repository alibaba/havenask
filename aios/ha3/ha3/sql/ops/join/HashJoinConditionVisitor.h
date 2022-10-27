#ifndef ISEARCH_HASHJOINCONDITIONVISITOR_H
#define ISEARCH_HASHJOINCONDITIONVISITOR_H

#include <ha3/sql/common/common.h>
#include <ha3/sql/ops/condition/ConditionVisitor.h>
#include <autil/legacy/RapidJsonCommon.h>

BEGIN_HA3_NAMESPACE(sql);

class HashJoinConditionVisitor : public ConditionVisitor
{
public:
    HashJoinConditionVisitor(
            const std::map<std::string, std::pair<std::string, bool>> &output2InputMap);
    ~HashJoinConditionVisitor();
public:
    void visitAndCondition(AndCondition *condition) override;
    void visitOrCondition(OrCondition *condition) override;
    void visitNotCondition(NotCondition *condition) override;
    void visitLeafCondition(LeafCondition *condition) override;
public:
    void stealJoinColumns(std::vector<std::string> &left, std::vector<std::string> &right);

    std::vector<std::string> getLeftJoinColumns() {
        return _leftColumns;
    }
    std::vector<std::string> getRightJoinColumns() {
        return _rightColumns;
    }
private:
    bool parseJoinColumns(const autil_rapidjson::SimpleValue& condition,
                          std::string &left, std::string &right);
    bool parseJoinColumn(const autil_rapidjson::SimpleValue& value,
                         std::string &column, bool &hasAnyUdf);
    bool transOutputToInput(const std::string &outputFiled);

private:
    const std::map<std::string, std::pair<std::string, bool>> &_output2InputMap;
    std::vector<std::string> _leftColumns;
    std::vector<std::string> _rightColumns;
};

HA3_TYPEDEF_PTR(HashJoinConditionVisitor);

END_HA3_NAMESPACE(sql);

#endif //ISEARCH_HASHJOINCONDITIONVISITOR_H
