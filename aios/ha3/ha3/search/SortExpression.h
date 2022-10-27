#ifndef ISEARCH_SORTEXPRESSION_H
#define ISEARCH_SORTEXPRESSION_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/common/Ha3MatchDocAllocator.h>
#include <suez/turing/expression/framework/AttributeExpression.h>

BEGIN_HA3_NAMESPACE(search);

class SortExpression {
public:
    SortExpression(suez::turing::AttributeExpression *attributeExpression, bool sortFlag = false);
    ~SortExpression();
private:
    SortExpression(const SortExpression &);
    SortExpression& operator=(const SortExpression &);
public:
    suez::turing::AttributeExpression* getAttributeExpression() {
        return _attributeExpression; 
    }
    bool allocate(common::Ha3MatchDocAllocator *slabAllocator) {
        return _attributeExpression->allocate(slabAllocator);
    }
    bool evaluate(matchdoc::MatchDoc matchDoc) {
        return _attributeExpression->evaluate(matchDoc);
    }
    bool batchEvaluate(matchdoc::MatchDoc *matchDocs, uint32_t matchDocCount) {
        return _attributeExpression->batchEvaluate(matchDocs, matchDocCount);
    }
    matchdoc::ReferenceBase* getReferenceBase() const {
        return _attributeExpression->getReferenceBase();
    }
    void setEvaluated() {
        return _attributeExpression->setEvaluated();
    }
    bool isEvaluated() const {
        return _attributeExpression->isEvaluated();
    }
    VariableType getType() const {
        return _attributeExpression->getType();
    }
    void setSortFlag(bool flag) {
        _sortFlag = flag;
    }
    bool getSortFlag() const {
        return _sortFlag;
    }
    const std::string& getOriginalString() const { 
        return _attributeExpression->getOriginalString(); 
    }
    void setOriginalString(const std::string& originalString) { 
        _attributeExpression->setOriginalString(originalString);
    }
    bool isMultiValue() const {
        return _attributeExpression->isMultiValue();
    }
private:
    bool _sortFlag;
    suez::turing::AttributeExpression *_attributeExpression;
private:
    HA3_LOG_DECLARE();
};

typedef std::vector<SortExpression*> SortExpressionVector;
HA3_TYPEDEF_PTR(SortExpression);

END_HA3_NAMESPACE(search);

#endif //ISEARCH_SORTEXPRESSION_H
