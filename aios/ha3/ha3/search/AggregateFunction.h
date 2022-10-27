#ifndef ISEARCH_AGGREGATEFUNCTION_H
#define ISEARCH_AGGREGATEFUNCTION_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <string>
#include <sstream>
#include <matchdoc/MatchDocAllocator.h>
#include <matchdoc/MatchDoc.h>
#include <suez/turing/expression/framework/AttributeExpression.h>

BEGIN_HA3_NAMESPACE(search);
class AggFunResult;

class AggregateFunction
{
public:
    AggregateFunction(const std::string &functionName,
                      const std::string &parameter,
                      VariableType resultType);
    virtual ~AggregateFunction();

public:
    virtual void calculate(matchdoc::MatchDoc matchDoc, matchdoc::MatchDoc aggMatchDoc) = 0;
    virtual void declareResultReference(matchdoc::MatchDocAllocator *aggAllocator) = 0;
    virtual matchdoc::ReferenceBase *getReference(uint id = 0) const = 0;
    virtual void estimateResult(double factor, matchdoc::MatchDoc aggMatchDoc) {
        return;
    }
    virtual void beginSample(matchdoc::MatchDoc aggMatchDoc) {
        return;
    }
    virtual void endLayer(uint32_t sampleStep, double factor,
                          matchdoc::MatchDoc aggMatchDoc)
    {
        return;
    }

    template<typename T>
    const matchdoc::Reference<T> *getReferenceTyped() const {
        return dynamic_cast<const matchdoc::Reference<T> *>(getReference());
    }

    virtual void initFunction(matchdoc::MatchDoc aggMatchDoc, autil::mem_pool::Pool* pool = NULL) = 0;
    virtual std::string getStrValue(matchdoc::MatchDoc aggMatchDoc) const = 0;
    virtual suez::turing::AttributeExpression *getExpr() = 0;

    virtual bool canCodegen() {
        return false;
    }

    const std::string& getFunctionName() const {
        return _functionName;
    }
    const std::string& getParameter() const {
        return _parameter;
    }

    VariableType getResultType() const{
        return _resultType;
    }

    const std::string toString() const {
        return _functionName + "(" + _parameter + ")";
    }

protected:
    const std::string _functionName;
    const std::string _parameter;
    VariableType _resultType;
private:
    HA3_LOG_DECLARE();
};

END_HA3_NAMESPACE(search);

#endif //ISEARCH_AGGREGATEFUNCTION_H
