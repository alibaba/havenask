#ifndef ISEARCH_AGGREGATEDESCRIPTION_H
#define ISEARCH_AGGREGATEDESCRIPTION_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <autil/DataBuffer.h>
#include <ha3/common/FilterClause.h>
#include <ha3/common/AggFunDescription.h>
#include <suez/turing/expression/syntax/SyntaxExpr.h>

BEGIN_HA3_NAMESPACE(common);

class AggregateDescription
{
public:
    AggregateDescription();
    AggregateDescription(const std::string &originalString);
    ~AggregateDescription();
public:
    suez::turing::SyntaxExpr* getGroupKeyExpr() const;
    void setGroupKeyExpr(suez::turing::SyntaxExpr*);

    const std::vector<AggFunDescription*>& getAggFunDescriptions() const;
    void setAggFunDescriptions(const std::vector<AggFunDescription*>&);
    void appendAggFunDescription(AggFunDescription*);

    bool isRangeAggregate() const;
    const std::vector<std::string>& getRange() const;
    void setRange(const std::vector<std::string>&);

    uint32_t getMaxGroupCount() const;
    void setMaxGroupCount(int32_t);
    void setMaxGroupCount(const std::string &);

    //filter used in aggregate process
    FilterClause* getFilterClause() const;
    void setFilterClause(FilterClause*);

    std::string getOriginalString() const;
    void setOriginalString(const std::string &originString);

    uint32_t getAggThreshold() const;
    void setAggThreshold(uint32_t aggThreshold);
    void setAggThreshold(const std::string &aggThresholdStr);

    uint32_t getSampleStep() const;
    void setSampleStep(uint32_t sampleStep);
    void setSampleStep(const std::string &sampleStepStr);

    void serialize(autil::DataBuffer &dataBuffer) const;
    void deserialize(autil::DataBuffer &dataBuffer);
    std::string toString() const;

private:
    void clearAggDescriptions();
private:
    std::string _originalString;
    suez::turing::SyntaxExpr* _groupKeyExpr;
    std::vector<AggFunDescription*> _aggFunDescriptions;
    std::vector<std::string> _ranges;

    int32_t _maxGroupCount;
    FilterClause* _filterClause;

    uint32_t _aggThreshold;
    uint32_t _sampleStep;
private:
    HA3_LOG_DECLARE();
};

END_HA3_NAMESPACE(common);

#endif //ISEARCH_AGGREGATEDESCRIPTION_H
