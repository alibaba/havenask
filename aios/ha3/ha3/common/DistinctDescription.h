#ifndef ISEARCH_DISTINCTDESCRIPTION_H
#define ISEARCH_DISTINCTDESCRIPTION_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <autil/DataBuffer.h>
#include <suez/turing/expression/syntax/SyntaxExpr.h>
#include <ha3/common/FilterClause.h>

BEGIN_HA3_NAMESPACE(common);

#define DEFAULT_DISTINCT_MODULE_NAME ""

class DistinctDescription
{
public:
    DistinctDescription(const std::string &moduleName = DEFAULT_DISTINCT_MODULE_NAME,
                        const std::string &originalString = "",
                        int32_t distinctCount = 1, 
                        int32_t distinctTimes = 1,
                        int32_t maxItemCount = 0,
                        bool reservedFlag = true,
                        bool updateTotalHitFlag = false,
                        suez::turing::SyntaxExpr *syntaxExpr = NULL,
                        FilterClause *filterClause = NULL);

    ~DistinctDescription();
private:
    DistinctDescription(const DistinctDescription &);
    DistinctDescription& operator=(const DistinctDescription &);
public:
    int32_t getDistinctCount() const;
    void setDistinctCount(int32_t distinctCount);

    int32_t getDistinctTimes() const;
    void setDistinctTimes(int32_t distinctTimes);

    int32_t getMaxItemCount() const;
    void setMaxItemCount(int32_t maxItemCount);

    bool getReservedFlag() const;
    void setReservedFlag(bool reservedFalg);

    bool getUpdateTotalHitFlag() const;
    void setUpdateTotalHitFlag(bool uniqFlag);

    const std::string& getOriginalString() const;
    void setOriginalString(const std::string& originalString);

    const std::string& getModuleName() const;
    void setModuleName(const std::string& moduleName);

    suez::turing::SyntaxExpr *getRootSyntaxExpr() const;
    void setRootSyntaxExpr(suez::turing::SyntaxExpr* syntaxExpr);

    FilterClause* getFilterClause() const;
    void setFilterClause(FilterClause* filterClause);

    const std::vector<double>& getGradeThresholds() const;
    void setGradeThresholds(const std::vector<double>& gradeThresholds);

    void serialize(autil::DataBuffer &dataBuffer) const;
    void deserialize(autil::DataBuffer &dataBuffer);
    std::string toString() const;
private:
    std::string _moduleName;
    std::string _originalString;
    int32_t _distinctTimes;
    int32_t _distinctCount;
    int32_t _maxItemCount;
    bool _reservedFlag;
    bool _updateTotalHitFlag;
    suez::turing::SyntaxExpr *_rootSyntaxExpr;
    FilterClause* _filterClause;
    std::vector<double> _gradeThresholds;
private:
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(DistinctDescription);

END_HA3_NAMESPACE(common);

#endif //ISEARCH_DISTINCTDESCRIPTION_H
