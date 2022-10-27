#ifndef ISEARCH_DISTINCTPRPARSER_H
#define ISEARCH_DISTINCTPRPARSER_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <string>
#include <vector>
#include <ha3/common/DistinctDescription.h>
#include <ha3/common/ErrorResult.h>

BEGIN_HA3_NAMESPACE(queryparser);

class DistinctParser
{
public:
    DistinctParser(common::ErrorResult *errorResult);
    ~DistinctParser();
public:
    common::DistinctDescription *createDistinctDescription();
    std::vector<std::string *> *createThresholds();
    common::FilterClause* createFilterClause(suez::turing::SyntaxExpr *expr);
public:
    void setDistinctCount(common::DistinctDescription *distDes,
                          const std::string &clauseCountStr);
    void setDistinctTimes(common::DistinctDescription *distDes,
                          const std::string &clauseTimesStr);
    void setMaxItemCount(common::DistinctDescription *distDes,
                         const std::string &maxItemCountStr);
    void setReservedFlag(common::DistinctDescription *distDes,
                         const std::string &reservedFalg);
    void setUpdateTotalHitFlag(common::DistinctDescription *distDes,
                               const std::string &updateTotalHitFlagStr);
    void setGradeThresholds(common::DistinctDescription *distDes,
                            std::vector<std::string *> *gradeThresholdsStr);
private:
    common::ErrorResult *_errorResult;
private:
    HA3_LOG_DECLARE();
};

END_HA3_NAMESPACE(queryparser);

#endif //ISEARCH_DISTINCTPRPARSER_H
