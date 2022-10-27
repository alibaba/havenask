#ifndef ISEARCH_GRADECALCULATOR_H
#define ISEARCH_GRADECALCULATOR_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <matchdoc/Reference.h>
#include <ha3/rank/DistinctInfo.h>
#include <vector>

BEGIN_HA3_NAMESPACE(rank);
class GradeCalculatorBase {
public:
    virtual ~GradeCalculatorBase() {};
public:
    virtual void calculate(DistinctInfo* distInfo) const {}
    uint32_t calculate(double score) const {
        size_t i = 0;
        size_t gradeSize = _gradeThresholds.size();
        for (; i < gradeSize; ++i)
        {
            if (score < _gradeThresholds[i]) {
                break;
            }
        }
        return i;
    }
    void setGradeThresholds(const std::vector<double> &gradeThresholds) {
        _gradeThresholds = gradeThresholds;
    }
protected:
    std::vector<double> _gradeThresholds;
};

template<typename T>
class GradeCalculator : public GradeCalculatorBase
{
public:
    GradeCalculator(matchdoc::Reference<T> *ref) {
        _rawScoreRef = ref;
    }
    ~GradeCalculator() {}
private:
    GradeCalculator(const GradeCalculator &);
    GradeCalculator& operator=(const GradeCalculator &);
public:
    void calculate(DistinctInfo* distInfo) const {
        size_t gradeSize = _gradeThresholds.size();
        if (gradeSize == 0) {
            return;
        }
        T value;
        value = _rawScoreRef->get(distInfo->getMatchDoc());
        double score = (double)value;
        uint32_t gradeValue = GradeCalculatorBase::calculate(score);
        distInfo->setGradeBoost(gradeValue);
    }
private:
    matchdoc::Reference<T> *_rawScoreRef;
private:
    HA3_LOG_DECLARE();
};


END_HA3_NAMESPACE(rank);

#endif //ISEARCH_GRADECALCULATOR_H
