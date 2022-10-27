#ifndef ISEARCH_PAGEDISTINCTHIT_H
#define ISEARCH_PAGEDISTINCTHIT_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>

BEGIN_HA3_NAMESPACE(qrs);


class PageDistinctHitConvertor {
public:
    PageDistinctHitConvertor(const std::string &keyName,
                             const std::vector<bool> &sortFlags,
                             const std::vector<double> &grades)
    {
        _keyName = keyName;
        _sortFlags = sortFlags;
        _gradeCalculator.setGradeThresholds(grades);
    }

    template <typename KeyType>
    void convert(const HitVector &hits, 
                 std::vector<PageDistinctHit<KeyType>* > &pageDistHits)
    {
        assert(!_sortFlags.empty());

        pageDistHits.clear();
        pageDistHits.reserve(hits.size());
        for (size_t pos = 0; pos < hits.size(); pos++) {
            const Hit &hit = *hits[pos];
            PageDistinctHit<KeyType> *pgDistHit = 
                new PageDistinctHit<KeyType>();
            pgDistHit->pos = pos;
            uint32_t sortValueCount = hit.getSortExprCount();
            pgDistHit->sortValues.resize(_sortFlags.size());
            for (uint32_t i = 0; i < sortValueCount && i < _sortFlags.size(); i++) {
                const string &str = hit.getSortExprValue(i);
                double s = autil::StringUtil::strToDoubleWithDefault(str.c_str(), 0.0);
                if (_sortFlags[i]) {
                    s = -s;
                }
                pgDistHit->sortValues[i] = s;
            }

            pgDistHit->gid = hit.getGlobalIdentifier();

            if(!hit->getAttribute<KeyType>(_distKey, pgDistHit->keyValue))
            {
                HA3_LOG(WARN, 
                        "fail to get distkey [%s] value, "
                        "this hit was dropped!", _distKey.c_str());
                pgDistHit->hasKeyValue = false;
            } else {
                pgDistHit->hasKeyValue = true;
            }

            int32_t gradeValue = _gradeCalculator.calculate(
                    pgDistHit->sortValues[0]);
            if (_sortFlags[0]) {
                gradeValue = -gradeValue;
            }
            pgDistHit->gradeValue = gradeValue;

            pageDistHits.push_back(pgDistHit);
        }
    }
private:
    std::string _keyName;
    std::vector<bool> _sortFlags;
    rank::GradeCalculatorBase _gradeCalculator;
private:
    HA3_LOG_DECLARE();
};


END_HA3_NAMESPACE(qrs);

#endif //ISEARCH_PAGEDISTINCTHIT_H
