#ifndef ISEARCH_PAGEDISTINCTHITCONVERTOR_H
#define ISEARCH_PAGEDISTINCTHITCONVERTOR_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/common/Hit.h>
#include <ha3/rank/GradeCalculator.h>

BEGIN_HA3_NAMESPACE(qrs);

template <typename KeyType>
struct PageDistinctHit
{
    size_t pos;
    common::GlobalIdentifier gid;
    double *sortValues;
    int32_t gradeValue;
    bool hasKeyValue;
    KeyType keyValue;
};

template <typename KeyType>
class PageDistinctHitConvertor
{
public:
    typedef PageDistinctHit<KeyType> TypedHit; 
private:
    struct PageDistinctHitComparator {
        bool operator () (const TypedHit *hit1,
                          const TypedHit *hit2) const 
        {
            for (size_t i = 0; i < _sortValueCount; i++) {
                if (hit1->sortValues[i] < hit2->sortValues[i]) {
                    return false;
                } else if (hit2->sortValues[i] < hit1->sortValues[i]) {
                    return true;
                }
            }
            return hit1->gid > hit2->gid;
        }
    
        PageDistinctHitComparator(uint32_t count) {_sortValueCount = count;}
    private:
        uint32_t _sortValueCount;
    };

public:
    PageDistinctHitConvertor(const std::string &keyName,
                             const std::vector<bool> &sortFlags,
                             const std::vector<double> &grades)
        : _keyName(keyName), _sortFlags(sortFlags)
    {
        _gradeCalculator.setGradeThresholds(grades);
        if (_sortFlags.empty()) {
            _sortFlags.resize(1, false);
        }
    }
    ~PageDistinctHitConvertor() {}
private:
    PageDistinctHitConvertor(const PageDistinctHitConvertor &);
    PageDistinctHitConvertor& operator=(const PageDistinctHitConvertor &);
public:
    void convertAndSort(const std::vector<common::HitPtr> &hits, 
                        std::vector<TypedHit* > &pageDistHit);
    
private:
    void convertSortValues(const common::Hit &hit, 
                           TypedHit *pgDistHit) const;

    void convertDistKey(const common::Hit &hit, 
                        TypedHit *pgDistHit) const;

private:
    std::string _keyName;
    std::vector<bool> _sortFlags;
    rank::GradeCalculatorBase _gradeCalculator;
    std::vector<TypedHit> _typedHitVec;
    std::vector<double> _sortValuesVec;

private:
    friend class PageDistinctHitConvertorTest;
private:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP_TEMPLATE(qrs, PageDistinctHitConvertor, keyType);

template <typename KeyType>
void PageDistinctHitConvertor<KeyType>::convertAndSort(
        const std::vector<common::HitPtr> &hits, 
        std::vector<TypedHit*> &pageDistHits)
{
    pageDistHits.clear();
    assert(!_sortFlags.empty());
    _typedHitVec.resize(hits.size());
    _sortValuesVec.clear();
    _sortValuesVec.resize(hits.size() * _sortFlags.size(), 0.0);

    pageDistHits.clear();
    pageDistHits.reserve(hits.size());
    for (size_t pos = 0; pos < hits.size(); pos++) {
        const common::Hit &hit = *hits[pos];
        TypedHit *pgDistHit = &_typedHitVec[pos];
        pgDistHit->pos = pos;
        pgDistHit->sortValues = &_sortValuesVec[pos * _sortFlags.size()];
        convertSortValues(hit, pgDistHit);
        convertDistKey(hit, pgDistHit);
        pgDistHit->gid = hit.getGlobalIdentifier();
        
        pageDistHits.push_back(pgDistHit);
    }

    std::sort(pageDistHits.begin(), pageDistHits.end(), 
              PageDistinctHitComparator(_sortFlags.size()));
}

template <typename KeyType>
void PageDistinctHitConvertor<KeyType>::convertDistKey(
        const common::Hit &hit, TypedHit *pgDistHit) const
{
    if(!hit.getAttribute<KeyType>(_keyName, 
                                  pgDistHit->keyValue))
    {
        HA3_LOG(WARN, 
                "fail to get distkey [%s] value, "
                "this hit was dropped!", _keyName.c_str());
        pgDistHit->hasKeyValue = false;
    } else {
        pgDistHit->hasKeyValue = true;
    }
}

template <typename KeyType>
void PageDistinctHitConvertor<KeyType>::convertSortValues(
        const common::Hit &hit, TypedHit *pgDistHit) const
{
    pgDistHit->gradeValue = 0;
    uint32_t sortValueCount = hit.getSortExprCount();
    for (uint32_t i = 0; i < sortValueCount && i < _sortFlags.size(); i++) {
        const std::string &sortExprValueStr = hit.getSortExprValue(i);
        double s = autil::StringUtil::strToDoubleWithDefault(
                sortExprValueStr.c_str(), 0.0);
        if (i == 0) {
            int32_t gradeValue = _gradeCalculator.calculate(s);
            if (_sortFlags[0]) {
                gradeValue = -gradeValue;
            }
            pgDistHit->gradeValue = gradeValue;
        }

        if (_sortFlags[i]) {
            s = -s;
        }
        pgDistHit->sortValues[i] = s;
    }
}

END_HA3_NAMESPACE(qrs);

#endif //ISEARCH_PAGEDISTINCTHITCONVERTOR_H
