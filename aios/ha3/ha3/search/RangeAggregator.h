#ifndef ISEARCH_RANGEAGGREGATOR_H
#define ISEARCH_RANGEAGGREGATOR_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <string>
#include <ha3/util/Log.h>
#include <vector>
#include <ha3/search/NormalAggregator.h>
#include <map>
#include <assert.h>
#include <sstream>

BEGIN_HA3_NAMESPACE(search);

template <typename KeyType, typename ExprType = KeyType, 
          typename GroupMapType = typename UnorderedMapTraits<KeyType>::GroupMapType>
class RangeAggregator : public NormalAggregator<KeyType, ExprType, GroupMapType>
{
public:
    RangeAggregator(suez::turing::AttributeExpressionTyped<ExprType> *attriExpr,
                    const std::vector<KeyType> &ranges,
                    autil::mem_pool::Pool *pool,
                    int32_t maxKeyCount = MAX_AGGREGATE_GROUP_COUNT,
                    uint32_t aggThreshold = 0,
                    uint32_t sampleStep = 1,
                    uint32_t maxSortCount = 0);

    ~RangeAggregator();

    void setRange(const std::vector<KeyType> &ranges);
    KeyType findRangeId(const KeyType &a) const;

    const std::vector<KeyType> getRangeVector() const;
protected:
    KeyType getGroupKeyValue(const KeyType &value) const override {
        return findRangeId(value);
    }
    void initGroupKeyRef() override {
        const auto &aggAllocatorPtr =
            NormalAggregator<KeyType, ExprType, GroupMapType>::getAggAllocator();
        // todo string type?
        _strGroupKeyRef = aggAllocatorPtr->template declare<std::string>(
                common::GROUP_KEY_REF, SL_CACHE);
    }
    void setGroupKey(matchdoc::MatchDoc matchDoc,
                     const KeyType &groupKeyValue) override
    {
        _strGroupKeyRef->set(matchDoc, transGroupKeyToString(groupKeyValue));
    }
private:
    std::string transGroupKeyToString(const KeyType &groupKeyValue);
private:
    std::vector<KeyType> _ranges;
    matchdoc::Reference<std::string> *_strGroupKeyRef;
private:
    HA3_LOG_DECLARE();
    friend class RangeAggregatorTest;
};

//////////////////////////////////////////////////////////////////////////
// implementation
//
template<typename KeyType, typename ExprType, typename GroupMapType>
RangeAggregator<KeyType, ExprType, GroupMapType>::RangeAggregator(
        suez::turing::AttributeExpressionTyped<ExprType> *attriExpr,
        const std::vector<KeyType> &ranges,
        autil::mem_pool::Pool *pool,
        int32_t maxKeyCount,
        uint32_t aggThreshold,
        uint32_t sampleStep,
        uint32_t maxSortCount)
    : NormalAggregator<KeyType, ExprType, GroupMapType>(attriExpr, maxKeyCount, pool,
            aggThreshold, sampleStep, maxSortCount)
{
    this->_ranges.push_back(util::NumericLimits<KeyType>::min());
    this->_ranges.insert(this->_ranges.end(), ranges.begin(), ranges.end());
    this->_ranges.push_back(util::NumericLimits<KeyType>::max());
}

template<typename KeyType, typename ExprType, typename GroupMapType>
RangeAggregator<KeyType, ExprType, GroupMapType>::~RangeAggregator() {
}

template<typename KeyType, typename ExprType, typename GroupMapType>
void RangeAggregator<KeyType, ExprType, GroupMapType>::setRange(const std::vector<KeyType> &ranges) {
    this->_ranges = ranges;
}

template<typename KeyType, typename ExprType, typename GroupMapType>
KeyType RangeAggregator<KeyType, ExprType, GroupMapType>::findRangeId(const KeyType &a) const {
    for (typename std::vector<KeyType>::const_reverse_iterator it = _ranges.rbegin() + 1;
         it != _ranges.rend(); it++)
    {
        if (*it <= a) {
            return (*it);
        }
    }
    assert(false);
    return util::NumericLimits<KeyType>::min();
}

template<typename KeyType, typename ExprType, typename GroupMapType>
const std::vector<KeyType> RangeAggregator<KeyType, ExprType, GroupMapType>::getRangeVector() const {
    return this->_ranges;
}

template<typename KeyType, typename ExprType, typename GroupMapType>
std::string RangeAggregator<KeyType, ExprType, GroupMapType>::transGroupKeyToString(const KeyType &groupKeyValue) {
    assert(_ranges.size());
    typename std::vector<KeyType>::const_iterator it
        = std::find(_ranges.begin(), _ranges.end() - 1, groupKeyValue);

    return autil::StringUtil::toString(*it) + ":"
        + autil::StringUtil::toString(*(it + 1));
}

END_HA3_NAMESPACE(search);

#endif //ISEARCH_RANGEAGGREGATOR_H
