#ifndef ISEARCH_MULTILEVELRANGESET_H
#define ISEARCH_MULTILEVELRANGESET_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/common/RangeSet.h>

BEGIN_HA3_NAMESPACE(common);

class MultiLevelRangeSet
{
public:
    explicit MultiLevelRangeSet(uint32_t level);
    ~MultiLevelRangeSet();
private:
    MultiLevelRangeSet(const MultiLevelRangeSet &);
    MultiLevelRangeSet& operator=(const MultiLevelRangeSet &);
public:
    bool push(uint32_t from, uint32_t to);
    bool tryPush(uint32_t from, uint32_t to);
    bool isComplete() const;

    size_t getLevel() const {return _rangeSets.size();} 
private:
    std::vector<RangeSet> _rangeSets;
private:
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(MultiLevelRangeSet);

END_HA3_NAMESPACE(common);

#endif //ISEARCH_MULTILEVELRANGESET_H
