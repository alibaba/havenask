#ifndef ISEARCH_RANGESET_H
#define ISEARCH_RANGESET_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>

BEGIN_HA3_NAMESPACE(common);

class RangeSet
{
public:
    RangeSet();
    ~RangeSet();
private:
    typedef std::map<uint32_t, uint32_t> RangeMap;
public:
    bool push(uint32_t from, uint32_t to);
    bool tryPush(uint32_t from, uint32_t to);
    bool isComplete() const;
    uint32_t getCoveredRange() const;
private:
    RangeMap _map;
private:
    friend class RangeSetTest;
private:
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(RangeSet);



END_HA3_NAMESPACE(common);

#endif //ISEARCH_RANGESET_H
