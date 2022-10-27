#include <ha3/common/RangeSet.h>
#include <algorithm>

BEGIN_HA3_NAMESPACE(common);
HA3_LOG_SETUP(common, RangeSet);

RangeSet::RangeSet() { 
}

RangeSet::~RangeSet() { 
}

bool RangeSet::push(uint32_t from, uint32_t to)
{
    if (from > to) {
        HA3_LOG(WARN, "Don't give me invalid range!");
        return false;
    }

    uint32_t key = from;    
    RangeMap::iterator it = _map.upper_bound(from);
    if (it != _map.begin()) {
        --it;
        if (it->second >= to) {
            return false;
        }
        if (from > it->second + 1) {
            ++it;
        } else {
            key = it->first;
        }
    }
    
    uint32_t value = to;
    RangeMap::iterator it2 = _map.upper_bound(to + 1);
    if (it2 != _map.begin()) {
        RangeMap::iterator tmpIt = it2;
        --tmpIt;
        value = std::max(to, tmpIt->second);
    }
    
    _map.erase(it, it2);
    _map[key] = value;
    return true;
}

bool RangeSet::tryPush(uint32_t from, uint32_t to) {
    if (from > to) {
        return false;
    }
    
    for (RangeMap::iterator it = _map.begin(); it != _map.end(); ++it) {
        if (from >= it->first && to <= it->second) {
            return false;
        }
    }
    return true;
}

bool RangeSet::isComplete() const {
    return (_map.size() == 1) 
        && (_map.begin()->first == 0) 
        && (_map.begin()->second == MAX_PARTITION_RANGE);
}

uint32_t RangeSet::getCoveredRange() const {
    uint32_t ret = 0;
    for (RangeMap::const_iterator it = _map.begin(); it != _map.end(); ++it) {
        ret += it->second - it->first + 1;
    }
    return ret;
}

END_HA3_NAMESPACE(common);

