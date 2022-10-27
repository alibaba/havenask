#include <ha3/common/MultiLevelRangeSet.h>

BEGIN_HA3_NAMESPACE(common);
HA3_LOG_SETUP(common, MultiLevelRangeSet);

MultiLevelRangeSet::MultiLevelRangeSet(uint32_t level)
    : _rangeSets(level)
{ 
}

MultiLevelRangeSet::~MultiLevelRangeSet() { 
}

bool MultiLevelRangeSet::push(uint32_t from, uint32_t to) 
{
    for (size_t i = 0; i < _rangeSets.size(); ++i) {
        if (_rangeSets[i].push(from, to)) {
            return true;
        }
    }
    return false;
}

bool MultiLevelRangeSet::tryPush(uint32_t from, uint32_t to) 
{
    for (size_t i = 0; i < _rangeSets.size(); ++i) {
        if (_rangeSets[i].tryPush(from, to)) {
            return true;
        }
    }
    return false;
}

bool MultiLevelRangeSet::isComplete() const 
{
    for (size_t i = 0; i < _rangeSets.size(); ++i) {
        if (!_rangeSets[i].isComplete()) {
            return false;
        }
    }
    return true;
}


END_HA3_NAMESPACE(common);

