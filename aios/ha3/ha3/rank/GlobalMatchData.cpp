#include <ha3/rank/GlobalMatchData.h>
#include <assert.h>

BEGIN_HA3_NAMESPACE(rank);
HA3_LOG_SETUP(rank, GlobalMatchData);

GlobalMatchData::GlobalMatchData(int32_t docCount) {
    _docCount = docCount;
}

GlobalMatchData::~GlobalMatchData() { 
}

int32_t GlobalMatchData::getDocCount() const {
    return _docCount;
}

void GlobalMatchData::setDocCount(int32_t docCount) {
    _docCount = docCount;
}

END_HA3_NAMESPACE(rank);

