#include <ha3/rank/DistinctInfo.h>
#include <matchdoc/MatchDoc.h>
#include <assert.h>

BEGIN_HA3_NAMESPACE(rank);
HA3_LOG_SETUP(rank, DistinctInfo);

docid_t DistinctInfo::getDocId() const {
    return _matchDoc.getDocId();
}
END_HA3_NAMESPACE(rank);

