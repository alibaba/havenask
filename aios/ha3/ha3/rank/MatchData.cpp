#include <ha3/rank/MatchData.h>
#include <assert.h>

using namespace std;
BEGIN_HA3_NAMESPACE(rank);
HA3_LOG_SETUP(rank, MatchData);

/*void MatchData::reset() {
    TermMatchData *curMatchData = _termMatchData + 1;
    TermMatchData *end = _termMatchData + _numTerms;
    while (curMatchData < end) {
        curMatchData->~TermMatchData();
        ++curMatchData ;
    }
    _numTerms = 0;
    }*/

END_HA3_NAMESPACE(rank);
