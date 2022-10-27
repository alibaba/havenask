#ifndef ISEARCH_SEARCH_MATCHDATA_H_
#define ISEARCH_SEARCH_MATCHDATA_H_
#include <ha3/common.h>
#include <ha3/util/Log.h>
#include <ha3/rank/TermMatchData.h>
#include <matchdoc/Trait.h>

BEGIN_HA3_NAMESPACE(rank);

class MatchData 
{
public:
    MatchData()
        : _numTerms(0)
        , _maxNumTerms(0)
    {
    }
    ~MatchData() { 
        reset();
    }
private:
    MatchData(const MatchData&);
    MatchData& operator=(const MatchData &);
public:
    static uint32_t sizeOf(uint32_t numTerms) {
       return (uint32_t) (sizeof(MatchData) + 
                          (numTerms - 1) * sizeof(TermMatchData));
    }
public:
    void reset() {
        TermMatchData *curMatchData = _termMatchData + 1;
        TermMatchData *end = _termMatchData + _numTerms;
        while (curMatchData < end) {
            curMatchData->~TermMatchData();
            ++curMatchData ;
        }
        _numTerms = 0;
    }
    TermMatchData& nextFreeMatchData() {
        assert(_numTerms < _maxNumTerms);
        if (_numTerms > 0) {
            new(&_termMatchData[_numTerms])TermMatchData;
        }
        return _termMatchData[_numTerms++];
    }

    const TermMatchData& getTermMatchData(uint32_t idx) const {
        assert(idx < _numTerms);
        return _termMatchData[idx];
    }

    TermMatchData& getTermMatchData(uint32_t idx) {
        assert(idx < _numTerms);
        return _termMatchData[idx];
    }

    void setMaxNumTerms(uint32_t maxNumTerms) {_maxNumTerms = maxNumTerms;}
    uint32_t getNumTerms() const {return _numTerms;}
    
private:
    uint32_t _numTerms;
    uint32_t _maxNumTerms;
    TermMatchData _termMatchData[1];
private:
    HA3_LOG_DECLARE();    
};

END_HA3_NAMESPACE(rank);

NOT_SUPPORT_CLONE_TYPE(isearch::rank::MatchData);
NOT_SUPPORT_SERIZLIE_TYPE(isearch::rank::MatchData);

#endif
