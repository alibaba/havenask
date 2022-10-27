#ifndef ISEARCH_SEARCH_MATCHVALUES_H_
#define ISEARCH_SEARCH_MATCHVALUES_H_
#include <ha3/common.h>

BEGIN_HA3_NAMESPACE(rank);

class MatchValues 
{
public:
    MatchValues()
        : _numTerms(0)
        , _maxNumTerms(0)
    {
    }
    ~MatchValues() { 
    }
private:
    MatchValues(const MatchValues&);
    MatchValues& operator=(const MatchValues &);
public:
    static uint32_t sizeOf(uint32_t numTerms) {
       return (uint32_t) (sizeof(MatchValues) + 
                          (numTerms - 1) * sizeof(matchvalue_t));
    }
public:
    matchvalue_t& nextFreeMatchValue() {
        assert(_numTerms < _maxNumTerms);
        if (_numTerms > 0) {
            new(&_termMatchValue[_numTerms])matchvalue_t;
        }
        return _termMatchValue[_numTerms++];
    }

    const matchvalue_t& getTermMatchValue(uint32_t idx) const {
        assert(idx < _numTerms);
        return _termMatchValue[idx];
    }

    matchvalue_t& getTermMatchValue(uint32_t idx) {
        assert(idx < _numTerms);
        return _termMatchValue[idx];
    }

    void setMaxNumTerms(uint32_t maxNumTerms) {_maxNumTerms = maxNumTerms;}
    uint32_t getNumTerms() const {return _numTerms;}
    
private:
    uint32_t _numTerms;
    uint32_t _maxNumTerms;
    matchvalue_t _termMatchValue[1];
private:
    HA3_LOG_DECLARE();    
};

typedef MatchValues Ha3MatchValues;

END_HA3_NAMESPACE(rank);
NOT_SUPPORT_CLONE_TYPE(isearch::rank::MatchValues);
NOT_SUPPORT_SERIZLIE_TYPE(isearch::rank::MatchValues);

#endif
