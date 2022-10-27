#ifndef ISEARCH_TERMMATCHDATA_H_
#define ISEARCH_TERMMATCHDATA_H_

#include <ha3/index/index.h>
#include <ha3/isearch.h>
#include <indexlib/index/normal/inverted_index/accessor/term_match_data.h>
#include <indexlib/index/normal/inverted_index/accessor/in_doc_position_state.h>


BEGIN_HA3_NAMESPACE(rank);

class TermMatchData {
public:
    TermMatchData() {}
    TermMatchData(index::TermMatchData &tmd) {
        _tmd = tmd;
    }
    TermMatchData(const TermMatchData &tmd) = delete;
    ~TermMatchData() {
        if (NULL != _tmd.GetInDocPositionState()) {
            _tmd.FreeInDocPositionState();
        }
    }
private:
    TermMatchData& operator=(const TermMatchData &);
public:
    inline bool isMatched() const {return _tmd.IsMatched();}
    inline tf_t getTermFreq() const {return _tmd.GetTermFreq();}
    inline uint32_t getFirstOcc() const {return _tmd.GetFirstOcc(); }
    /* return the matched 'fieldbitmap' for this term. */
    inline fieldbitmap_t getFieldBitmap() const {return _tmd.GetFieldMap();}
    inline index::InDocPositionIteratorPtr getInDocPositionIterator() const {
        return _tmd.GetInDocPositionIterator();
    }
    inline index::TermMatchData& getTermMatchData() { return _tmd;}
    inline const index::TermMatchData& getTermMatchData() const { return _tmd;}

    inline docpayload_t getDocPayload() const {return _tmd.GetDocPayload();}

    inline void setDocPayload(docpayload_t docPayload) {
        _tmd.SetDocPayload(docPayload);
    }
    inline void setTermFreq(tf_t termFreq) {
        _tmd.SetTermFreq(termFreq);
    }
    inline void setFirstOcc(uint32_t firstOcc) {
        _tmd.SetFirstOcc(firstOcc); 
    }
    inline void setFieldBitmap(fieldbitmap_t fieldBitmap) {
        _tmd.SetFieldMap(fieldBitmap); 
    }

    inline void setInDocPositionState(index::InDocPositionState *state) {
        _tmd.SetInDocPositionState(state);
    }

    inline void setTermMatchData(const index::TermMatchData &tmd) {
        _tmd = tmd;
    }
    
    // for test
    inline void setMatched(bool flag) {
        _tmd.SetMatched(flag);
    }
private:
    index::TermMatchData _tmd;
};

END_HA3_NAMESPACE(rank);
#endif
