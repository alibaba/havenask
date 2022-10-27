#ifndef ISEARCH_TERM_METAINFO_H_
#define ISEARCH_TERM_METAINFO_H_

#include <ha3/index/index.h>
#include <indexlib/index/normal/inverted_index/accessor/term_meta.h>
#include <ha3/common/Term.h>

BEGIN_HA3_NAMESPACE(rank);

class TermMetaInfo {
public:
    TermMetaInfo() {
        _df = 0;
        _tf =0;
        _payload = 0;
        _matchDataLevel = MDL_TERM;
    }
    TermMetaInfo(IE_NAMESPACE(index)::TermMeta *termMeta) {
        setTermMeta(termMeta);
    }
    ~TermMetaInfo() {

    }
public:
    inline df_t getDocFreq() const {
        return _df;
    }
    inline tf_t getTotalTermFreq() const {
        return _tf;
    }
    inline termpayload_t getPayload() const {
        return _payload;
    }
    inline int32_t getTermBoost() const {
        return _term.getBoost();
    }
    inline std::string getIndexName() const{
        return _term.getIndexName();
    }
    inline std::string getTruncateName() const{
        return _term.getTruncateName();
    }
    inline const common::RequiredFields& getRequiredFields() const{
        return _term.getRequiredFields();
    }
    inline const std::string& getTermText() const {
        return _term.getWord();
    }
    void setTerm(const common::Term &term) {
        _term = term;
    }
    void setTermMeta(IE_NAMESPACE(index)::TermMeta *termMeta) {
        _df = termMeta->GetDocFreq();
        _tf = termMeta->GetTotalTermFreq();
        _payload = termMeta->GetPayload();
    }
    void setMatchDataLevel(MatchDataLevel level) {
        _matchDataLevel = level;
    }
    void setQueryLabel(const std::string& label) {
        _queryLabel = label;
    }
    MatchDataLevel getMatchDataLevel() const { return _matchDataLevel; }
    const std::string& getQueryLabel() const { return _queryLabel; }
private:
    df_t _df;
    tf_t _tf;
    termpayload_t _payload;
    common::Term _term;
    MatchDataLevel _matchDataLevel;
    std::string  _queryLabel;
};

END_HA3_NAMESPACE(rank);
#endif
