#ifndef ISEARCH_FAKEPHASESCORER_H
#define ISEARCH_FAKEPHASESCORER_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/common/Request.h>
#include <matchdoc/MatchDoc.h>
#include <matchdoc/Reference.h>
#include <ha3/rank/ScoringProvider.h>
#include <ha3/rank/Scorer.h>

BEGIN_HA3_NAMESPACE(search);

class FakePhaseScorer : public rank::Scorer
{
public:
    FakePhaseScorer(const std::string &name);
    ~FakePhaseScorer();

public:
    bool beginRequest(suez::turing::ScoringProvider *provider);
    score_t score(matchdoc::MatchDoc &matchDoc);
    void endRequest();
    void destroy();
    Scorer* clone();

private:
    template<typename T>
    T fromString(const std::string &value) {
        return autil::StringUtil::fromString<T>(value.c_str());
    }
    template<typename T>
    T getValue(const common::ConfigClause *configClause,
               const std::string &key)
    {
        std::string value = configClause->getKVPairValue(key + "_" + _name);
        if (value.empty()) {
            return T();
        }
        return fromString<T>(value);
    }
protected:
    TRACE_DECLARE();

private:
    std::string _name;
    const matchdoc::Reference<int32_t> *_typeRef;
    const matchdoc::Reference<score_t> *_scoreRef;
    const matchdoc::Reference<std::string> *_stringRef;
    int32_t _terminateSeekCount;
    common::QueryTerminator *_queryTerminator;
    common::Tracer *_requestTracer;
    std::string _auxTableKey;
    IE_NAMESPACE(partition)::AuxTableReaderTyped<int32_t> *_auxTableReader;
private:
    HA3_LOG_DECLARE();
};

///////////////////////////////////////////////////////////////////

template<>
inline bool FakePhaseScorer::fromString<bool>(const std::string &value) {
    if (value == "true" || value == "TRUE" || value == "yes" || value == "Y") {
        return true;
    }
    return false;
}

template<>
inline std::vector<score_t> FakePhaseScorer::fromString<std::vector<score_t> >(const std::string &value) {
    std::vector<score_t> ret;
    autil::StringUtil::fromString<score_t>(value.c_str(), ret, ",");
    return ret;
}

END_HA3_NAMESPACE(search);

#endif //ISEARCH_FAKEMULTIPHASESCORER1_H
