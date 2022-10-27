#ifndef ISEARCH_COMMUNICATION_SCORER_H
#define ISEARCH_COMMUNICATION_SCORER_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/rank/Scorer.h>
#include <ha3/common/Ha3MatchDocAllocator.h>
#include <ha3/rank/ScoringProvider.h>
#include <string>

BEGIN_HA3_NAMESPACE(rank);
USE_HA3_NAMESPACE(common);
using namespace std;

#define SCORER_CONF "scorer_conf"

class CommunicationScorer : public Scorer
{
public:
    CommunicationScorer(const std::string &name = "CommunicationScorer")
        : Scorer(name)
        , _distanceRef(NULL)
        , _priceAttr(NULL)
        , _sellerIdRef(NULL)
        , _sellerIdAttr(NULL)
    {
    }
    Scorer* clone();
    virtual ~CommunicationScorer() {}
public:
    bool beginRequest(suez::turing::ScoringProvider *provider);
    score_t score(matchdoc::MatchDoc &matchDoc);
    void endRequest() {}
    void destroy() { delete this; }
private:
    matchdoc::Reference<double> *_distanceRef;
    const matchdoc::Reference<double> *_priceAttr;
    matchdoc::Reference<uint32_t> *_sellerIdRef;
    const matchdoc::Reference<uint32_t> *_sellerIdAttr;
};

END_HA3_NAMESPACE(rank);

#endif //ISEARCH_COMMUNICATION_SCORER_H
