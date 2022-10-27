#include "CommunicationScorer.h"
#include <ha3/rank/MatchData.h>

USE_HA3_NAMESPACE(common);
USE_HA3_NAMESPACE(search);

BEGIN_HA3_NAMESPACE(rank);

bool CommunicationScorer::beginRequest(suez::turing::ScoringProvider *provider) {
    _distanceRef = provider->findVariableReference<double>("distance");
    _priceAttr = provider->requireAttribute<double>("price2");

    _sellerIdAttr = provider->requireAttribute<uint32_t>("user_id");
    _sellerIdRef = provider->declareVariable<uint32_t>("user_id", true);
    return _sellerIdRef && _sellerIdAttr && _priceAttr;
}

score_t CommunicationScorer::score(matchdoc::MatchDoc &matchDoc) {
    assert(_priceAttr);
    assert(_sellerIdAttr);
    assert(_sellerIdRef);
    score_t score = 0.0;
    double price;
    price = _priceAttr->get(matchDoc);
    if (_distanceRef) {
        double distance;
        distance = _distanceRef->get(matchDoc);
        score = 1 / distance * price;
    } else {
        score = 1 / price;
    }
    uint32_t userId = 0;
    userId = _sellerIdAttr->get(matchDoc);
    _sellerIdRef->set(matchDoc, userId);

    return score;
}

END_HA3_NAMESPACE(rank);
