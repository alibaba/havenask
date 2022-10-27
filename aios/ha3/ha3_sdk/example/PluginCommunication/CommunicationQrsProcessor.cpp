#include "CommunicationQrsProcessor.h"
#include <autil/StringUtil.h>

using namespace std;
using namespace autil;

USE_HA3_NAMESPACE(common);

BEGIN_HA3_NAMESPACE(qrs);

void CommunicationQrsProcessor::process(RequestPtr &requestPtr, 
                                        ResultPtr &resultPtr) 
{
    // do something before search.
    QrsProcessor::process(requestPtr, resultPtr);
    if (!doDedup(resultPtr)) {
        return;
    }
    addMinPostageToMetaHits(resultPtr);
}

bool CommunicationQrsProcessor::doDedup(ResultPtr &resultPtr)
{
    map<uint32_t, HitPtr> userIdMap;
    Hits *hits = resultPtr->getHits();
    const vector<HitPtr> &hitVec = hits->getHitVec();
    for (vector<HitPtr>::const_iterator it = hitVec.begin();
         it != hitVec.end(); ++it)
    {
        const HitPtr &hit = *it;
        uint32_t userId = 0;
        ErrorCode ec = hit->getVariableValue("user_id", userId);
        if (ERROR_NONE != ec) {
            ErrorResult error(ec, "get variable[user_id] failed");
            resultPtr->addErrorResult(error);
            return false;
        }
        map<uint32_t, HitPtr>::iterator mIt = userIdMap.find(userId);
        if (mIt == userIdMap.end()) {
            userIdMap[userId] = hit;
        } else {
            const HitPtr& lastHit = mIt->second;
            if (lastHit->lessThan(hit)) {
                userIdMap[userId] = hit;
            }
        }
    }
    vector<HitPtr> resultHitVec;
    resultHitVec.reserve(userIdMap.size());
    for (map<uint32_t, HitPtr>::const_iterator it = userIdMap.begin();
         it != userIdMap.end(); ++it)
    {
        resultHitVec.push_back(it->second);
    }
    hits->setHitVec(resultHitVec);
    hits->sortHits();
    return true;
}

void CommunicationQrsProcessor::addMinPostageToMetaHits(ResultPtr &resultPtr) {
    vector<double*> minPostages = resultPtr->getGlobalVarialbes<double>("min_postage");
    double minPostage = numeric_limits<double>::max();
    for (vector<double*>::iterator it = minPostages.begin();
         it != minPostages.end(); ++it)
    {
        double postage = *(*it);
        minPostage = minPostage < postage ? minPostage : postage;
    }

    MetaHit metaHit;
    metaHit["min_postage"] = StringUtil::toString(minPostage);
    resultPtr->getHits()->addMetaHit("user_aggregate", metaHit);
}

END_HA3_NAMESPACE(qrs);

