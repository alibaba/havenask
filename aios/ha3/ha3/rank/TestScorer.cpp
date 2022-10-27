#include <ha3/rank/TestScorer.h>
#include <autil/StringUtil.h>
#include <ha3/util/NetFunction.h>

using namespace std;
using namespace autil;

BEGIN_HA3_NAMESPACE(rank);
HA3_LOG_SETUP(rank, TestScorer);

TestScorer::TestScorer()
  : DefaultScorer("TestScorer")
{
    _int32Ref = NULL;
    _count = 0;
    if (!util::NetFunction::getPrimaryIP(_ip)) {
        throw 0;
    }
}

TestScorer::~TestScorer() {
}

bool TestScorer::beginRequest(suez::turing::ScoringProvider *provider) {
    string ips = provider->getKVPairValue("searcher");
    if (util::NetFunction::containIP(ips, _ip) || ips == "all") {
        string beginRequestSleepTime = provider->getKVPairValue("sleepInScorerBeginRequest");
        if (!beginRequestSleepTime.empty()) {
            usleep(1000ll * StringUtil::fromString<int>(beginRequestSleepTime));
        }
        string declareVariableName = provider->getKVPairValue("declareVariable");
        if (!declareVariableName.empty()) {
            _int32Ref = provider->declareVariable<int32_t>(declareVariableName, true);
        }
    }

    return DefaultScorer::beginRequest(provider);
}

score_t TestScorer::score(matchdoc::MatchDoc &matchDoc) {
    if (_int32Ref) {
        _int32Ref->set(matchDoc, _count++);
    }
    return DefaultScorer::score(matchDoc);
}

END_HA3_NAMESPACE(rank);
