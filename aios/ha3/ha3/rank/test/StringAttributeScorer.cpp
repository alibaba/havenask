#include <ha3/rank/test/StringAttributeScorer.h>
#include <autil/MultiValueType.h>
USE_HA3_NAMESPACE(search);
USE_HA3_NAMESPACE(common);

using namespace autil;
using namespace std;
using namespace autil;
BEGIN_HA3_NAMESPACE(rank);
HA3_LOG_SETUP(rank, StringAttributeScorer);

StringAttributeScorer::StringAttributeScorer(const string &name)
    : Scorer(name)
{
    _strAttrName = "RAW_URL";
}

StringAttributeScorer::~StringAttributeScorer() { 
}

bool StringAttributeScorer::beginRequest(suez::turing::ScoringProvider *provider_){
    auto provider = dynamic_cast<ScoringProvider *>(provider_);
    _matchDataRef = provider->requireMatchData();
    _attrRef = provider->requireAttribute<MultiChar>(_strAttrName);
    return _matchDataRef && _attrRef;
}

score_t StringAttributeScorer::score(matchdoc::MatchDoc &matchDoc) {
    MultiChar multiChar;
    multiChar = _attrRef->get(matchDoc);
    const char *c = multiChar.data();
    uint32_t len = multiChar.size();
    if (len == 0) {
        HA3_LOG(DEBUG, "Miss string attribute [%s]. docid[%d]", 
            _strAttrName.c_str(), matchDoc.getDocId());
    } else {
        stringstream ss;
        for (uint32_t i = 0u; i < len; i++) {
            ss << c[i];
        }
        HA3_LOG(DEBUG, "Fetch string attribute [%s]. content[%s], docid[%d]", 
            _strAttrName.c_str(), ss.str().c_str(), matchDoc.getDocId());
    }
    return 0.0f;
}

Scorer* StringAttributeScorer::clone() {
    return new StringAttributeScorer(*this);
}

void StringAttributeScorer::endRequest() {
}

void StringAttributeScorer::destroy() {
    delete this;
}
END_HA3_NAMESPACE(rank);

