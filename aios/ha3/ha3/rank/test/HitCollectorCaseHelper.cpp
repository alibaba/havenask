#include <ha3/rank/test/HitCollectorCaseHelper.h>
#include <autil/StringUtil.h>
#include <autil/StringTokenizer.h>
#include <ha3/common/Ha3MatchDocAllocator.h>

using namespace std;
using namespace autil;
USE_HA3_NAMESPACE(common);
USE_HA3_NAMESPACE(search);

BEGIN_HA3_NAMESPACE(rank);
HA3_LOG_SETUP(rank, HitCollectorCaseHelper);

HitCollectorCaseHelper::HitCollectorCaseHelper() {
}

HitCollectorCaseHelper::~HitCollectorCaseHelper() {
}

void HitCollectorCaseHelper::makeData(
        HitCollectorBase *hitCollector,
        const string &matchDocStr,
        const vector<FakeRankExpression*> &attrExprs,
        const common::Ha3MatchDocAllocatorPtr &allocatorPtr)
{
    common::Ha3MatchDocAllocator *allocator
        = dynamic_cast<common::Ha3MatchDocAllocator*>(allocatorPtr.get());
    vector<const matchdoc::Reference<score_t> *> refs;
    for (size_t i = 0; i < attrExprs.size(); ++i) {
        refs.push_back(attrExprs[i]->getReference());
    }
    StringTokenizer st(matchDocStr, ";", StringTokenizer::TOKEN_IGNORE_EMPTY
                       | StringTokenizer::TOKEN_TRIM);
    for (size_t i = 0; i < st.getNumTokens(); ++i) {
        StringTokenizer st2(st[i], ",", StringTokenizer::TOKEN_IGNORE_EMPTY
                           | StringTokenizer::TOKEN_TRIM);
        assert(st2.getNumTokens() >= 2);
        docid_t docId = StringUtil::fromString<docid_t>(st2[0].c_str());
        matchdoc::MatchDoc matchDoc = allocator->allocate(docId);
        for (size_t i = 0; i < refs.size(); ++i) {
            float score = StringUtil::fromString<float>(st2[i+1].c_str());
            refs[i]->set(matchDoc, score);
        }
        if (st2.getNumTokens() == refs.size() + 2 && st2[refs.size() + 1] == "d") {
            matchDoc.setDeleted();
        }
        hitCollector->collect(matchDoc);
    }
}

void HitCollectorCaseHelper::checkPop(DistinctHitCollector *hitCollector, const string &popOpteratorStr,
                                      const common::Ha3MatchDocAllocatorPtr &allocatorPtr)
{
    StringTokenizer st(popOpteratorStr, ",", StringTokenizer::TOKEN_IGNORE_EMPTY
                       | StringTokenizer::TOKEN_TRIM);
    for (size_t i = 0; i < st.getNumTokens(); ++i) {
        docid_t docId = StringUtil::fromString<docid_t>(st[i].c_str());
        matchdoc::MatchDoc item = hitCollector->popItem();
        if (docId == INVALID_DOCID) {
            assert(matchdoc::INVALID_MATCHDOC == item);
        } else {
            assert(matchdoc::INVALID_MATCHDOC != item);
            assert(docId == item.getDocId());
            allocatorPtr->deallocate(item);
        }
    }
}

class DocIdComp {
public:
    bool operator() (matchdoc::MatchDoc a, matchdoc::MatchDoc b) {
        return a.getDocId() < b.getDocId();
    }
};

void HitCollectorCaseHelper::checkLeftMatchDoc(HitCollectorBase *hitCollector,
        const std::string &popOpteratorStr,
        const common::Ha3MatchDocAllocatorPtr &allocatorPtr)
{
    hitCollector->flush();
    StringTokenizer st(popOpteratorStr, ",", StringTokenizer::TOKEN_IGNORE_EMPTY
                       | StringTokenizer::TOKEN_TRIM);
    assert((uint32_t)st.getNumTokens() == hitCollector->getItemCount());
    autil::mem_pool::PoolVector<matchdoc::MatchDoc> matchDocs(hitCollector->getPool());
    hitCollector->stealAllMatchDocs(matchDocs);
    assert(uint32_t(0) == hitCollector->getItemCount());
    assert(st.getNumTokens() == matchDocs.size());
    sort(matchDocs.begin(), matchDocs.end(), DocIdComp());
    vector<docid_t> docIdVec;
    for (size_t i = 0; i < st.getNumTokens(); ++i) {
        docIdVec.push_back(StringUtil::fromString<docid_t>(st[i].c_str()));
    }
    sort(docIdVec.begin(), docIdVec.end());
    for (size_t i = 0; i < docIdVec.size(); ++i) {
        assert(docIdVec[i] == matchDocs[i].getDocId());
        allocatorPtr->deallocate(matchDocs[i]);
    }
}

END_HA3_NAMESPACE(rank);
