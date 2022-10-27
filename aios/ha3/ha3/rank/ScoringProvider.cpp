#include <ha3/rank/ScoringProvider.h>
#include <suez/turing/expression/framework/AttributeExpressionCreator.h>
#include <suez/turing/expression/framework/ExpressionEvaluator.h>
#include <assert.h>
#include <string>

using namespace std;
using namespace suez::turing;
USE_HA3_NAMESPACE(search);
USE_HA3_NAMESPACE(common);
USE_HA3_NAMESPACE(index);
USE_HA3_NAMESPACE(inverted_index);
USE_HA3_NAMESPACE(config);

BEGIN_HA3_NAMESPACE(rank);
HA3_LOG_SETUP(rank, ScoringProvider);

ScoringProvider::ScoringProvider(const RankResource &rankResource)
    : suez::turing::ScoringProvider(rankResource.matchDocAllocator.get(),
                                    rankResource.pool,
                                    rankResource.cavaAllocator,
                                    rankResource.requestTracer,
                                    rankResource.partitionReaderSnapshot,
                                    rankResource.kvpairs)
    , _rankResource(rankResource)
{
    auto creator = dynamic_cast<AttributeExpressionCreator *>(rankResource.attrExprCreator);
    if (creator) {
        setAttributeExpressionCreator(creator);
    }
    _totalMatchDocs = 0;
    setSearchPluginResource(&_rankResource);
    setupTraceRefer(convertRankTraceLevel(getRequest()));
}

ScoringProvider::~ScoringProvider() {
}

SectionReaderWrapperPtr
ScoringProvider::getSectionReader(const string &indexName) const {
    if (NULL == _rankResource.indexReaderPtr.get()) {
        SectionReaderWrapperPtr nullWrapperPtr;
        return nullWrapperPtr;
    }

    const SectionAttributeReader* attrReader
        = _rankResource.indexReaderPtr->GetSectionReader(indexName);
    if (NULL == attrReader) {
        HA3_LOG(WARN, "not found the SectionAttributeReader, indexName:%s",
            indexName.c_str());
        SectionReaderWrapperPtr nullWrapperPtr;
        return nullWrapperPtr;
    }
    SectionReaderWrapperPtr ptr(new SectionReaderWrapper(attrReader));
    return ptr;
}

void ScoringProvider::prepareMatchDoc(matchdoc::MatchDoc matchDoc) {
    ExpressionEvaluator<ExpressionVector> evaluator(getNeedEvaluateAttrs(),
            getAllocator()->getSubDocAccessor());
    evaluator.evaluateExpressions(matchDoc);
    prepareTracer(matchDoc);
}

END_HA3_NAMESPACE(rank);
