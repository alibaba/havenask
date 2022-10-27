#ifndef ISEARCH_SCORINGPROVIDER_H
#define ISEARCH_SCORINGPROVIDER_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <string>
#include <assert.h>
#include <ha3/common/Request.h>
#include <ha3/search/QueryExecutor.h>
#include <matchdoc/MatchDoc.h>
#include <ha3/rank/GlobalMatchData.h>
#include <ha3/config/IndexInfoHelper.h>
#include <indexlib/index/normal/inverted_index/accessor/section_attribute_reader.h>
#include <indexlib/index/normal/inverted_index/accessor/index_reader.h>
#include <ha3/index/SectionReaderWrapper.h>
#include <ha3/search/RankResource.h>
#include <map>
#include <ha3/common/Tracer.h>
#include <ha3/common/AttributeItem.h>
#include <ha3/common/AggregateResult.h>
#include <ha3/common/AggResultReader.h>
#include <ha3/search/ProviderBase.h>
#include <suez/turing/expression/util/FieldBoostTable.h>
#include <suez/turing/expression/provider/ScoringProvider.h>

namespace ha3 {
class ScorerProvider;
};

BEGIN_HA3_NAMESPACE(rank);

class ScoringProvider : public suez::turing::ScoringProvider,
                        public search::ProviderBase
{
public:
    typedef std::vector<suez::turing::AttributeExpression*> AttributeVector;
public:
    ScoringProvider(const search::RankResource &rankResource);
    ~ScoringProvider();
public:
    template<typename T>
    T *declareGlobalVariable(const std::string &variName,
                             bool needSerizlize = false);
    const config::IndexInfoHelper *getIndexInfoHelper() const {
        return _rankResource.indexInfoHelper;
    }
    const suez::turing::FieldBoostTable *getFieldBoostTable() const {
        return _rankResource.boostTable;
    }
    void setFieldBoostTable(const suez::turing::FieldBoostTable *fieldBoostTable) {
        _rankResource.boostTable = fieldBoostTable;
    }
    void setIndexReader(const index::IndexReaderPtr &indexReaderPtr) {
        _rankResource.indexReaderPtr = indexReaderPtr;
    }
    index::SectionReaderWrapperPtr getSectionReader(const std::string &indexName = "") const;
    void setTotalMatchDocs(uint32_t v) { _totalMatchDocs = v; }
    uint32_t getTotalMatchDocs() const { return _totalMatchDocs; }
    suez::turing::SuezCavaAllocator *getCavaAllocator() {
        return _rankResource.cavaAllocator;
    }
    // for test
    void prepareMatchDoc(matchdoc::MatchDoc matchDoc);
private:
    search::RankResource _rankResource;
    uint32_t _totalMatchDocs;
private:
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(ScoringProvider);

template<typename T>
T *ScoringProvider::declareGlobalVariable(const std::string &variName,
        bool needSerizlize)
{
    _declareVariable = true;
    return doDeclareGlobalVariable<T>(variName, needSerizlize);
}

END_HA3_NAMESPACE(rank);

#endif //ISEARCH_SCORINGPROVIDER_H
