#ifndef ISEARCH_TESTHELPERBASE_H
#define ISEARCH_TESTHELPERBASE_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/rank/MatchData.h>
#include <ha3/rank/SimpleMatchData.h>
#include <suez/turing/test/IndexWrap.h>
#include <ha3_sdk/testlib/common/FakeIndexParam.h>
#include <ha3/common/Request.h>
#include <suez/turing/expression/framework/AttributeExpressionCreator.h>
#include <suez/turing/expression/framework/ExpressionEvaluator.h>
#include <ha3/common/Ha3MatchDocAllocator.h>
#include <ha3/common/DataProvider.h>
#include <ha3/search/SearchPluginResource.h>

BEGIN_HA3_NAMESPACE(common);

class TestHelperBase
{
public:
    TestHelperBase(const std::string &testPath);
    virtual ~TestHelperBase();
private:
    TestHelperBase(const TestHelperBase &);
    TestHelperBase& operator=(const TestHelperBase &);
public:
    virtual bool init(TestParamBase *param);
    void fillSearchPluginResource(HA3_NS(search)::SearchPluginResource *resource);
    const HA3_NS(common)::RequestPtr& getRequest() const {
        return _requestPtr;
    }
    suez::turing::AttributeExpressionCreatorBase* getAttrExprCreator() const {
        return _attrExprCreator;
    }
    const std::vector<matchdoc::MatchDoc>& getMatchDocs() const {
        return _matchDocs;
    }
    HA3_NS(common)::Ha3MatchDocAllocatorPtr getMatchDocAllocator() const {
        return _mdAllocatorPtr;
    }
    void allocateMatchDocs(const std::string &docIdStr);
    void prepareForAttr(std::vector<matchdoc::MatchDoc> &matchDocs) {
        suez::turing::ExpressionEvaluator<suez::turing::ExpressionVector> evaluator(
                _providerBase->getNeedEvaluateAttrs(),
                _providerBase->getAllocator()->getSubDocAccessor());
        evaluator.batchEvaluateExpressions(matchDocs.data(), matchDocs.size());
    }

    autil::mem_pool::Pool * getPool() {
        return _pool;
    }
protected:
    void requireMatchData(HA3_NS(common)::TestParamBase *param);
    bool fillMatchDatas(const std::string &inputStr, 
                        const std::vector<matchdoc::MatchDoc> &matchDocs);
    bool fillSimpleMatchDatas(const std::string &inputStr, 
                              const std::vector<matchdoc::MatchDoc> &matchDocs);
    void allocateMatchDocs(const std::string &docIdStr, 
                           std::vector<matchdoc::MatchDoc> &matchDocVec);
    void deallocateMatchDocs(const std::vector<matchdoc::MatchDoc> &matchDocs);
protected:
    std::string _testPath;
    suez::turing::InvertedTableParam _invertParam;
    suez::turing::IndexWrapPtr _indexWrap;
    IE_NAMESPACE(partition)::IndexPartitionReaderPtr _indexPartReader;
    autil::mem_pool::Pool *_pool;
    HA3_NS(common)::RequestPtr _requestPtr;
    HA3_NS(common)::Ha3MatchDocAllocatorPtr _mdAllocatorPtr;
    std::vector<matchdoc::MatchDoc> _matchDocs;
    suez::turing::AttributeExpressionCreatorBase *_attrExprCreator;
    matchdoc::Reference<HA3_NS(rank)::MatchData> *_matchDataRef;
    matchdoc::Reference<HA3_NS(rank)::SimpleMatchData> *_simpleMatchDataRef;
    IE_NAMESPACE(partition)::PartitionReaderSnapshotPtr _partitionReaderSnapshot;
    suez::turing::ProviderBase *_providerBase;
    suez::turing::FunctionProviderPtr _functionProvider;
    HA3_NS(common)::Tracer* _requestTracer;
    suez::turing::VirtualAttributes _virtualAttributes;
    HA3_NS(common)::DataProvider *_dataProvider;
private:
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(TestHelperBase);

END_HA3_NAMESPACE(common);

#endif //ISEARCH_TESTHELPERBASE_H
