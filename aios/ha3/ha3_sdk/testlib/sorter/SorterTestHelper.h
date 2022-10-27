#ifndef ISEARCH_SORTERTESTHELPER_H
#define ISEARCH_SORTERTESTHELPER_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/sorter/SorterResource.h>
#include <ha3/sorter/Sorter.h>
#include <ha3/rank/MatchData.h>
#include <ha3/common/MultiErrorResult.h>
#include <ha3_sdk/testlib/common/FakeIndexParam.h>
#include <ha3_sdk/testlib/common/TestHelperBase.h>
#include <suez/turing/expression/plugin/SorterWrapper.h>

BEGIN_HA3_NAMESPACE(sorter);

class SorterTestHelper : public common::TestHelperBase
{
public:
    SorterTestHelper(const std::string &testPath);
    ~SorterTestHelper();
private:
    SorterTestHelper(const SorterTestHelper &);
    SorterTestHelper& operator=(const SorterTestHelper &);
public:
    bool init(common::TestParamBase *param);
    bool beginRequest(Sorter *sorter);
    bool beginSort(Sorter *sorter);
    bool sort(Sorter *sorter);
    bool endSort(Sorter *sorter);
    bool endRequest(Sorter *sorter);
public:
    suez::turing::AttributeExpression* getScoreAttributeExpression() const {
        return _scoreExpression;
    }
    SorterProvider* getSorterProvider() const {
        return _provider;
    }
    common::SorterTestParam* getSorterTestParam() {
        return &_param;
    }
private:
    bool initScoreAttrExpression();
    SorterLocation fromString(const std::string &locationStr);
    template <typename T>
    bool evaluateScoreAttrExpr(
            suez::turing::AttributeExpression *attrExpr,
            const T &matchDocs);
protected:
    suez::turing::AttributeExpression *_scoreExpression;
    rank::ComparatorCreator *_cmpCreator;
    search::SortExpressionCreator *_sortExprCreator;
    const common::MultiErrorResultPtr _errorResult;

    HA3_NS(sorter)::SorterProvider *_provider;
    common::SorterTestParam _param;
    suez::turing::SorterWrapperPtr _sorterWrap;
private:
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(SorterTestHelper);

END_HA3_NAMESPACE(sorter);

#endif //ISEARCH_SORTERTESTHELPER_H
