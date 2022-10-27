#pragma once

#include<unittest/unittest.h>
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <matchdoc/MatchDoc.h>
#include <ha3/search/AggregateFunction.h>
#include <matchdoc/Reference.h>
#include <ha3/search/AttributeExpression.h>
#include <ha3/search/NormalAggregator.h>
#include <ha3/common/Ha3MatchDocAllocator.h>
#include <ha3_sdk/testlib/search/FakeAttributeExpressionMaker.h>
#include <suez/turing/expression/framework/AttributeExpressionPool.h>
#include <autil/MultiValueType.h>

BEGIN_HA3_NAMESPACE(search);
class MatchDocAllocator;
class AggregateFunction;

class NormalAggregatorDenseMapTest : public TESTBASE {
public:
    NormalAggregatorDenseMapTest();
    ~NormalAggregatorDenseMapTest();
public:
    enum AggMode { AGG_NORMAL, AGG_BATCH };
    void setUp() override;
    void tearDown() override;
private:
    typedef typename DenseMapTraits<int32_t>::GroupMapType GroupInt32Map;
    typedef typename DenseMapTraits<int64_t>::GroupMapType GroupInt64Map;
    typedef typename DenseMapTraits<autil::MultiChar>::GroupMapType GroupMultiCharMap;
    typedef NormalAggregator<int32_t, int32_t, GroupInt32Map> NormalAggregatorDenseInt32;
    typedef NormalAggregator<int64_t, int64_t, GroupInt64Map> NormalAggregatorDenseInt64;
    typedef NormalAggregator<autil::MultiChar, autil::MultiChar, GroupMultiCharMap> NormalAggregatorDenseMultiChar;
    typedef BatchAggregator<int32_t, int32_t, GroupInt32Map> BatchAggregatorDenseInt32;
    typedef BatchAggregator<int64_t, int64_t, GroupInt64Map> BatchAggregatorDenseInt64;
    typedef BatchAggregator<autil::MultiChar, autil::MultiChar, GroupMultiCharMap> BatchAggregatorDenseMultiChar;
protected:
    template <typename T, typename T_KEY>
    void testAggregator(int32_t size);

    template <typename T>
    AttributeExpressionTyped<T>* initKeyExpression(const std::string &name);
    template <typename T>
    AttributeExpressionTyped<T>* initFunExpression(const std::string &name);
    template <typename T>
    AttributeExpressionTyped<T>* initFunExpressionWithFakeData(const std::string &name,
            const std::vector<T> &fakeData);

    template <typename T>
    AttributeExpressionTyped<autil::MultiValueType<T> >*
    initMultiValueKeyExpression(const std::string &name);


    template <typename T>
    void makeFakeData_fun(std::vector<T> &fakeData, int count);
    template <typename T>
    void makeFakeData_key(std::vector<T> &fakeData, int count, int mod = 2);
    template <typename T>
    void makeFakeMutiValueData_key(std::vector<std::vector<T> > &fakeData, int count);

    template <typename T, typename T_KEY>
    void checkResult_RT(NormalAggregator<T_KEY, T_KEY, typename DenseMapTraits<T_KEY>::GroupMapType >* agg,
                        const std::string &funName, T v1, T v2);
    template <typename T, typename T_KEY>
    void testAggregator_RT(int32_t size);

    void aggMatchDoc(int32_t docNum, Aggregator *agg);
    void aggSubMatchDoc(const std::string &subDocNumStr, Aggregator *agg);
protected:
    std::vector<matchdoc::MatchDoc> _mdVector;
    common::Ha3MatchDocAllocator *_mdAllocator;

    AttributeExpression *_keyAttributeExpr;
    AttributeExpression *_funAttributeExpr;

    suez::turing::AttributeExpressionPool *_attrExprPool;
    autil::mem_pool::Pool *_pool;
    int DOC_NUM;
    AggMode _mode;
    std::shared_ptr<FakeAttributeExpressionMaker> _fakeAttrExprMaker;
private:
    HA3_LOG_DECLARE();
};

END_HA3_NAMESPACE(search);
