#pragma once

#include "ha3/search/Aggregator.h"
#include <suez/turing/expression/framework/VariableTypeTraits.h>
#include <ha3_sdk/testlib/search/FakeAttributeExpressionMaker.h>

// USE_HA3_NAMESPACE(index);

BEGIN_HA3_NAMESPACE(search);

class AggregateBase {
public:
    AggregateBase();
    ~AggregateBase();
public:
    void baseSetUp();
    void baseTearDown();
public:
    void clear();
    void resetExpression();
    template <typename T>
    suez::turing::AttributeExpressionTyped<T>* initKeyExpression(const std::string &name);
    template <typename T>
    suez::turing::AttributeExpressionTyped<T>* initFunExpression(const std::string &name);
    template <typename T>
    suez::turing::AttributeExpressionTyped<T>* initFunExpressionWithFakeData(const std::string &name,
            const std::vector<T> &fakeData);
    template <typename T>
    suez::turing::AttributeExpressionTyped<autil::MultiValueType<T> >*
    initMultiValueKeyExpression(const std::string &name);

    void aggMatchDoc(int32_t docNum, Aggregator *agg);
    void aggSubMatchDoc(const std::string &subDocNumStr, Aggregator *agg);
    template <typename T>
    void makeFakeData_fun(std::vector<T> &fakeData, int count);
    template <typename T>
    void makeFakeData_key(std::vector<T> &fakeData, int count, int mod = 2);
    template <typename T>
    void makeFakeMutiValueData_key(std::vector<std::vector<T> > &fakeData, int count);

protected:
    suez::turing::AttributeExpression *_keyAttributeExpr;
    suez::turing::AttributeExpression *_funAttributeExpr;
    suez::turing::AttributeExpressionPool *_attrExprPool;
    autil::mem_pool::Pool *_pool;
    const int32_t DOC_NUM = 10;
    common::Ha3MatchDocAllocator *_mdAllocator;
    std::vector<matchdoc::MatchDoc> _mdVector;
    std::shared_ptr<FakeAttributeExpressionMaker> _fakeAttrExprMaker;
};

template <typename T>
suez::turing::AttributeExpressionTyped<T>* AggregateBase::initKeyExpression(
        const std::string &name)
{


    std::vector<T> fakeData;
    makeFakeData_key(fakeData, DOC_NUM);//make fake key data
    suez::turing::AttributeExpressionTyped<T> *expr
        = _fakeAttrExprMaker->createAttrExpr<T>(name,
                autil::StringUtil::toString(fakeData, ","));
    _attrExprPool->addNeedDeleteExpr(expr);
    return expr;
}

template <typename T>
suez::turing::AttributeExpressionTyped<T>*
AggregateBase::initFunExpression(const std::string &name)
{


    std::vector<T> fakeData;
    makeFakeData_fun(fakeData, DOC_NUM);//make fake function data
    suez::turing::AttributeExpressionTyped<T> *expr
        = _fakeAttrExprMaker->createAttrExpr<T>(name,
                autil::StringUtil::toString(fakeData, ","));
    _attrExprPool->addNeedDeleteExpr(expr);
    return expr;
}

template <typename T>
suez::turing::AttributeExpressionTyped<T>*
AggregateBase::initFunExpressionWithFakeData(
        const std::string &name , const std::vector<T> &fakeData)
{


    suez::turing::AttributeExpressionTyped<T> *expr =
        _fakeAttrExprMaker->createAttrExpr<T>(name,
                autil::StringUtil::toString(fakeData, ","));
    _attrExprPool->addNeedDeleteExpr(expr);
    return expr;
}

template <typename T>
void AggregateBase::makeFakeData_fun(std::vector<T> &fakeData, int count)
{
    for (int i = 0; i < count; i++) {
        fakeData.push_back((T)i);
    }
}

template <typename T>
void AggregateBase::makeFakeData_key(std::vector<T> &fakeData, int count, int mod)
{
    for (int i = 0; i < count; i++) {
        fakeData.push_back((T)(i % mod));
    }
}

template <typename T>
void AggregateBase::makeFakeMutiValueData_key(std::vector<std::vector<T> > &fakeData, int count) {
    const int recordPerDoc = 3;
    for (int i = 0; i < count; i++) {
        std::vector<T> records;
        for (int j = 0; j < recordPerDoc; j++) {
            records.push_back(j);
        }
        fakeData.push_back(records);
    }
}

template <typename T>
suez::turing::AttributeExpressionTyped<autil::MultiValueType<T> >*
AggregateBase::initMultiValueKeyExpression(const std::string &name)
{
    std::vector<std::vector<T> > fakeData;
    makeFakeMutiValueData_key(fakeData, DOC_NUM);//make fake key data

    suez::turing::AttributeExpressionTyped<autil::MultiValueType<T> > *expr =
        _fakeAttrExprMaker->createMultiValueAttrExpr<T>(
                name, autil::StringUtil::toString(fakeData, ",", "#"));

    _attrExprPool->addNeedDeleteExpr(expr);
    return expr;
}

END_HA3_NAMESPACE();
