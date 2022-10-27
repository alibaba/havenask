#ifndef ISEARCH_FAKEATTRIBUTEEXPRESSIONMAKER_H
#define ISEARCH_FAKEATTRIBUTEEXPRESSIONMAKER_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <suez/turing/expression/framework/AttributeExpression.h>
#include <ha3_sdk/testlib/index/FakeAttributeReader.h>
#include <ha3_sdk/testlib/index/FakeMultiValueAttributeReader.h>
#include <suez/turing/expression/framework/AtomicAttributeExpression.h>
#include <suez/turing/expression/framework/ConstAttributeExpression.h>
#include <suez/turing/expression/framework/ArgumentAttributeExpression.h>
#include <autil/StringUtil.h>

BEGIN_HA3_NAMESPACE(search);

class FakeAttributeExpressionMaker
{
public:
    FakeAttributeExpressionMaker(autil::mem_pool::Pool *pool = NULL) {
        _pool = pool;
        _ownPool = false;
        if (_pool == NULL) {
            _pool = new autil::mem_pool::Pool;
            _ownPool = true;
        }
    }
    virtual ~FakeAttributeExpressionMaker() {
        if (_ownPool) {
            delete _pool;
            _pool = NULL;
        }
    }
public:
    void deleteAttrExpr(suez::turing::AttributeExpression *attrExpr) {
        POOL_DELETE_CLASS(attrExpr);
    }

    // fakeDataStr format:
    // attrVal1,attrVal2,....
    template <typename T>
    suez::turing::AttributeExpressionTyped<T>* createAttrExpr(const std::string &attrName,
            const std::string &fakeDataStr)
    {
        auto *reader = new index::FakeAttributeReader<T>();
        _attrReaderHolderVec.push_back(index::AttributeReaderPtr(reader));
        reader->setAttributeValues(fakeDataStr);

        index::AttributeIteratorBase* iterator = reader->CreateIterator(_pool);
        typename suez::turing::AtomicAttributeExpression<T>::Iterator* it
            = dynamic_cast<index::AttributeIteratorTyped<T>* >(iterator);
        suez::turing::AttributeExpressionTyped<T> *expr = POOL_NEW_CLASS(_pool,
                suez::turing::AtomicAttributeExpression<T>, attrName, it);
        return expr;
    }

    // T is base data type, such as char, int
    // multiValue data format:
    //    val11,val12#val21,val22#val31,val32,val33...
    template <typename T>
    suez::turing::AttributeExpressionTyped<autil::MultiValueType<T> >* createMultiValueAttrExpr(
            const std::string &attrName,
            const std::string &fakeDataStr)
    {
        auto *reader = new index::FakeMultiValueAttributeReader<T>();
        _attrReaderHolderVec.emplace_back(reader);
        reader->setAttributeValues(fakeDataStr);

        index::AttributeIteratorBase* iterator = reader->CreateIterator(_pool);
        auto it = dynamic_cast<index::AttributeIteratorTyped<autil::MultiValueType<T> >* >(iterator);
        suez::turing::AttributeExpressionTyped<autil::MultiValueType<T> > *expr =
            POOL_NEW_CLASS(_pool, suez::turing::AtomicAttributeExpression<autil::MultiValueType<T> >,
                           attrName, it);
        return expr;
    }

    // data format for string attribute:
    // string1,string2,string3,....
    suez::turing::AttributeExpressionTyped<autil::MultiValueType<char> >* createStringAttrExpr(
            const std::string &attrName,
            const std::string &fakeDataStr)
    {
        auto *reader = new index::FakeStringAttributeReader();
        _attrReaderHolderVec.push_back(index::AttributeReaderPtr(reader));

        reader->setAttributeValues(fakeDataStr);

        index::AttributeIteratorBase* iterator = reader->CreateIterator(_pool);
        suez::turing::AtomicAttributeExpression<autil::MultiValueType<char> >::Iterator* it
            = dynamic_cast<index::AttributeIteratorTyped<autil::MultiValueType<char> >* >(iterator);
        suez::turing::AttributeExpressionTyped<autil::MultiValueType<char> > *expr =
            POOL_NEW_CLASS(_pool, suez::turing::AtomicAttributeExpression<autil::MultiValueType<char> >,
                           attrName, it);
        return expr;
    }

    template <typename T>
    suez::turing::AttributeExpressionTyped<T>* createConstAttrExpr(const std::string &value) {
        T exprValue = autil::StringUtil::numberFromString<T>(value);
        suez::turing::AttributeExpressionTyped<T> *ret = POOL_NEW_CLASS(_pool,
                suez::turing::ConstAttributeExpression<T>,
                exprValue);
        return ret;
    }

    suez::turing::AttributeExpression* createArgumentAttrExpr(
            suez::turing::AtomicSyntaxExprType atomicSyntaxExprType,
            const std::string &name,
            const std::string &value)
    {
        suez::turing::AttributeExpression* ret = POOL_NEW_CLASS(_pool,
                suez::turing::ArgumentAttributeExpression, atomicSyntaxExprType);
        ret->setOriginalString(value);
        return ret;
    }
private:
    autil::mem_pool::Pool *_pool;
    bool _ownPool;
    std::vector<index::AttributeReaderPtr> _attrReaderHolderVec;
private:
    HA3_LOG_DECLARE();
};


END_HA3_NAMESPACE(search);

#endif //ISEARCH_FAKEATTRIBUTEEXPRESSIONMAKER_H
