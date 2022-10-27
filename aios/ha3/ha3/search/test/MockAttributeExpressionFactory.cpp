#include <ha3/search/test/MockAttributeExpressionFactory.h>
#include <suez/turing/expression/framework/AtomicAttributeExpression.h>
#include <indexlib/index/normal/attribute/accessor/attribute_iterator_typed.h>
#include <indexlib/index/normal/attribute/accessor/attribute_reader.h>
#include <indexlib/index/normal/primarykey/primary_key_index_reader.h>

using namespace std;
using namespace std::tr1;
using namespace autil;
using namespace suez::turing;
IE_NAMESPACE_USE(index);
IE_NAMESPACE_USE(partition);

USE_HA3_NAMESPACE(common);

BEGIN_HA3_NAMESPACE(search);
HA3_LOG_SETUP(search, MockAttributeExpressionFactory);

MockAttributeExpressionFactory::MockAttributeExpressionFactory(
        const IndexPartitionReaderWrapperPtr &idxPartReaderWrapperPtr,
        JoinDocIdConverterCreator *docIdConverterFactory,
        matchdoc::Reference<matchdoc::MatchDoc> *subDocRef,
        autil::mem_pool::Pool *pool)
    : AttributeExpressionFactory("", NULL, NULL, NULL, NULL)
    , _idxPartReaderWrapperPtr(idxPartReaderWrapperPtr)
    , _docIdConverterFactory(docIdConverterFactory)
    , _subDocRef(subDocRef)
    , _pool(pool)
{
}

suez::turing::AttributeExpression *MockAttributeExpressionFactory::createExpressionForPKAttr(
        const IndexPartitionReaderPtr &indexPartReader)
{
    PrimaryKeyIndexReaderPtr pkIndexReaderPtr = indexPartReader->GetPrimaryKeyReader();
    if (!pkIndexReaderPtr) {
        return NULL;
    }

    DefaultDocIdAccessor docIdAccessor;

    return createExpressionWithReader<DefaultDocIdAccessor>(
            pkIndexReaderPtr->GetPKAttributeReader(),
            PRIMARYKEY_REF, docIdAccessor);
}

suez::turing::AttributeExpression* MockAttributeExpressionFactory::createAtomicExpr(
        const string &attributeName)
{
    if (attributeName == PRIMARYKEY_REF) {
        return createExpressionForPKAttr(_idxPartReaderWrapperPtr->getReader());
    }
    AttributeMetaInfo attrMetaInfo;
    bool ret = _idxPartReaderWrapperPtr->getAttributeMetaInfo(attributeName, attrMetaInfo);
    if (!ret) {
        HA3_LOG(DEBUG, "attribute [%s] does not exist", attributeName.c_str());
        return NULL;
    }
    AttributeType attrType = attrMetaInfo.getAttributeType();
    if (attrType == AT_MAIN_ATTRIBUTE) {
        return createNormalAttrExpr(attrMetaInfo);
    } else if (attrType == AT_SUB_ATTRIBUTE) {
        return createSubAttrExpr(attrMetaInfo);
    } else {
        assert(false);
        return NULL;
    }
}

suez::turing::AttributeExpression* MockAttributeExpressionFactory::createNormalAttrExpr(
        const AttributeMetaInfo &attrMetaInfo)
{
    const IndexPartitionReaderPtr& indexPartReader =
        attrMetaInfo.getIndexPartReader();
    const string& attrName = attrMetaInfo.getAttributeName();
    AttributeReaderPtr attrReaderPtr = indexPartReader->GetAttributeReader(attrName);
    DefaultDocIdAccessor docIdAccessor;
    return createExpressionWithReader<DefaultDocIdAccessor>(
            attrReaderPtr, attrName, docIdAccessor);
}

suez::turing::AttributeExpression* MockAttributeExpressionFactory::createJoinAttrExpr(
        const AttributeMetaInfo &attrMetaInfo)
{
    // TODO
    return NULL;
    // assert(_docIdConverterFactory);
    // JoinDocIdConverterBase* converter =
    //     _docIdConverterFactory->createJoinDocIdConverter(attrMetaInfo);
    // if (!converter) {
    //     HA3_LOG(WARN, "create JoinDocIdConverter for join field [%s] failed!",
    //             attrMetaInfo.getJoinFieldName().c_str());
    //     return NULL;
    // }

    // JoinDocIdAccessor docIdAccessor(converter);
    // AttributeReaderPtr joinAttrReaderPtr = attrMetaInfo.getJoinAttributeReader();
    // assert(joinAttrReaderPtr);
    // const string &attrName = attrMetaInfo.getAttributeName();
    // auto expr = createExpressionWithReader<JoinDocIdAccessor>(
    //         joinAttrReaderPtr, attrName, docIdAccessor);
    // expr->setIsSubExpression(attrMetaInfo.isSubJoin());
    // return expr;
}

suez::turing::AttributeExpression* MockAttributeExpressionFactory::createSubAttrExpr(
        const AttributeMetaInfo &attrMetaInfo)
{
    const string& attrName = attrMetaInfo.getAttributeName();
    if (_subDocRef == NULL) {
        HA3_LOG(WARN, "create sub attribute expression [%s] is not allow",
                attrName.c_str());
        return NULL;
    }
    const IndexPartitionReaderPtr& indexPartReader =
        attrMetaInfo.getIndexPartReader();
    AttributeReaderPtr attrReaderPtr = indexPartReader->GetAttributeReader(attrName);
    SubDocIdAccessor subDocIdAccessor(_subDocRef);
    auto expr = createExpressionWithReader<SubDocIdAccessor>(
            attrReaderPtr, attrName, subDocIdAccessor);
    expr->setIsSubExpression(true);
    return expr;
}

template <typename DocIdAccessor>
suez::turing::AttributeExpression* MockAttributeExpressionFactory::createExpressionWithReader(
        const AttributeReaderPtr &attrReaderPtr,
        const string &attrName,
        const DocIdAccessor &docIdAccessor)
{
    if (!attrReaderPtr.get()) {
        return NULL;
    }

    _attrReaderHolderVec.emplace_back(attrReaderPtr);

    suez::turing::AttributeExpression *expr = NULL;
    AttrType attrType = attrReaderPtr->GetType();
    auto vt = attrType2VariableType(attrType);
    bool isMulti = attrReaderPtr->IsMultiValue();

#define ATTR_TYPE_CASE_HELPER(vt)                                       \
    case vt:                                                            \
        if (isMulti) {                                                  \
            typedef VariableTypeTraits<vt, true>::AttrExprType AttrExprType; \
            expr = doCreateAtomicAttrExprTyped<AttrExprType, DocIdAccessor>( \
                    attrName, attrReaderPtr, docIdAccessor);            \
        } else {                                                        \
            typedef VariableTypeTraits<vt, false>::AttrExprType AttrExprType; \
            expr = doCreateAtomicAttrExprTyped<AttrExprType, DocIdAccessor>( \
                    attrName, attrReaderPtr, docIdAccessor);            \
        }                                                               \
        break

    switch (vt) {
        NUMERIC_VARIABLE_TYPE_MACRO_HELPER(ATTR_TYPE_CASE_HELPER);
        ATTR_TYPE_CASE_HELPER(vt_string);
        ATTR_TYPE_CASE_HELPER(vt_hash_128);
    default:
        HA3_LOG(WARN, "unsupport variable type[%d] for attribute", (int32_t)vt);
        break;
    }

#undef ATTR_TYPE_CASE_HELPER
    if (!expr) {
        HA3_LOG(WARN, "create AtomicAttributeExpression error, "
                "attribute name [%s]", attrName.c_str());
    }

    return expr;
}

template<typename T, typename DocIdAccessor>
suez::turing::AttributeExpression* MockAttributeExpressionFactory::doCreateAtomicAttrExprTyped(
        const string &attrName,
        const AttributeReaderPtr &attReaderPtr,
        const DocIdAccessor &docIdAccessor)
{
    typedef AtomicAttributeExpression<T, DocIdAccessor> AtomicAttributeExpressionTyped;
    typedef typename AtomicAttributeExpression<T, DocIdAccessor>::Iterator Iterator;
    AttributeIteratorBase *attrIterBase = attReaderPtr->CreateIterator(_pool);
    assert(attrIterBase);
    Iterator *attrIter = dynamic_cast<Iterator*>(attrIterBase);
    assert(attrIter != NULL);
    if (attrIter) {
        return POOL_NEW_CLASS(_pool, AtomicAttributeExpressionTyped,
                              attrName, attrIter, docIdAccessor);
    }
    return NULL;
}

template<typename T>
suez::turing::AttributeExpression* MockAttributeExpressionFactory::doCreateAttributeExpression(
        matchdoc::ReferenceBase *refer,
        autil::mem_pool::Pool *pool)
{
    auto vr = dynamic_cast<matchdoc::Reference<T> *>(refer);
    assert(vr);
    auto expr = POOL_NEW_CLASS(pool, AttributeExpressionTyped<T>);
    expr->setReference(vr);
    return expr;
}


suez::turing::AttributeExpression* MockAttributeExpressionFactory::createAttributeExpression(
        matchdoc::ReferenceBase *refer, autil::mem_pool::Pool *pool)
{
    auto valueType = refer->getValueType();
    auto vt = valueType.getBuiltinType();
    auto isMulti = valueType.isMultiValue();

#define CREATE_ATTRIBUTE_EXPRESSION_HELPER(vt_type)                     \
    case vt_type:                                                       \
    {                                                                   \
        if (isMulti) {                                                  \
            typedef VariableTypeTraits<vt_type, true>::AttrItemType T; \
            return doCreateAttributeExpression<T>(refer, pool);         \
        } else {                                                        \
            typedef VariableTypeTraits<vt_type, false>::AttrItemType T; \
            return doCreateAttributeExpression<T>(refer, pool);         \
        }                                                               \
    }                                                                   \
    break

    switch(vt) {
    NUMERIC_VARIABLE_TYPE_MACRO_HELPER_WITH_BOOL(
    CREATE_ATTRIBUTE_EXPRESSION_HELPER);
    default:
        assert(false);
        break;
    }
    return NULL;
}

END_HA3_NAMESPACE(search);
