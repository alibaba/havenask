#ifndef ISEARCH_MOCK_ATTRIBUTEEXPRESSIONFACTORY_H
#define ISEARCH_MOCK_ATTRIBUTEEXPRESSIONFACTORY_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/search/IndexPartitionReaderWrapper.h>
#include <suez/turing/expression/framework/JoinDocIdConverterCreator.h>
#include <suez/turing/expression/framework/AttributeExpressionFactory.h>
#include <suez/turing/expression/framework/DocIdAccessor.h>
#include <ha3/index/index.h>
#include <autil/mem_pool/Pool.h>

BEGIN_HA3_NAMESPACE(search);

class MockAttributeExpressionFactory : public suez::turing::AttributeExpressionFactory
{
public:
    MockAttributeExpressionFactory(
            const IndexPartitionReaderWrapperPtr &idxPartReaderWrapperPtr,
            suez::turing::JoinDocIdConverterCreator *docIdConverterFactory,
            matchdoc::Reference<matchdoc::MatchDoc> *subDocRef,
            autil::mem_pool::Pool *pool);
    virtual ~MockAttributeExpressionFactory() {}
public:
    /* virtual for FakeMockAttributeExpressionFactory */
    virtual suez::turing::AttributeExpression* createAtomicExpr(const std::string &attrName);
    static suez::turing::AttributeExpression* createAttributeExpression(
            matchdoc::ReferenceBase *refer, autil::mem_pool::Pool *pool);
private:
    template<typename T>
    static suez::turing::AttributeExpression* doCreateAttributeExpression(
            matchdoc::ReferenceBase *refer, autil::mem_pool::Pool *pool);

    suez::turing::AttributeExpression* createExpressionForPKAttr(
            const IE_NAMESPACE(partition)::IndexPartitionReaderPtr &indexPartReader);
    suez::turing::AttributeExpression* createNormalAttrExpr(const AttributeMetaInfo &attrMetaInfo);
    suez::turing::AttributeExpression* createJoinAttrExpr(const AttributeMetaInfo &attrMetaInfo);
    suez::turing::AttributeExpression* createSubAttrExpr(const AttributeMetaInfo &attrMetaInfo);
    template<typename T, typename DocIdAccessor>
    suez::turing::AttributeExpression* doCreateAtomicAttrExprTyped(
            const std::string &attrName,
            const index::AttributeReaderPtr &attrReaderPtr,
            const DocIdAccessor &docIdAccessor);
    template <typename DocIdAccessor>
    suez::turing::AttributeExpression* createExpressionWithReader(
            const index::AttributeReaderPtr &attributeReaderPtr,
            const std::string &attributeName,
            const DocIdAccessor &docIdAccessor);
private:
    IndexPartitionReaderWrapperPtr _idxPartReaderWrapperPtr;
    suez::turing::JoinDocIdConverterCreator *_docIdConverterFactory;
    matchdoc::Reference<matchdoc::MatchDoc> *_subDocRef;
    autil::mem_pool::Pool *_pool;
    std::vector<index::AttributeReaderPtr> _attrReaderHolderVec;
private:
    friend class MockAttributeExpressionFactoryTest;
private:
    HA3_LOG_DECLARE();
};

END_HA3_NAMESPACE(search);

#endif //ISEARCH_MOCK_ATTRIBUTEEXPRESSIONFACTORY_H
