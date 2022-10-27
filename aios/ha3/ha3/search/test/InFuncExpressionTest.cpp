#include<unittest/unittest.h>
#include <ha3/search/test/InFuncExpressionTest.h>
#include <ha3/search/ConstAttributeExpression.h>
#include <ha3/search/ArgumentAttributeExpression.h>

BEGIN_HA3_NAMESPACE(search);
HA3_LOG_SETUP(search, InFuncExpressionTest);


InFuncExpressionTest::InFuncExpressionTest() { 
}

InFuncExpressionTest::~InFuncExpressionTest() { 
}

void InFuncExpressionTest::setUp() { 
    HA3_LOG(DEBUG, "setUp!");
}

void InFuncExpressionTest::tearDown() { 
    HA3_LOG(DEBUG, "tearDown!");
}

void InFuncExpressionTest::testSimpleProcess() { 
    HA3_LOG(DEBUG, "Begin Test!");

    MatchDocAllocator *matchDocAllocator = new MatchDocAllocator(_pool);
    FunctionSubExprVecPtr funcSubExprVecPtr(new FunctionSubExprVec);
    funcSubExprVecPtr->push(new ConstAttributeExpression<int32_t>(101));
    ArgumentAttributeExpression *argExpr = new ArgumentAttributeExpression(STRING_VALUE);
    argExpr->setOriginalString("\"30|50|100\"");
    funcSubExprVecPtr->push(argExpr);

    ASSERT_TRUE(funcSubExprVecPtr->exprs[0]->init(matchDocAllocator));
    ASSERT_TRUE(funcSubExprVecPtr->exprs[1]->init(matchDocAllocator));

    BuildInFunctionFactory *factory = new BuildInFunctionFactory;
    KeyValueMap parameters1;
    ASSERT_TRUE( factory->init(parameters1));
    KeyValueMap parameters2;
    config::ResourceReaderPtr resourceReader;
    AttributeExpression* expr = factory->createFuncExpression("in", funcSubExprVecPtr, parameters2, resourceReader);

    ASSERT_TRUE(expr);
    ASSERT_TRUE(expr->init(matchDocAllocator));

    MatchDoc *matchDoc = matchDocAllocator->allocateMatchDoc(0);
    
    ASSERT_TRUE(expr->evaluate(matchDoc));
    AttributeExpressionTyped<bool>* typedExpr = 
        dynamic_cast<AttributeExpressionTyped<bool> * >(expr);
    ASSERT_TRUE(typedExpr);
    ASSERT_TRUE(!typedExpr->getValue());
    matchDocAllocator->deallocateMatchDoc(matchDoc);
    delete expr;
    delete funcSubExprVecPtr->exprs[0];
    delete funcSubExprVecPtr->exprs[1];

    factory->destroy();
    delete matchDocAllocator;
}


template<typename T> 
void InFuncExpressionTest::checkDifferentType(const std::string &str, T value, bool expectedValue){
    MatchDocAllocator *matchDocAllocator = new MatchDocAllocator(_pool);
    FunctionSubExprVecPtr funcSubExprVecPtr(new FunctionSubExprVec);
    funcSubExprVecPtr->push(new ConstAttributeExpression<T>(value));
    ArgumentAttributeExpression *argExpr = new ArgumentAttributeExpression(STRING_VALUE);
    argExpr->setOriginalString(str);
    funcSubExprVecPtr->push(argExpr);

    ASSERT_TRUE(funcSubExprVecPtr->exprs[0]->init(matchDocAllocator));
    ASSERT_TRUE(funcSubExprVecPtr->exprs[1]->init(matchDocAllocator));

    BuildInFunctionFactory *factory = new BuildInFunctionFactory;
    KeyValueMap parameters1;
    ASSERT_TRUE( factory->init(parameters1));
    KeyValueMap parameters2;
    config::ResourceReaderPtr resourceReader;
    AttributeExpression* expr = factory->createFuncExpression("in", funcSubExprVecPtr, parameters2, resourceReader);

    ASSERT_TRUE(expr);

    MatchDoc *matchDoc = matchDocAllocator->allocateMatchDoc(0);
    
    ASSERT_TRUE(expr->evaluate(matchDoc));
    AttributeExpressionTyped<bool>* typedExpr = 
        dynamic_cast<AttributeExpressionTyped<bool> * >(expr);
    ASSERT_TRUE(typedExpr);
    ASSERT_TRUE(typedExpr->getValue() == expectedValue);
    matchDocAllocator->deallocateMatchDoc(matchDoc);
    delete expr;
    delete funcSubExprVecPtr->exprs[0];
    delete funcSubExprVecPtr->exprs[1];
    factory->destroy();
    delete matchDocAllocator;
}

void InFuncExpressionTest::testDifferentType() { 
    HA3_LOG(DEBUG, "Begin Test!");
    checkDifferentType<int8_t>("\"1|10|56\"", 10, true);
    checkDifferentType<int8_t>("\"1|10|56\"", 11, false);
    checkDifferentType<uint8_t>("\"1|11|56\"", 11, true);
    checkDifferentType<uint8_t>("\"1|11|56\"", 12, false);
    checkDifferentType<int16_t>("\"1|1111|5655\"", 1111, true);
    checkDifferentType<int16_t>("\"1|1111|5655\"", 1112, false);
    checkDifferentType<uint16_t>("\"1|1111|5655\"", 1111, true);
    checkDifferentType<uint16_t>("\"1|1111|5655\"", 1112, false);
    checkDifferentType<int32_t>("\"1|1111000000|5655\"", 1111000000, true);
    checkDifferentType<int32_t>("\"1|1111000000|5655\"", 1112, false);
    checkDifferentType<uint32_t>("\"1|1111000000|5655\"", 1111000000, true);
    checkDifferentType<uint32_t>("\"1|1111000000|5655\"", 1112, false);
    checkDifferentType<int64_t>("\"1|1111000000|5655\"", 1111000000, true);
    checkDifferentType<int64_t>("\"1|1111000000|5655\"", 1112, false);
    checkDifferentType<uint64_t>("\"1|111100000000000|5655\"",111100000000000 , true);
    checkDifferentType<uint64_t>("\"1|111100000000000|5655\"", 1112, false);
    checkDifferentType<float>("\"1|111100.12|5655\"", 111100.12, true);
    checkDifferentType<float>("\"1|111100.12|5655\"", 1111.00, false);
    checkDifferentType<double>("\"1|111100000000.5|5655\"", 111100000000.5, true);
    checkDifferentType<double>("\"1|111100000000.5|5655\"", 1112, false);

}

void InFuncExpressionTest::testInitFail(){
    MatchDocAllocator *matchDocAllocator = new MatchDocAllocator(_pool);
    FunctionSubExprVecPtr funcSubExprVecPtr(new FunctionSubExprVec);
    funcSubExprVecPtr->push(new ConstAttributeExpression<int8_t>(123));
    ArgumentAttributeExpression *argExpr = new ArgumentAttributeExpression(STRING_VALUE);
    argExpr->setOriginalString("\"a|123|456\"");
    funcSubExprVecPtr->push(argExpr);

    ASSERT_TRUE(funcSubExprVecPtr->exprs[0]->init(matchDocAllocator));
    ASSERT_TRUE(funcSubExprVecPtr->exprs[1]->init(matchDocAllocator));

    BuildInFunctionFactory *factory = new BuildInFunctionFactory;
    KeyValueMap parameters1;
    ASSERT_TRUE( factory->init(parameters1));
    KeyValueMap parameters2;
    config::ResourceReaderPtr resourceReader;
    AttributeExpression* expr = factory->createFuncExpression("in", funcSubExprVecPtr, parameters2, resourceReader);

    ASSERT_TRUE(expr);
    ASSERT_TRUE(!expr->init(matchDocAllocator));

    delete expr;
    delete funcSubExprVecPtr->exprs[0];
    delete funcSubExprVecPtr->exprs[1];
    factory->destroy();
    delete matchDocAllocator;
}
END_HA3_NAMESPACE(search);

