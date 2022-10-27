#include "ha3/config/IndexInfos.h"
#include "ha3/config/TableInfo.h"
#include <indexlib/indexlib.h>
#include <suez/turing/common/JoinConfigInfo.h>
#include<unittest/unittest.h>
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/common/Request.h>
#include <ha3/qrs/RequestValidator.h>
#include <ha3_sdk/testlib/qrs/RequestCreator.h>
#include <suez/turing/expression/util/TableInfo.h>
#include <map>
#include <string>
#include <autil/StringUtil.h>
#include <ha3/common/ErrorDefine.h>
#include <suez/turing/expression/syntax/AtomicSyntaxExpr.h>
#include <suez/turing/expression/syntax/BinarySyntaxExpr.h>
#include <ha3/common/TermQuery.h>
#include <ha3/queryparser/RequestParser.h>
#include <ha3/config/ClusterConfigInfo.h>
#include <ha3/config/QrsConfig.h>
#include <iostream>

using namespace std;
using namespace autil;
using namespace suez::turing;
USE_HA3_NAMESPACE(common);
USE_HA3_NAMESPACE(search);
USE_HA3_NAMESPACE(queryparser);
USE_HA3_NAMESPACE(config);

BEGIN_HA3_NAMESPACE(qrs);

class RequestValidatorTest : public TESTBASE {
public:
    RequestValidatorTest();
    ~RequestValidatorTest();
public:
    void setUp();
    void tearDown();
protected:
    void checkErrorResult(std::string queryStr, ErrorCode errorCode);
    IndexInfos* createFakeIndexInfos(uint32_t count = 1, bool useSubSchema = true);
    AttributeInfos* createFakeAttributeInfos(bool createMulti = true, bool useSubSchema = true);
    AttributeInfos* createFakeMainAttributeInfos(bool createMulti = false, bool useSubSchema = false);
    AttributeInfos* createFakeAuxAttributeInfos(bool createMulti = false, bool useSubSchema = false);
    common::Request* prepareSimpleRequest();
    template <typename ExprType>
    void innerTestFilterClauseWithString(const std::string &attrName,
            bool ok, ErrorCode errorCode);
protected:
    ClusterTableInfoMapPtr _clusterTableInfoMapPtr;
    common::RequestPtr _requestPtr;
    RequestValidator* _requestValidator;
protected:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(qrs, RequestValidatorTest);


RequestValidatorTest::RequestValidatorTest() {
}

RequestValidatorTest::~RequestValidatorTest() {
}

void RequestValidatorTest::setUp() {
    HA3_LOG(DEBUG, "setUp!");
    _clusterTableInfoMapPtr.reset(new ClusterTableInfoMap);
    TableInfoPtr tableInfoPtr(new TableInfo());
    IndexInfos *indexInfos = createFakeIndexInfos();
    AttributeInfos *attrInfos = createFakeAttributeInfos();
    tableInfoPtr->setIndexInfos(indexInfos);
    tableInfoPtr->setAttributeInfos(attrInfos);
    tableInfoPtr->setTableName("table1");
    tableInfoPtr->setSubSchemaFlag(true);
    (*_clusterTableInfoMapPtr)["cluster1.default"] = tableInfoPtr;

    ClusterConfigMapPtr clusterConfigMapPtr(new ClusterConfigMap);
    ClusterConfigInfo clusterInfo1;
    clusterConfigMapPtr->insert(make_pair("cluster1.default", clusterInfo1));

    ClusterConfigInfo clusterInfo2;
    clusterConfigMapPtr->insert(make_pair("cluster2.default", clusterInfo2));

    ClusterConfigInfo clusterInfo3;
    clusterInfo3._hashMode._hashFields.push_back("pk_field");
    clusterInfo3._hashMode._hashFunction = "HASH";
    ASSERT_TRUE(clusterInfo3.initHashFunc());
    clusterConfigMapPtr->insert(make_pair("pk_cluster.default", clusterInfo3));

    ClusterConfigInfo clusterInfo4;
     clusterConfigMapPtr->insert(make_pair("1_2.default", clusterInfo4));

     ClusterConfigInfo clusterInfoMain;
     JoinConfigInfo joinConfigInfo(false, "aux", "company_id", false, false);
     clusterInfoMain._joinConfig.addJoinInfo(joinConfigInfo);
     clusterInfoMain._joinConfig.setScanJoinCluster("aux");
     clusterInfoMain.setQueryInfo(QueryInfo("default0", OP_AND));
     clusterConfigMapPtr->insert(make_pair("main.default", clusterInfoMain));

     ClusterConfigInfo clusterInfoAux;
     clusterConfigMapPtr->insert(make_pair("aux.default", clusterInfoAux));

     TableInfoPtr tableInfoPtr2(new TableInfo());
     IndexInfos* indexInfos2 = new IndexInfos();
     IndexInfo* indexInfo = new IndexInfo();
     indexInfo->setIndexName("pk");
     indexInfo->setIndexType(it_primarykey64);
     indexInfo->fieldName = "pk_field";
     indexInfos2->addIndexInfo(indexInfo);
     tableInfoPtr2->setIndexInfos(indexInfos2);
     tableInfoPtr2->setTableName("pk_table");
     (*_clusterTableInfoMapPtr)["pk_cluster.default"] = tableInfoPtr2;

     TableInfoPtr tableInfoMain(new TableInfo);
     IndexInfos *mainIndexInfos = new IndexInfos;
     IndexInfo *mainPkIndexInfo = new IndexInfo;
     mainPkIndexInfo->setIndexName("main_pk");
     mainPkIndexInfo->setIndexType(it_primarykey64);
     mainIndexInfos->addIndexInfo(mainPkIndexInfo);
     IndexInfo *indexInfoMainDefault = new IndexInfo();
     indexInfoMainDefault->setIndexName("default0");
     indexInfoMainDefault->addField("title", 100);
     indexInfoMainDefault->addField("body", 50);
     indexInfoMainDefault->setIndexType(it_expack);
     indexInfoMainDefault->setAnalyzerName("SimpleAnalyzer");
     mainIndexInfos->addIndexInfo(indexInfoMainDefault);
     tableInfoMain->setIndexInfos(mainIndexInfos);
     auto *mainAttrInfos = createFakeMainAttributeInfos();
     tableInfoMain->setAttributeInfos(mainAttrInfos);
     (*_clusterTableInfoMapPtr)["main.default"] = tableInfoMain;
     TableInfoPtr tableInfoAux(new TableInfo);
     IndexInfos *auxIndexInfos = new IndexInfos;
     IndexInfo *auxPkIndexInfo = new IndexInfo;
     auxPkIndexInfo->setIndexName("aux_pk");
     auxPkIndexInfo->setIndexType(it_primarykey64);
     auxIndexInfos->addIndexInfo(auxPkIndexInfo);
     tableInfoAux->setIndexInfos(auxIndexInfos);
     auto *auxAttrInfos = createFakeAuxAttributeInfos();
     tableInfoAux->setAttributeInfos(auxAttrInfos);
     (*_clusterTableInfoMapPtr)["aux.default"] = tableInfoAux;

     _requestValidator = new RequestValidator(_clusterTableInfoMapPtr,
             QRS_RETURN_HITS_LIMIT, clusterConfigMapPtr,
             ClusterFuncMapPtr(new ClusterFuncMap), CavaPluginManagerMapPtr(),
             ClusterSorterNamesPtr(new ClusterSorterNames));
 }

 void RequestValidatorTest::tearDown() {
     HA3_LOG(DEBUG, "tearDown!");

     DELETE_AND_SET_NULL(_requestValidator);
 }

 TEST_F(RequestValidatorTest, testSimpleProcess) {
     HA3_LOG(DEBUG, "Begin Test!");

     _requestPtr.reset(prepareSimpleRequest());
     ASSERT_TRUE(_requestPtr);

     _requestPtr->setConfigClause(NULL);
     bool ret = _requestValidator->validate(_requestPtr);
     ASSERT_TRUE(!ret);

     ASSERT_EQ(ERROR_NO_CONFIG_CLAUSE, _requestValidator->getErrorCode());
 }

 TEST_F(RequestValidatorTest, testQueryClauseNotExist) {
     HA3_LOG(DEBUG, "Begin Test!");

     _requestPtr.reset(prepareSimpleRequest());
     ASSERT_TRUE(_requestPtr);

     _requestPtr->setQueryClause(NULL);
     bool ret = _requestValidator->validate(_requestPtr);
     ASSERT_TRUE(!ret);
     ASSERT_EQ(ERROR_NO_QUERY_CLAUSE, _requestValidator->getErrorCode());
 }

 TEST_F(RequestValidatorTest, testValidateVirtualAttributeClause) {
     HA3_LOG(DEBUG, "Begin Test!");

     //success, virtualAttributeClause is NULL
     {
         _requestPtr.reset(prepareSimpleRequest());
         bool ret = _requestValidator->validate(_requestPtr);
         ASSERT_TRUE(ret);
     }

     //success
     {
         _requestPtr.reset(prepareSimpleRequest());
         VirtualAttributeClause *virtualAttributeClause =
             new VirtualAttributeClause;
         AtomicSyntaxExpr *syntaxExpr1 =
             new AtomicSyntaxExpr("price", vt_unknown, ATTRIBUTE_NAME);
         virtualAttributeClause->addVirtualAttribute("va1", syntaxExpr1);

         SyntaxExpr *leftExpr1 =
             new AtomicSyntaxExpr("222", vt_unknown, INTEGER_VALUE);
         SyntaxExpr *rightExpr1 =
             new AtomicSyntaxExpr("va1", vt_unknown, ATTRIBUTE_NAME);
         AddSyntaxExpr *syntaxExpr2 =
             new AddSyntaxExpr(leftExpr1, rightExpr1);
         virtualAttributeClause->addVirtualAttribute("va2", syntaxExpr2);

         SyntaxExpr *leftExpr2 =
             new AtomicSyntaxExpr("va2", vt_unknown, ATTRIBUTE_NAME);
         SyntaxExpr *rightExpr2 =
             new AtomicSyntaxExpr("va1", vt_unknown, ATTRIBUTE_NAME);
         MinusSyntaxExpr *syntaxExpr3 =
             new MinusSyntaxExpr(leftExpr2, rightExpr2);
         virtualAttributeClause->addVirtualAttribute("va3", syntaxExpr3);

         _requestPtr->setVirtualAttributeClause(virtualAttributeClause);

         bool ret = _requestValidator->validate(_requestPtr);
         ASSERT_TRUE(ret);

         ASSERT_EQ(SYNTAX_EXPR_TYPE_ATOMIC_ATTR,
                              syntaxExpr1->getSyntaxExprType());
         ASSERT_EQ(vt_int32, syntaxExpr1->getExprResultType());
         ASSERT_TRUE(!syntaxExpr1->isMultiValue());

         ASSERT_EQ(SYNTAX_EXPR_TYPE_CONST_VALUE,
                              leftExpr1->getSyntaxExprType());
         ASSERT_EQ(vt_int32, leftExpr1->getExprResultType());
         ASSERT_TRUE(!leftExpr1->isMultiValue());

         ASSERT_EQ(SYNTAX_EXPR_TYPE_VIRTUAL_ATTR,
                              rightExpr1->getSyntaxExprType());

         ASSERT_EQ(SYNTAX_EXPR_TYPE_ADD,
                              syntaxExpr2->getSyntaxExprType());
         ASSERT_EQ(vt_int32, syntaxExpr2->getExprResultType());
         ASSERT_TRUE(!syntaxExpr2->isMultiValue());

         ASSERT_EQ(SYNTAX_EXPR_TYPE_VIRTUAL_ATTR,
                              leftExpr2->getSyntaxExprType());
         ASSERT_EQ(SYNTAX_EXPR_TYPE_VIRTUAL_ATTR,
                              rightExpr2->getSyntaxExprType());

         ASSERT_EQ(SYNTAX_EXPR_TYPE_MINUS,
                              syntaxExpr3->getSyntaxExprType());
         ASSERT_EQ(vt_int32, syntaxExpr3->getExprResultType());
         ASSERT_TRUE(!syntaxExpr3->isMultiValue());
     }

     //failed, name confict with attribute
     {
         _requestPtr.reset(prepareSimpleRequest());
         VirtualAttributeClause *virtualAttributeClause =
             new VirtualAttributeClause;
         AtomicSyntaxExpr *syntaxExpr1 =
             new AtomicSyntaxExpr("price", vt_unknown, ATTRIBUTE_NAME);
         virtualAttributeClause->addVirtualAttribute("type", syntaxExpr1);
         _requestPtr->setVirtualAttributeClause(virtualAttributeClause);
         bool ret = _requestValidator->validate(_requestPtr);
         ASSERT_TRUE(!ret);
     }

     //failed, virtual attr order wrong
     {
         _requestPtr.reset(prepareSimpleRequest());
         VirtualAttributeClause *virtualAttributeClause =
             new VirtualAttributeClause;
         AtomicSyntaxExpr *syntaxExpr1 =
             new AtomicSyntaxExpr("price", vt_unknown, ATTRIBUTE_NAME);
         virtualAttributeClause->addVirtualAttribute("va1", syntaxExpr1);

         SyntaxExpr *leftExpr2 =
             new AtomicSyntaxExpr("va2", vt_unknown, ATTRIBUTE_NAME);
         SyntaxExpr *rightExpr2 =
             new AtomicSyntaxExpr("va1", vt_unknown, ATTRIBUTE_NAME);
         MinusSyntaxExpr *syntaxExpr3 =
             new MinusSyntaxExpr(leftExpr2, rightExpr2);
         virtualAttributeClause->addVirtualAttribute("va3", syntaxExpr3);

         SyntaxExpr *leftExpr1 =
             new AtomicSyntaxExpr("222", vt_unknown, INTEGER_VALUE);
         SyntaxExpr *rightExpr1 =
             new AtomicSyntaxExpr("va1", vt_unknown, ATTRIBUTE_NAME);
         AddSyntaxExpr *syntaxExpr2 =
             new AddSyntaxExpr(leftExpr1, rightExpr1);
         virtualAttributeClause->addVirtualAttribute("va2", syntaxExpr2);
         _requestPtr->setVirtualAttributeClause(virtualAttributeClause);
         bool ret = _requestValidator->validate(_requestPtr);
         ASSERT_TRUE(!ret);
     }
     // success
     {
         RequestPtr requestPtr = RequestCreator::prepareRequest(
                 "config=cluster:cluster1&&query=a&&virtual_attribute=va1:222");
         ASSERT_TRUE(requestPtr);
         bool ret = _requestValidator->validate(requestPtr);
         ASSERT_TRUE(ret);

         requestPtr = RequestCreator::prepareRequest(
                 "config=cluster:cluster1&&query=a&&virtual_attribute=va1:\"222\"");
         ASSERT_TRUE(requestPtr);
         ret = _requestValidator->validate(requestPtr);
         ASSERT_TRUE(ret);

         requestPtr = RequestCreator::prepareRequest(
                 "config=cluster:cluster1&&query=a&&virtual_attribute=va1:222.2");
         ASSERT_TRUE(requestPtr);
         ret = _requestValidator->validate(requestPtr);
         ASSERT_TRUE(ret);
     }
 }

 TEST_F(RequestValidatorTest, testValidateQueryWithTruncateName) {
     HA3_LOG(DEBUG, "Begin Test!");
     RequestPtr requestPtr = RequestCreator::prepareRequest(
             "config=cluster:cluster1"
             "&&query=expack_index:abc#secondaryChain^200");
     ASSERT_TRUE(requestPtr);
     bool ret = _requestValidator->validate(requestPtr);
     ASSERT_TRUE(ret);
     ASSERT_EQ(ERROR_NONE,
                          _requestValidator->getErrorCode());
     QueryClause *queryClause = requestPtr->getQueryClause();
     TermQuery *termQuery = dynamic_cast<TermQuery*>(queryClause->getRootQuery());
     ASSERT_TRUE(termQuery);
     const Term &term = termQuery->getTerm();
     ASSERT_EQ((int32_t)200, term.getBoost());
     ASSERT_EQ(string("secondaryChain"), term.getTruncateName());
 }

 TEST_F(RequestValidatorTest, testValidateQueryWithRequiredFields) {
     HA3_LOG(DEBUG, "Begin Test!");
     //success1
     {
         RequestPtr requestPtr = RequestCreator::prepareRequest("config=cluster:cluster1"
                 "&&query=expack_index(title,body):k"
                 "&&virtual_attribute=va1:222 > price && filter=va1");
         ASSERT_TRUE(requestPtr);
         bool ret = _requestValidator->validate(requestPtr);
         ASSERT_TRUE(ret);
         ASSERT_EQ(ERROR_NONE,
                              _requestValidator->getErrorCode());
     }

     //success2
     {
         RequestPtr requestPtr = RequestCreator::prepareRequest("config=cluster:cluster1"
                 "&&query=expack_index[title,body]:k"
                 "&&virtual_attribute=va1:222 > price && filter=va1");
         ASSERT_TRUE(requestPtr);
         bool ret = _requestValidator->validate(requestPtr);
         ASSERT_TRUE(ret);
         ASSERT_EQ(ERROR_NONE,
                              _requestValidator->getErrorCode());
     }

     //falied: index not exist
     {
         RequestPtr requestPtr = RequestCreator::prepareRequest("config=cluster:cluster1"
                 "&&query=notexist[title,body]:k"
                 "&&virtual_attribute=va1:222 > price && filter=va1");
         ASSERT_TRUE(requestPtr);
         bool ret = _requestValidator->validate(requestPtr);
         ASSERT_TRUE(!ret);
         ASSERT_EQ(ERROR_INDEX_NOT_EXIST,
                              _requestValidator->getErrorCode());
     }

     //falied: field not exist
     {
         RequestPtr requestPtr = RequestCreator::prepareRequest("config=cluster:cluster1"
                 "&&query=expack_index[title,notexist]:k"
                 "&&virtual_attribute=va1:222 > price && filter=va1");
         ASSERT_TRUE(requestPtr);
         bool ret = _requestValidator->validate(requestPtr);
         ASSERT_TRUE(!ret);
         ASSERT_EQ(ERROR_INDEX_FIELD_INVALID,
                              _requestValidator->getErrorCode());
     }

     //falied: index type not right
     {
         RequestPtr requestPtr = RequestCreator::prepareRequest("config=cluster:cluster1"
                 "&&query=default0[title,body]:k"
                 "&&virtual_attribute=va1:222 > price && filter=va1");
         ASSERT_TRUE(requestPtr);
         bool ret = _requestValidator->validate(requestPtr);
         ASSERT_TRUE(!ret);
         ASSERT_EQ(ERROR_INDEX_TYPE_INVALID,
                              _requestValidator->getErrorCode());
     }

     //falied: parse failed
     {
         RequestPtr requestPtr = RequestCreator::prepareRequest("config=cluster:cluster1"
                 "&&query=expack_index[title,body):k"
                 "&&virtual_attribute=va1:222 > price && filter=va1");
         ASSERT_TRUE(!requestPtr);
     }

     // va2 == va1: test serialize and deserialize
     {
         RequestPtr requestPtr = RequestCreator::prepareRequest("config=cluster:cluster1"
                 "&&query=expack_index(title,body):k"
                 "&&virtual_attribute=va1:222>price;va2:va1");
         ASSERT_TRUE(requestPtr);
         bool ret = _requestValidator->validate(requestPtr);
         ASSERT_TRUE(ret);
         ASSERT_EQ(ERROR_NONE,
                              _requestValidator->getErrorCode());
         VirtualAttributeClause *virtualAttributeClause =
             requestPtr->getVirtualAttributeClause();
         autil::DataBuffer buffer;
         virtualAttributeClause->serialize(buffer);
         VirtualAttributeClause virtualAttributeClause2;
         virtualAttributeClause2.deserialize(buffer);
         ASSERT_EQ(virtualAttributeClause->getOriginalString(),
                              virtualAttributeClause2.getOriginalString());
     }
 }

 TEST_F(RequestValidatorTest, testFilterWithVirtualAttribute) {
     HA3_LOG(DEBUG, "Begin Test!");
     //success1
     {
         RequestPtr requestPtr = RequestCreator::prepareRequest(
                 "config=cluster:cluster1&&query=a"
                 "&&virtual_attribute=va1:222 > price && filter=va1");
         ASSERT_TRUE(requestPtr);
         bool ret = _requestValidator->validate(requestPtr);
         ASSERT_TRUE(ret);
         ASSERT_EQ(ERROR_NONE,
                              _requestValidator->getErrorCode());
     }

     //success2
     {
         RequestPtr requestPtr = RequestCreator::prepareRequest(
                 "config=cluster:cluster1&&query=a"
                 "&&virtual_attribute=va1:price;va2:222 + va1 "
                 "&&filter=va1>123 AND 456=va2");
         ASSERT_TRUE(requestPtr);
         bool ret = _requestValidator->validate(requestPtr);
         ASSERT_TRUE(ret);
         ASSERT_EQ(ERROR_NONE,
                              _requestValidator->getErrorCode());
     }

     //failed, va type not match and not exist
     {
         RequestPtr requestPtr = RequestCreator::prepareRequest(
                 "config=cluster:cluster1&&query=a"
                 "&&virtual_attribute=va1:222 + price && filter=va1");
         ASSERT_TRUE(requestPtr);
         bool ret = _requestValidator->validate(requestPtr);
         ASSERT_TRUE(!ret);
         ASSERT_EQ(ERROR_SYNTAX_EXPRESSION_TYPE_NOT_SUPPORT,
                              _requestValidator->getErrorCode());

         requestPtr = RequestCreator::prepareRequest("config=cluster:cluster1&&query=a"
             "&&virtual_attribute=va1:222 + price && filter=va2");
         ASSERT_TRUE(requestPtr);
         ret = _requestValidator->validate(requestPtr);
         ASSERT_TRUE(!ret);
         ASSERT_EQ(ERROR_ATTRIBUTE_NOT_EXIST,
                              _requestValidator->getErrorCode());
     }
 }

 TEST_F(RequestValidatorTest, testFilterClauseError) {
     HA3_LOG(DEBUG, "Begin Test!");

     _requestPtr.reset(prepareSimpleRequest());
     ASSERT_TRUE(_requestPtr);

     AtomicSyntaxExpr *syntaxExpr
         = new AtomicSyntaxExpr("price", vt_unknown, ATTRIBUTE_NAME);
     FilterClause *filterClause = new FilterClause(syntaxExpr);
     _requestPtr->setFilterClause(filterClause);

     bool ret = _requestValidator->validate(_requestPtr);
     ASSERT_TRUE(!ret);
     ASSERT_EQ(ERROR_SYNTAX_EXPRESSION_TYPE_NOT_SUPPORT,
                          _requestValidator->getErrorCode());
     ASSERT_EQ(string("syntax location: FILTER_SYNTAX_LOCATION"),
                          _requestValidator->getErrorMsg());
 }

 TEST_F(RequestValidatorTest, testFilterClauseErrorWithMultiValue) {
     HA3_LOG(DEBUG, "Begin Test!");

     _requestPtr.reset(prepareSimpleRequest());
     ASSERT_TRUE(_requestPtr);

     AtomicSyntaxExpr *leftExpr
         = new AtomicSyntaxExpr("multi_attr_1", vt_int32, ATTRIBUTE_NAME);
     AtomicSyntaxExpr *rightExpr
         = new AtomicSyntaxExpr("multi_attr_2", vt_int32, ATTRIBUTE_NAME);

     SyntaxExpr* expr = new EqualSyntaxExpr(leftExpr, rightExpr);

     FilterClause *filterClause = new FilterClause(expr);
     _requestPtr->setFilterClause(filterClause);

     bool ret = _requestValidator->validate(_requestPtr);
     ASSERT_TRUE(!ret);
     ASSERT_EQ(ERROR_EXPRESSION_WITH_MULTI_VALUE,
                          _requestValidator->getErrorCode());
 }

 TEST_F(RequestValidatorTest, testFilterClauseWithMultiValue){
     HA3_LOG(DEBUG, "Begin Test!");

     _requestPtr.reset(prepareSimpleRequest());
     ASSERT_TRUE(_requestPtr);

     AtomicSyntaxExpr *leftExpr
         = new AtomicSyntaxExpr("multi_attr_1", vt_int32, ATTRIBUTE_NAME);
     AtomicSyntaxExpr *rightExpr
         = new AtomicSyntaxExpr("123", vt_int32, INTEGER_VALUE);
     SyntaxExpr* equalExpr = new EqualSyntaxExpr(leftExpr, rightExpr);
     FilterClause *filterClause = new FilterClause(equalExpr);
     _requestPtr->setFilterClause(filterClause);

     bool ret = _requestValidator->validate(_requestPtr);
     ASSERT_TRUE(ret);
     ASSERT_EQ(ERROR_NONE,
                          _requestValidator->getErrorCode());

     leftExpr = new AtomicSyntaxExpr("multi_attr_1", vt_int32, ATTRIBUTE_NAME);
     rightExpr = new AtomicSyntaxExpr("123", vt_int32, INTEGER_VALUE);
     SyntaxExpr *notEqualExpr = new EqualSyntaxExpr(leftExpr, rightExpr);
     filterClause = new FilterClause(notEqualExpr);
     _requestPtr->setFilterClause(filterClause);

     ret = _requestValidator->validate(_requestPtr);
     ASSERT_TRUE(ret);
     ASSERT_EQ(ERROR_NONE,
                          _requestValidator->getErrorCode());
 }

 TEST_F(RequestValidatorTest, testFilterClauseWithString) {
     // supported syntax expression
 #define INNER_TEST_FILTER_WITH_SUPPORTED_EXPR(attrName) {               \
         innerTestFilterClauseWithString<EqualSyntaxExpr>(attrName, true, ERROR_NONE); \
         innerTestFilterClauseWithString<NotEqualSyntaxExpr>(attrName, true, ERROR_NONE); \
         innerTestFilterClauseWithString<LessSyntaxExpr>(attrName, true, ERROR_NONE); \
         innerTestFilterClauseWithString<GreaterSyntaxExpr>(attrName, true, ERROR_NONE); \
         innerTestFilterClauseWithString<LessEqualSyntaxExpr>(attrName, true, ERROR_NONE); \
         innerTestFilterClauseWithString<GreaterEqualSyntaxExpr>(attrName, true, ERROR_NONE); \
     }
     INNER_TEST_FILTER_WITH_SUPPORTED_EXPR("string_attr");
 #undef INNER_TEST_FILTER_WITH_SUPPORTED_EXPR
     // only Equal/NotEqual support multi string attribute
     innerTestFilterClauseWithString<EqualSyntaxExpr>("multi_string_attr", true, ERROR_NONE);
     innerTestFilterClauseWithString<NotEqualSyntaxExpr>("multi_string_attr", true, ERROR_NONE);

     // not supported syntax expression
 #define INNER_TEST_FILTER_WITH_NOT_SUPPORTED_EXPR(attrName) {\
         innerTestFilterClauseWithString<AddSyntaxExpr>(                 \
                 attrName, false, ERROR_STRING_IN_BINARY_EXPRESSION);    \
         innerTestFilterClauseWithString<MinusSyntaxExpr>(               \
                 attrName, false, ERROR_STRING_IN_BINARY_EXPRESSION);    \
         innerTestFilterClauseWithString<MulSyntaxExpr>(                 \
                 attrName, false, ERROR_STRING_IN_BINARY_EXPRESSION);    \
         innerTestFilterClauseWithString<DivSyntaxExpr>(                 \
                 attrName, false, ERROR_STRING_IN_BINARY_EXPRESSION);    \
         innerTestFilterClauseWithString<AndSyntaxExpr>(                 \
                 attrName, false, ERROR_STRING_IN_BINARY_EXPRESSION);    \
         innerTestFilterClauseWithString<OrSyntaxExpr>(                  \
                 attrName, false, ERROR_STRING_IN_BINARY_EXPRESSION);    \
         innerTestFilterClauseWithString<BitAndSyntaxExpr>(              \
                 attrName, false, ERROR_STRING_IN_BINARY_EXPRESSION);    \
         innerTestFilterClauseWithString<BitOrSyntaxExpr>(               \
                 attrName, false, ERROR_STRING_IN_BINARY_EXPRESSION);    \
         innerTestFilterClauseWithString<BitXorSyntaxExpr>(              \
                 attrName, false, ERROR_STRING_IN_BINARY_EXPRESSION);    \
     }
         INNER_TEST_FILTER_WITH_NOT_SUPPORTED_EXPR("string_attr");
         INNER_TEST_FILTER_WITH_NOT_SUPPORTED_EXPR("multi_string_attr");
 #undef INNER_TEST_FILTER_WITH_NOT_SUPPORTED_EXPR

         innerTestFilterClauseWithString<LessSyntaxExpr>("multi_string_attr",
                 false, ERROR_EXPRESSION_WITH_MULTI_VALUE);
         innerTestFilterClauseWithString<GreaterSyntaxExpr>("multi_string_attr",
                 false, ERROR_EXPRESSION_WITH_MULTI_VALUE);
         innerTestFilterClauseWithString<LessEqualSyntaxExpr>("multi_string_attr",
                 false, ERROR_EXPRESSION_WITH_MULTI_VALUE);
         innerTestFilterClauseWithString<GreaterEqualSyntaxExpr>("multi_string_attr",
                 false, ERROR_EXPRESSION_WITH_MULTI_VALUE);
 }

 template <typename ExprType>
 void RequestValidatorTest::innerTestFilterClauseWithString(
         const string &attrName, bool ok, ErrorCode errorCode)
 {
     //cout << typeid(ExprType).name() << endl;
     _requestPtr.reset(prepareSimpleRequest());
     ASSERT_TRUE(_requestPtr);
     AtomicSyntaxExpr *leftExpr =
         new AtomicSyntaxExpr(attrName, vt_string, ATTRIBUTE_NAME);
     AtomicSyntaxExpr *rightExpr =
         new AtomicSyntaxExpr("hello", vt_string, STRING_VALUE);
     SyntaxExpr *equalExpr = new ExprType(leftExpr, rightExpr);
     FilterClause *filterClause = new FilterClause(equalExpr);
     _requestPtr->setFilterClause(filterClause);
     bool ret = _requestValidator->validate(_requestPtr);
     ASSERT_EQ(ok, ret);
     ASSERT_EQ(errorCode, _requestValidator->getErrorCode());
 }

 TEST_F(RequestValidatorTest, testFilterClauseWithWrongOperator){
     HA3_LOG(DEBUG, "Begin Test!");

     _requestPtr.reset(prepareSimpleRequest());
     ASSERT_TRUE(_requestPtr);

     AtomicSyntaxExpr *leftExpr
         = new AtomicSyntaxExpr("multi_attr_1", vt_int32, ATTRIBUTE_NAME);
     AtomicSyntaxExpr *rightExpr
         = new AtomicSyntaxExpr("123", vt_int32, INTEGER_VALUE);

     SyntaxExpr* expr = new GreaterSyntaxExpr(leftExpr, rightExpr);

     FilterClause *filterClause = new FilterClause(expr);
     _requestPtr->setFilterClause(filterClause);

     bool ret = _requestValidator->validate(_requestPtr);
     ASSERT_TRUE(!ret);
     ASSERT_EQ(ERROR_EXPRESSION_WITH_MULTI_VALUE,
                          _requestValidator->getErrorCode());
 }


 TEST_F(RequestValidatorTest, testAuxQuerySimple) {
     HA3_LOG(DEBUG, "Begin Test!");
     {
         RequestPtr requestPtr = RequestCreator::prepareRequest(
                 "config=cluster:main&&query=a"
                 "&& aux_query=aux_pk:1");
         ASSERT_TRUE(requestPtr);
         bool ret = _requestValidator->validate(requestPtr);
         ASSERT_TRUE(ret);
         ASSERT_EQ(ERROR_NONE,
                              _requestValidator->getErrorCode());
     }
 }

 TEST_F(RequestValidatorTest, testAuxQueryInClusterWithoutAux) {
     HA3_LOG(DEBUG, "Begin Test!");
     {
         RequestPtr requestPtr = RequestCreator::prepareRequest(
                 "config=cluster:cluster1&&query=a"
                 "&& aux_query=aux_pk:1");
         ASSERT_TRUE(requestPtr);
         bool ret = _requestValidator->validate(requestPtr);
         ASSERT_TRUE(!ret);
         ASSERT_EQ(ERROR_NO_SCAN_JOIN_CLUSTER_NAME,
                              _requestValidator->getErrorCode());
     }
 }

 TEST_F(RequestValidatorTest, testAuxQueryIndexNotExist) {
     HA3_LOG(DEBUG, "Begin Test!");
     {
         RequestPtr requestPtr = RequestCreator::prepareRequest(
                 "config=cluster:main&&query=a"
                 "&& aux_query=aux_pk_not_exist:1");
         ASSERT_TRUE(requestPtr);
         bool ret = _requestValidator->validate(requestPtr);
         ASSERT_TRUE(!ret);
         ASSERT_EQ(ERROR_INDEX_NOT_EXIST,
                              _requestValidator->getErrorCode());
     }
 }


 TEST_F(RequestValidatorTest, testAuxFilterSimple) {
     HA3_LOG(DEBUG, "Begin Test!");
     {
         RequestPtr requestPtr = RequestCreator::prepareRequest(
                 "config=cluster:main&&query=a"
                 "&& aux_filter=aux_price=1");
         ASSERT_TRUE(requestPtr);
         bool ret = _requestValidator->validate(requestPtr);
         ASSERT_TRUE(ret);
         ASSERT_EQ(ERROR_NONE,
                              _requestValidator->getErrorCode());
     }
 }

 TEST_F(RequestValidatorTest, testAuxFilterInClusterWithoutAux) {
     HA3_LOG(DEBUG, "Begin Test!");
     {
         RequestPtr requestPtr = RequestCreator::prepareRequest(
                 "config=cluster:cluster1&&query=a"
                 "&& aux_filter=aux_price=1");
         ASSERT_TRUE(requestPtr);
         bool ret = _requestValidator->validate(requestPtr);
         ASSERT_TRUE(!ret);
         ASSERT_EQ(ERROR_NO_SCAN_JOIN_CLUSTER_NAME,
                              _requestValidator->getErrorCode());
     }
}

 TEST_F(RequestValidatorTest, testAuxFilterAttrNotExist) {
     HA3_LOG(DEBUG, "Begin Test!");
     {
         RequestPtr requestPtr = RequestCreator::prepareRequest(
                 "config=cluster:main&&query=a"
                 "&& aux_filter=aux_price_not_exist=1");
         ASSERT_TRUE(requestPtr);
         bool ret = _requestValidator->validate(requestPtr);
         ASSERT_TRUE(!ret);
         ASSERT_EQ(ERROR_ATTRIBUTE_NOT_EXIST,
                              _requestValidator->getErrorCode());
     }
 }

 TEST_F(RequestValidatorTest, testAuxFilterWithVirtualAttribute) {
     // aux filter don't support virtual attribute
     HA3_LOG(DEBUG, "Begin Test!");
     {
         RequestPtr requestPtr = RequestCreator::prepareRequest(
                 "config=cluster:main&&query=a"
                 "&&virtual_attribute=va1:222 > aux_price && aux_filter=va1");
         ASSERT_TRUE(requestPtr);
         bool ret = _requestValidator->validate(requestPtr);
         ASSERT_TRUE(!ret);
         ASSERT_EQ(ERROR_ATTRIBUTE_NOT_EXIST,
                              _requestValidator->getErrorCode());
     }
 }

 TEST_F(RequestValidatorTest, testMultiClausesWithVirtualAttribute) {
     HA3_LOG(DEBUG, "Begin Test!");

     RequestPtr requestPtr = RequestCreator::prepareRequest("config=cluster:cluster1&&query=a"
             "&&virtual_attribute=va1:price"
             "&&filter=va1>3&&sort=+va1&&"
             "distinct=dist_key:va1,dist_count:1,dist_filter:va1>4"
             "&&aggregate=group_key:va1,agg_fun:sum(va1+1)");
     ASSERT_TRUE(requestPtr);
     ASSERT_TRUE(_requestValidator->validate(requestPtr));
     ASSERT_EQ(ERROR_NONE, _requestValidator->getErrorCode());

     requestPtr = RequestCreator::prepareRequest("config=cluster:cluster1&&query=a"
             "&&virtual_attribute=va1:price"
             "&&filter=va1>3&&sort=+va1"
             "&&attribute=va1,type"
             "&&aggregate=group_key:va1,agg_fun:sum(va1+1)");
     ASSERT_TRUE(requestPtr);
     ASSERT_TRUE(_requestValidator->validate(requestPtr));
     ASSERT_EQ(ERROR_NONE, _requestValidator->getErrorCode());

     requestPtr = RequestCreator::prepareRequest("config=cluster:cluster1&&query=a"
             "&&virtual_attribute=va1:price"
             "&&filter=va1>3&&sort=+va1"
             "&&attribute=va1,type,va2"
             "&&aggregate=group_key:va1,agg_fun:sum(va1+1)");
     ASSERT_TRUE(requestPtr);
     ASSERT_TRUE(!_requestValidator->validate(requestPtr));
     ASSERT_EQ(ERROR_ATTRIBUTE_NOT_EXIST,
                          _requestValidator->getErrorCode());

     requestPtr = RequestCreator::prepareRequest("config=cluster:cluster1&&query=a"
             "&&virtual_attribute=va1:price;va2:va1+2;va3:va2-va1"
             "&&filter=va1>3&&sort=+va1&&"
             "distinct=dist_key:va1,dist_count:1,dist_filter:va2>4"
             "&&aggregate=group_key:va2,agg_fun:sum(va3)");
     ASSERT_TRUE(requestPtr);
     ASSERT_TRUE(_requestValidator->validate(requestPtr));
     ASSERT_EQ(ERROR_NONE, _requestValidator->getErrorCode());

     requestPtr = RequestCreator::prepareRequest("config=cluster:cluster1&&query=a"
             "&&virtual_attribute=va1:price"
             "&&filter=va1>3&&sort=+va1&&"
             "distinct=dist_key:va1,dist_count:1,dist_filter:va1>4"
             "&&aggregate=group_key:va1,agg_fun:sum(va2+1)");
     ASSERT_TRUE(requestPtr);
     ASSERT_TRUE(!_requestValidator->validate(requestPtr));
     ASSERT_EQ(ERROR_ATTRIBUTE_NOT_EXIST,
                          _requestValidator->getErrorCode());

     requestPtr = RequestCreator::prepareRequest("config=cluster:cluster1&&query=a"
             "&&virtual_attribute=va1:price"
             "&&filter=va1&&sort=+va1&&"
             "distinct=dist_key:va1,dist_count:1,dist_filter:va1>4"
             "&&aggregate=group_key:va1,agg_fun:sum(va1+1)");
     ASSERT_TRUE(requestPtr);
     ASSERT_TRUE(!_requestValidator->validate(requestPtr));
     ASSERT_EQ(ERROR_SYNTAX_EXPRESSION_TYPE_NOT_SUPPORT,
                          _requestValidator->getErrorCode());

     requestPtr = RequestCreator::prepareRequest("config=cluster:cluster1&&query=a"
             "&&virtual_attribute=va1:price;va2:\"abxp\""
             "&&filter=va1&&sort=+va1&&"
             "distinct=dist_key:va1,dist_count:1,dist_filter:va1>4"
             "&&aggregate=group_key:va1,agg_fun:sum(va2+1)");
     ASSERT_TRUE(requestPtr);
     ASSERT_TRUE(!_requestValidator->validate(requestPtr));
     ASSERT_EQ(ERROR_SYNTAX_EXPRESSION_TYPE_NOT_SUPPORT,
                          _requestValidator->getErrorCode());

     requestPtr = RequestCreator::prepareRequest("config=cluster:cluster1&&query=a"
             "&&virtual_attribute=va1:price;va1:type"
             "&&filter=va1>3&&sort=+va1&&"
             "distinct=dist_key:va1,dist_count:1,dist_filter:va1>4"
             "&&aggregate=group_key:va1,agg_fun:sum(va1+1)");
     ASSERT_TRUE(!requestPtr);

     requestPtr = RequestCreator::prepareRequest("config=cluster:cluster1&&query=a"
             "&&virtual_attribute=va1:va2 + 1;va2:price"
             "&&filter=va1>3&&sort=+va1&&"
             "distinct=dist_key:va1,dist_count:1,dist_filter:va1>4"
             "&&aggregate=group_key:va1,agg_fun:sum(va1+1)");
     ASSERT_TRUE(requestPtr);
     ASSERT_TRUE(!_requestValidator->validate(requestPtr));
     ASSERT_EQ(ERROR_ATTRIBUTE_NOT_EXIST,
                          _requestValidator->getErrorCode());
 }

 TEST_F(RequestValidatorTest, testRankSortConflictWithDistinctSort) {
     HA3_LOG(DEBUG, "Begin Test!");
     RequestPtr requestPtr = RequestCreator::prepareRequest(
             "config=cluster:cluster1&&query=a"
             "&&distinct=dist_key:price,dist_count:1"
             "&&rank_sort=sort:RANK,percent:40;sort:price,percent:60");
     ASSERT_TRUE(requestPtr);
     ASSERT_TRUE(!_requestValidator->validate(requestPtr));
     ASSERT_EQ(ERROR_RANK_SORT_CLAUSE_AND_DISTINCT_CLAUSE_CONFLICT,
                          _requestValidator->getErrorCode());

     requestPtr = RequestCreator::prepareRequest(
             "config=cluster:cluster1&&query=a"
             "&&distinct=dist_key:price,dist_count:1"
             "&&rank_sort=sort:RANK,percent:40");
     ASSERT_TRUE(requestPtr);
     ASSERT_TRUE(_requestValidator->validate(requestPtr));
     ASSERT_EQ(ERROR_NONE, _requestValidator->getErrorCode());

     requestPtr = RequestCreator::prepareRequest(
             "config=cluster:cluster1&&query=a"
             "&&distinct=dist_key:price,dist_count:2;dist_key:price,dist_count:1"
             "&&rank_sort=sort:RANK,percent:40;sort:price,percent:60");
     ASSERT_TRUE(requestPtr);
     ASSERT_TRUE(!_requestValidator->validate(requestPtr));
     ASSERT_EQ(ERROR_RANK_SORT_CLAUSE_AND_DISTINCT_CLAUSE_CONFLICT,
                          _requestValidator->getErrorCode());

     requestPtr = RequestCreator::prepareRequest(
             "config=cluster:cluster1&&query=a"
             "&&distinct=dist_key:price,dist_count:2;none_dist"
             "&&rank_sort=sort:RANK,percent:40;sort:price,percent:60");
     ASSERT_TRUE(requestPtr);
     ASSERT_TRUE(!_requestValidator->validate(requestPtr));
     ASSERT_EQ(ERROR_RANK_SORT_CLAUSE_AND_DISTINCT_CLAUSE_CONFLICT,
                          _requestValidator->getErrorCode());

     requestPtr = RequestCreator::prepareRequest(
             "config=cluster:cluster1&&query=a"
             "&&distinct=dist_key:price,dist_count:2;none_dist"
             "&&rank_sort=sort:RANK,percent:40");
     ASSERT_TRUE(requestPtr);
     ASSERT_TRUE(_requestValidator->validate(requestPtr));
     ASSERT_EQ(ERROR_NONE, _requestValidator->getErrorCode());

     requestPtr = RequestCreator::prepareRequest(
             "config=cluster:cluster1&&query=a"
             "&&distinct=none_dist;dist_key:price,dist_count:1"
             "&&rank_sort=sort:RANK,percent:40;sort:price,percent:60");
     ASSERT_TRUE(requestPtr);
     ASSERT_TRUE(_requestValidator->validate(requestPtr));
     ASSERT_EQ(ERROR_NONE, _requestValidator->getErrorCode());
 }

 TEST_F(RequestValidatorTest, testRankSortWithSubDoc) {
     {
         RequestPtr requestPtr = RequestCreator::prepareRequest(
                 "config=cluster:cluster1&&query=a"
                 "&&rank_sort=sort:RANK,percent:40;sort:sub_attr_int8,percent:60");
         ASSERT_TRUE(requestPtr);
         _requestValidator->validate(requestPtr);
         ASSERT_EQ(ERROR_SYNTAX_EXPRESSION_SUB_DOC_NOT_SUPPORT, _requestValidator->getErrorCode());
     }
     {
         RequestPtr requestPtr = RequestCreator::prepareRequest(
                 "config=cluster:cluster1,sub_doc:group&&query=a"
                 "&&rank_sort=sort:RANK,percent:40;sort:sub_attr_int8,percent:60");
         ASSERT_TRUE(requestPtr);
         _requestValidator->validate(requestPtr);
         ASSERT_EQ(ERROR_SYNTAX_EXPRESSION_SUB_DOC_NOT_SUPPORT, _requestValidator->getErrorCode());
     }
     {
        RequestPtr requestPtr = RequestCreator::prepareRequest(
                 "config=cluster:cluster1,sub_doc:flat&&query=a"
                 "&&rank_sort=sort:RANK,percent:40;sort:sub_attr_int8,percent:60");
         ASSERT_TRUE(requestPtr);
         _requestValidator->validate(requestPtr);
         ASSERT_EQ(ERROR_NONE, _requestValidator->getErrorCode());
     }
 }

 TEST_F(RequestValidatorTest, testPKFilterClause) {
     HA3_LOG(DEBUG, "Begin Test!");

     _requestPtr.reset(prepareSimpleRequest());
     ASSERT_TRUE(_requestPtr);
     ASSERT_TRUE(_requestValidator->validate(_requestPtr));
     ASSERT_EQ(ERROR_NONE, _requestValidator->getErrorCode());

     PKFilterClause *pkFilterClause = new PKFilterClause;
     pkFilterClause->setOriginalString("aaa");
     _requestPtr->setPKFilterClause(pkFilterClause);

     ASSERT_TRUE(_requestValidator->validate(_requestPtr));
     ASSERT_EQ(ERROR_NONE, _requestValidator->getErrorCode());
 }
 TEST_F(RequestValidatorTest, testPKFilterClauseWithError) {
     HA3_LOG(DEBUG, "Begin Test!");

     _requestPtr.reset(prepareSimpleRequest());
     ASSERT_TRUE(_requestPtr);
     ASSERT_TRUE(_requestValidator->validate(_requestPtr));
     ASSERT_EQ(ERROR_NONE, _requestValidator->getErrorCode());

     PKFilterClause *pkFilterClause = new PKFilterClause;
     pkFilterClause->setOriginalString("");
     _requestPtr->setPKFilterClause(pkFilterClause);

     ASSERT_TRUE(!_requestValidator->validate(_requestPtr));
     ASSERT_EQ(ERROR_PKFILTER_CLAUSE, _requestValidator->getErrorCode());

 }

 TEST_F(RequestValidatorTest, testValidateAttributeClause) {
     HA3_LOG(DEBUG, "Begin Test!");

     _requestPtr.reset(prepareSimpleRequest());
     ASSERT_TRUE(_requestPtr);
     {
         AttributeClause *attributeClause = new AttributeClause;

         _requestPtr->setAttributeClause(attributeClause);
         ASSERT_TRUE(_requestValidator->validate(_requestPtr));

         attributeClause->addAttribute("attr_int8");
         ASSERT_TRUE(_requestValidator->validate(_requestPtr));

         attributeClause->addAttribute("attr_not_exist");
         ASSERT_TRUE(!_requestValidator->validate(_requestPtr));
         ASSERT_EQ(ERROR_ATTRIBUTE_NOT_EXIST, _requestValidator->getErrorCode());
        ASSERT_EQ(string("attribute [attr_not_exist] not exist"),
                             _requestValidator->getErrorMsg());
    }
    {
        AttributeClause *attributeClause = new AttributeClause;
        _requestPtr->setAttributeClause(attributeClause);
        attributeClause->addAttribute("sub_attr_int8");
        ASSERT_TRUE(!_requestValidator->validate(_requestPtr));
        ASSERT_EQ(ERROR_ATTRIBUTE_EXIST_IN_SUB_DOC, _requestValidator->getErrorCode());
        ASSERT_EQ(string("attribute [sub_attr_int8] is exist in sub doc"),
                             _requestValidator->getErrorMsg());
    }
    {
        AttributeClause *attributeClause = new AttributeClause;
        ConfigClause *configClause = _requestPtr->getConfigClause();
        configClause->setSubDocDisplayType(SUB_DOC_DISPLAY_FLAT);
        _requestPtr->setAttributeClause(attributeClause);
        attributeClause->addAttribute("sub_attr_int8");
        ASSERT_TRUE(_requestValidator->validate(_requestPtr));
     }
}

TEST_F(RequestValidatorTest, testSortClauseError) {
    HA3_LOG(DEBUG, "Begin Test!");

    _requestPtr.reset(prepareSimpleRequest());
    ASSERT_TRUE(_requestPtr);

    AtomicSyntaxExpr *leftExpr
        = new AtomicSyntaxExpr("price", vt_unknown, ATTRIBUTE_NAME);
    AtomicSyntaxExpr *rightExpr
        = new AtomicSyntaxExpr("111", vt_unknown, INTEGER_VALUE);
    SyntaxExpr* syntaxExpr = new GreaterSyntaxExpr(leftExpr, rightExpr);

    SortDescription* sortDes = new SortDescription;
    sortDes->setExpressionType(SortDescription::RS_NORMAL);
    sortDes->setRootSyntaxExpr(syntaxExpr);

    SortClause *sortClause = new SortClause;
    sortClause->addSortDescription(sortDes);
    _requestPtr->setSortClause(sortClause);

    bool ret = _requestValidator->validate(_requestPtr);
    ASSERT_TRUE(!ret);
    ASSERT_EQ(ERROR_SYNTAX_EXPRESSION_TYPE_NOT_SUPPORT,
                         _requestValidator->getErrorCode());
    ASSERT_EQ(string("syntax location: SORT_SYNTAX_LOCATION"),
                         _requestValidator->getErrorMsg());
}

TEST_F(RequestValidatorTest, testSortClauseErrorWithSameExpr) {
    HA3_LOG(DEBUG, "Begin Test!");

    _requestPtr.reset(prepareSimpleRequest());
    ASSERT_TRUE(_requestPtr);

    AtomicSyntaxExpr *leftExpr
        = new AtomicSyntaxExpr("price", vt_unknown, ATTRIBUTE_NAME);
    AtomicSyntaxExpr *rightExpr
        = new AtomicSyntaxExpr("price", vt_unknown, ATTRIBUTE_NAME);

    SortClause *sortClause = new SortClause;
    sortClause->addSortDescription(leftExpr, false, false);
    sortClause->addSortDescription(rightExpr, false, true);
    _requestPtr->setSortClause(sortClause);

    _requestValidator->validate(_requestPtr);
    ASSERT_EQ(ERROR_SORT_SAME_EXPRESSION, _requestValidator->getErrorCode());
    ASSERT_EQ(string("two expression [price]"), _requestValidator->getErrorMsg());
}

TEST_F(RequestValidatorTest, testSortClauseWithSubDoc) {
    {
        _requestPtr.reset(prepareSimpleRequest());
        ASSERT_TRUE(_requestPtr);
        SortClause *sortClause = new SortClause;
        AtomicSyntaxExpr *leftExpr
            = new AtomicSyntaxExpr("sub_attr_int8", vt_unknown, ATTRIBUTE_NAME);
        sortClause->addSortDescription(leftExpr, false, false);
        _requestPtr->setSortClause(sortClause);
        _requestValidator->validate(_requestPtr);
        ASSERT_EQ(ERROR_SYNTAX_EXPRESSION_SUB_DOC_NOT_SUPPORT,
                             _requestValidator->getErrorCode());
    }
    {
        _requestPtr.reset(prepareSimpleRequest());
        ASSERT_TRUE(_requestPtr);
        ConfigClause *configClause = _requestPtr->getConfigClause();
        configClause->setSubDocDisplayType(SUB_DOC_DISPLAY_GROUP);
        SortClause *sortClause = new SortClause;
        AtomicSyntaxExpr *leftExpr
            = new AtomicSyntaxExpr("sub_attr_int8", vt_unknown, ATTRIBUTE_NAME);
        sortClause->addSortDescription(leftExpr, false, false);
        _requestPtr->setSortClause(sortClause);
        _requestValidator->validate(_requestPtr);
        ASSERT_EQ(ERROR_SYNTAX_EXPRESSION_SUB_DOC_NOT_SUPPORT,
                             _requestValidator->getErrorCode());
    }
    {
        _requestPtr.reset(prepareSimpleRequest());
        ASSERT_TRUE(_requestPtr);
        ConfigClause *configClause = _requestPtr->getConfigClause();
        configClause->setSubDocDisplayType(SUB_DOC_DISPLAY_FLAT);
        SortClause *sortClause = new SortClause;
        AtomicSyntaxExpr *leftExpr
            = new AtomicSyntaxExpr("sub_attr_int8", vt_unknown, ATTRIBUTE_NAME);
        sortClause->addSortDescription(leftExpr, false, false);
        _requestPtr->setSortClause(sortClause);
        _requestValidator->validate(_requestPtr);
        ASSERT_EQ(ERROR_NONE,
                             _requestValidator->getErrorCode());
    }
}

TEST_F(RequestValidatorTest, testSortClauseErrorWithTwoRank) {
    HA3_LOG(DEBUG, "Begin Test!");

    _requestPtr.reset(prepareSimpleRequest());
    ASSERT_TRUE(_requestPtr);

    SortClause *sortClause = new SortClause;
    sortClause->addSortDescription(NULL, true, false);
    sortClause->addSortDescription(NULL, true, true);
    _requestPtr->setSortClause(sortClause);

    _requestValidator->validate(_requestPtr);
    ASSERT_EQ(ERROR_SORT_SAME_EXPRESSION, _requestValidator->getErrorCode());
    ASSERT_EQ(string("two rank expression"), _requestValidator->getErrorMsg());
}

TEST_F(RequestValidatorTest, testSortClauseWithMultiValue) {
    HA3_LOG(DEBUG, "Begin Test!");

    _requestPtr.reset(prepareSimpleRequest());
    ASSERT_TRUE(_requestPtr);

    AtomicSyntaxExpr *expr
        = new AtomicSyntaxExpr("multi_attr_1", vt_unknown, ATTRIBUTE_NAME);

    SortDescription* sortDes = new SortDescription;
    sortDes->setExpressionType(SortDescription::RS_NORMAL);
    sortDes->setRootSyntaxExpr(expr);

    SortClause *sortClause = new SortClause;
    sortClause->addSortDescription(sortDes);
    _requestPtr->setSortClause(sortClause);

    bool ret = _requestValidator->validate(_requestPtr);
    ASSERT_TRUE(!ret);
    ASSERT_EQ(ERROR_SYNTAX_EXPRESSION_TYPE_NOT_SUPPORT,
                         _requestValidator->getErrorCode());
}

TEST_F(RequestValidatorTest, testDistinctClauseError) {
    HA3_LOG(DEBUG, "Begin Test!");

    _requestPtr.reset(prepareSimpleRequest());
    ASSERT_TRUE(_requestPtr);
    bool ret = _requestValidator->validate(_requestPtr);
    ASSERT_TRUE(ret);

    AtomicSyntaxExpr *syntaxExpr
        = new AtomicSyntaxExpr("not_exist_attr", vt_unknown, ATTRIBUTE_NAME);
    DistinctDescription *distDesc = new DistinctDescription(DEFAULT_DISTINCT_MODULE_NAME,
            "", 1, 1, 0, true, false, syntaxExpr, NULL);
    DistinctClause *distinctClause = new DistinctClause;
    distinctClause->addDistinctDescription(distDesc);
    _requestPtr->setDistinctClause(distinctClause);

    ret = _requestValidator->validate(_requestPtr);
    ASSERT_TRUE(!ret);
    ASSERT_EQ(ERROR_ATTRIBUTE_NOT_EXIST,
                         _requestValidator->getErrorCode());
    string errorMsg = "attribute [not_exist_attr] not exist";
    ASSERT_EQ(errorMsg, _requestValidator->getErrorMsg());
}

TEST_F(RequestValidatorTest, testDistinctClauseErrorWithFloat) {
    HA3_LOG(DEBUG, "Begin Test!");

    _requestPtr.reset(prepareSimpleRequest());
    ASSERT_TRUE(_requestPtr);

    AtomicSyntaxExpr *syntaxExpr
        = new AtomicSyntaxExpr("attr_float", vt_unknown, ATTRIBUTE_NAME);
    DistinctDescription *distDesc = new DistinctDescription(DEFAULT_DISTINCT_MODULE_NAME,
            "", 1, 1, 0, true, false, syntaxExpr, NULL);
    DistinctClause *distinctClause = new DistinctClause;
    distinctClause->addDistinctDescription(distDesc);
    _requestPtr->setDistinctClause(distinctClause);

    bool ret = _requestValidator->validate(_requestPtr);
    ASSERT_TRUE(!ret);
    ASSERT_EQ(ERROR_SYNTAX_EXPRESSION_TYPE_NOT_SUPPORT,
                         _requestValidator->getErrorCode());
    string errorMsg = string("syntax location: DISTINCT_SYNTAX_LOCATION");
    ASSERT_EQ(errorMsg, _requestValidator->getErrorMsg());
}

TEST_F(RequestValidatorTest, testDistinctClauseWithMultiValue) {
    HA3_LOG(DEBUG, "Begin Test!");

    _requestPtr.reset(prepareSimpleRequest());
    ASSERT_TRUE(_requestPtr);

    AtomicSyntaxExpr *syntaxExpr
        = new AtomicSyntaxExpr("multi_attr_1", vt_unknown, ATTRIBUTE_NAME);
    DistinctDescription *distDesc = new DistinctDescription(DEFAULT_DISTINCT_MODULE_NAME,
            "", 1, 1, 0, true, false, syntaxExpr, NULL);
    DistinctClause *distinctClause = new DistinctClause;
    distinctClause->addDistinctDescription(distDesc);

    _requestPtr->setDistinctClause(distinctClause);

    bool ret = _requestValidator->validate(_requestPtr);
    ASSERT_TRUE(ret);
    ASSERT_EQ(ERROR_NONE, _requestValidator->getErrorCode());
}

TEST_F(RequestValidatorTest, testDistinctClauseWithFilter) {
    HA3_LOG(DEBUG, "Begin Test!");

    _requestPtr.reset(prepareSimpleRequest());
    ASSERT_TRUE(_requestPtr);

    AtomicSyntaxExpr *syntaxExpr
        = new AtomicSyntaxExpr("price", vt_unknown, ATTRIBUTE_NAME);
    DistinctDescription *distDesc = new DistinctDescription(DEFAULT_DISTINCT_MODULE_NAME,
            "", 1, 1, 0, true, false, syntaxExpr, NULL);
    DistinctClause *distinctClause = new DistinctClause;
    distinctClause->addDistinctDescription(distDesc);
    _requestPtr->setDistinctClause(distinctClause);

    AtomicSyntaxExpr *leftExpr = new AtomicSyntaxExpr("price",
            vt_unknown, ATTRIBUTE_NAME);
    AtomicSyntaxExpr *rightExpr = new AtomicSyntaxExpr("3",
            vt_int32, INTEGER_VALUE);
    LessSyntaxExpr *checkFilterSyntaxExpr =
        new LessSyntaxExpr(leftExpr, rightExpr);
    FilterClause *filterClause = new FilterClause(checkFilterSyntaxExpr);
    filterClause->setOriginalString("price2<3");
    distDesc->setFilterClause(filterClause);

    bool ret = _requestValidator->validate(_requestPtr);

    ASSERT_TRUE(ret);
    ASSERT_EQ(ERROR_NONE, _requestValidator->getErrorCode());
    ASSERT_EQ(vt_int32, leftExpr->getExprResultType());
}

TEST_F(RequestValidatorTest, testValidateDistinctWithSubDocClause) {
    {
        //sub doc:no
        RequestPtr requestPtr = RequestCreator::prepareRequest(
                "config=cluster:cluster1&&query=a&&distinct=dist_key:sub_attr_int8,"
                "dist_count:2,dist_times:1,dist_filter:attr_int8>2;"
                "dist_key:attr_int32,dist_count:1,dist_times:1,dist_filter:attr_int32>1");
        bool ret = _requestValidator->validate(requestPtr);
        ASSERT_TRUE(!ret);
        ASSERT_EQ(ERROR_SYNTAX_EXPRESSION_SUB_DOC_NOT_SUPPORT,
                             _requestValidator->getErrorCode());

        requestPtr = RequestCreator::prepareRequest(
                "config=cluster:cluster1&&query=a&&distinct=dist_key:attr_int8,"
                "dist_count:2,dist_times:1,dist_filter:attr_int8>2;"
                "dist_key:sub_attr_int32,dist_count:1,dist_times:1,dist_filter:attr_int32>1");
        ret = _requestValidator->validate(requestPtr);
        ASSERT_TRUE(!ret);
        ASSERT_EQ(ERROR_SYNTAX_EXPRESSION_SUB_DOC_NOT_SUPPORT,
                             _requestValidator->getErrorCode());


        requestPtr = RequestCreator::prepareRequest(
                "config=cluster:cluster1&&query=a&&distinct=dist_key:attr_int8,"
                "dist_count:2,dist_times:1,dist_filter:sub_attr_int8>2;"
                "dist_key:attr_int32,dist_count:1,dist_times:1,dist_filter:attr_int32>1");
        ret = _requestValidator->validate(requestPtr);
        ASSERT_TRUE(!ret);
        ASSERT_EQ(ERROR_SYNTAX_EXPRESSION_SUB_DOC_NOT_SUPPORT,
                             _requestValidator->getErrorCode());

        requestPtr = RequestCreator::prepareRequest(
                "config=cluster:cluster1&&query=a&&distinct=dist_key:attr_int8,"
                "dist_count:2,dist_times:1,dist_filter:attr_int8>2;"
                "dist_key:attr_int32,dist_count:1,dist_times:1,dist_filter:sub_attr_int32>1");
        ret = _requestValidator->validate(requestPtr);
        ASSERT_TRUE(!ret);
        ASSERT_EQ(ERROR_SYNTAX_EXPRESSION_SUB_DOC_NOT_SUPPORT,
                             _requestValidator->getErrorCode());
    }
    {
        //sub doc:group
        RequestPtr requestPtr = RequestCreator::prepareRequest(
                "config=cluster:cluster1,sub_doc:group&&query=a&&distinct=dist_key:sub_attr_int8,"
                "dist_count:2,dist_times:1,dist_filter:attr_int8>2;"
                "dist_key:attr_int32,dist_count:1,dist_times:1,dist_filter:attr_int32>1");
        bool ret = _requestValidator->validate(requestPtr);
        ASSERT_TRUE(!ret);
        ASSERT_EQ(ERROR_SYNTAX_EXPRESSION_SUB_DOC_NOT_SUPPORT,
                             _requestValidator->getErrorCode());

        requestPtr = RequestCreator::prepareRequest(
                "config=cluster:cluster1,sub_doc:group&&query=a&&distinct=dist_key:attr_int8,"
                "dist_count:2,dist_times:1,dist_filter:attr_int8>2;"
                "dist_key:sub_attr_int32,dist_count:1,dist_times:1,dist_filter:attr_int32>1");
        ret = _requestValidator->validate(requestPtr);
        ASSERT_TRUE(!ret);
        ASSERT_EQ(ERROR_SYNTAX_EXPRESSION_SUB_DOC_NOT_SUPPORT,
                             _requestValidator->getErrorCode());


        requestPtr = RequestCreator::prepareRequest(
                "config=cluster:cluster1,sub_doc:group&&query=a&&distinct=dist_key:attr_int8,"
                "dist_count:2,dist_times:1,dist_filter:sub_attr_int8>2;"
                "dist_key:attr_int32,dist_count:1,dist_times:1,dist_filter:attr_int32>1");
        ret = _requestValidator->validate(requestPtr);
        ASSERT_TRUE(!ret);
        ASSERT_EQ(ERROR_SYNTAX_EXPRESSION_SUB_DOC_NOT_SUPPORT,
                             _requestValidator->getErrorCode());

        requestPtr = RequestCreator::prepareRequest(
                "config=cluster:cluster1,sub_doc:group&&query=a&&distinct=dist_key:attr_int8,"
                "dist_count:2,dist_times:1,dist_filter:attr_int8>2;"
                "dist_key:attr_int32,dist_count:1,dist_times:1,dist_filter:sub_attr_int32>1");
        ret = _requestValidator->validate(requestPtr);
        ASSERT_TRUE(!ret);
        ASSERT_EQ(ERROR_SYNTAX_EXPRESSION_SUB_DOC_NOT_SUPPORT,
                             _requestValidator->getErrorCode());
    }
    {
        //sub doc:flat
        RequestPtr requestPtr = RequestCreator::prepareRequest(
                "config=cluster:cluster1,sub_doc:flat&&query=a&&distinct=dist_key:sub_attr_int8,"
                "dist_count:2,dist_times:1,dist_filter:attr_int8>2;"
                "dist_key:attr_int32,dist_count:1,dist_times:1,dist_filter:attr_int32>1");
        bool ret = _requestValidator->validate(requestPtr);
        ASSERT_TRUE(ret);
        ASSERT_EQ(ERROR_NONE, _requestValidator->getErrorCode());

        requestPtr = RequestCreator::prepareRequest(
                "config=cluster:cluster1,sub_doc:flat&&query=a&&distinct=dist_key:attr_int8,"
                "dist_count:2,dist_times:1,dist_filter:attr_int8>2;"
                "dist_key:sub_attr_int32,dist_count:1,dist_times:1,dist_filter:attr_int32>1");
        ret = _requestValidator->validate(requestPtr);
        ASSERT_TRUE(ret);
        ASSERT_EQ(ERROR_NONE, _requestValidator->getErrorCode());

        requestPtr = RequestCreator::prepareRequest(
                "config=cluster:cluster1,sub_doc:flat&&query=a&&distinct=dist_key:attr_int8,"
                "dist_count:2,dist_times:1,dist_filter:sub_attr_int8>2;"
                "dist_key:attr_int32,dist_count:1,dist_times:1,dist_filter:attr_int32>1");
        ret = _requestValidator->validate(requestPtr);
        ASSERT_TRUE(ret);
        ASSERT_EQ(ERROR_NONE, _requestValidator->getErrorCode());

        requestPtr = RequestCreator::prepareRequest(
                "config=cluster:cluster1,sub_doc:flat&&query=a&&distinct=dist_key:attr_int8,"
                "dist_count:2,dist_times:1,dist_filter:attr_int8>2;"
                "dist_key:attr_int32,dist_count:1,dist_times:1,dist_filter:sub_attr_int32>1");
        ret = _requestValidator->validate(requestPtr);
        ASSERT_TRUE(ret);
        ASSERT_EQ(ERROR_NONE, _requestValidator->getErrorCode());
    }
}

TEST_F(RequestValidatorTest, testDistinctClauseWithFilterUseSubDoc) {
    HA3_LOG(DEBUG, "Begin Test!");
    {
        _requestPtr.reset(prepareSimpleRequest());
        ASSERT_TRUE(_requestPtr);
        ConfigClause *configClause = _requestPtr->getConfigClause();
        configClause->setSubDocDisplayType(SUB_DOC_DISPLAY_GROUP);

        AtomicSyntaxExpr *syntaxExpr
            = new AtomicSyntaxExpr("price", vt_unknown, ATTRIBUTE_NAME);
        DistinctDescription *distDesc = new DistinctDescription(DEFAULT_DISTINCT_MODULE_NAME,
                "", 1, 1, 0, true, false, syntaxExpr, NULL);
        DistinctClause *distinctClause = new DistinctClause;
        distinctClause->addDistinctDescription(distDesc);
        _requestPtr->setDistinctClause(distinctClause);

        AtomicSyntaxExpr *leftExpr = new AtomicSyntaxExpr("sub_attr_int8",
                vt_unknown, ATTRIBUTE_NAME);
        AtomicSyntaxExpr *rightExpr = new AtomicSyntaxExpr("3",
                vt_int32, INTEGER_VALUE);
        LessSyntaxExpr *checkFilterSyntaxExpr =
            new LessSyntaxExpr(leftExpr, rightExpr);
        FilterClause *filterClause = new FilterClause(checkFilterSyntaxExpr);
        filterClause->setOriginalString("sub_attr_int8<3");
        distDesc->setFilterClause(filterClause);

        bool ret = _requestValidator->validate(_requestPtr);
        ASSERT_TRUE(!ret);

        ASSERT_EQ(ERROR_SYNTAX_EXPRESSION_SUB_DOC_NOT_SUPPORT,
                             _requestValidator->getErrorCode());
        string errorMsg = "syntax location: DISTINCT_FILTER_SYNTAX_LOCATION";
        ASSERT_EQ(errorMsg, _requestValidator->getErrorMsg());
    }
    {
        _requestPtr.reset(prepareSimpleRequest());
        ASSERT_TRUE(_requestPtr);
        ConfigClause *configClause = _requestPtr->getConfigClause();
        configClause->setSubDocDisplayType(SUB_DOC_DISPLAY_FLAT);
        AtomicSyntaxExpr *syntaxExpr
            = new AtomicSyntaxExpr("price", vt_unknown, ATTRIBUTE_NAME);
        DistinctDescription *distDesc = new DistinctDescription(DEFAULT_DISTINCT_MODULE_NAME,
                "", 1, 1, 0, true, false, syntaxExpr, NULL);
        DistinctClause *distinctClause = new DistinctClause;
        distinctClause->addDistinctDescription(distDesc);
        _requestPtr->setDistinctClause(distinctClause);

        AtomicSyntaxExpr *leftExpr = new AtomicSyntaxExpr("sub_attr_int8",
                vt_unknown, ATTRIBUTE_NAME);
        AtomicSyntaxExpr *rightExpr = new AtomicSyntaxExpr("3",
                vt_int32, INTEGER_VALUE);
        LessSyntaxExpr *checkFilterSyntaxExpr =
            new LessSyntaxExpr(leftExpr, rightExpr);
        FilterClause *filterClause = new FilterClause(checkFilterSyntaxExpr);
        filterClause->setOriginalString("sub_attr_int8<3");
        distDesc->setFilterClause(filterClause);

        bool ret = _requestValidator->validate(_requestPtr);
        ASSERT_TRUE(ret);
        ASSERT_EQ(ERROR_NONE,
                             _requestValidator->getErrorCode());
    }
}

TEST_F(RequestValidatorTest, testDistinctClauseWithFilterError) {
    HA3_LOG(DEBUG, "Begin Test!");

    _requestPtr.reset(prepareSimpleRequest());
    ASSERT_TRUE(_requestPtr);

    AtomicSyntaxExpr *syntaxExpr
        = new AtomicSyntaxExpr("price", vt_unknown, ATTRIBUTE_NAME);
    DistinctDescription *distDesc = new DistinctDescription(DEFAULT_DISTINCT_MODULE_NAME,
            "", 1, 1, 0, true, false, syntaxExpr, NULL);
    DistinctClause *distinctClause = new DistinctClause;
    distinctClause->addDistinctDescription(distDesc);
    _requestPtr->setDistinctClause(distinctClause);

    AtomicSyntaxExpr *expr = new AtomicSyntaxExpr("price",
            vt_unknown, ATTRIBUTE_NAME);
    FilterClause *filterClause = new FilterClause(expr);
    filterClause->setOriginalString("price");
    distDesc->setFilterClause(filterClause);

    bool ret = _requestValidator->validate(_requestPtr);
    ASSERT_TRUE(!ret);
    ASSERT_EQ(ERROR_SYNTAX_EXPRESSION_TYPE_NOT_SUPPORT,
                         _requestValidator->getErrorCode());
    ASSERT_EQ(string("syntax location: DISTINCT_FILTER_SYNTAX_LOCATION"),
                         _requestValidator->getErrorMsg());
}


TEST_F(RequestValidatorTest, testMultiDistinctClauses) {
    HA3_LOG(DEBUG, "Begin Test!");

    _requestPtr.reset(prepareSimpleRequest());
    ASSERT_TRUE(_requestPtr);

    AtomicSyntaxExpr *syntaxExpr1
        = new AtomicSyntaxExpr("price", vt_unknown, ATTRIBUTE_NAME);
    DistinctDescription *distDesc1 = new DistinctDescription(DEFAULT_DISTINCT_MODULE_NAME,
            "", 1, 1, 0, true, false, syntaxExpr1, NULL);

    AtomicSyntaxExpr *syntaxExpr2
        = new AtomicSyntaxExpr("attr_int32", vt_unknown, ATTRIBUTE_NAME);
    DistinctDescription *distDesc2 = new DistinctDescription(DEFAULT_DISTINCT_MODULE_NAME,
            "", 1, 1, 0, true, false, syntaxExpr2, NULL);

    DistinctClause *distinctClause = new DistinctClause;
    distinctClause->addDistinctDescription(distDesc1);
    distinctClause->addDistinctDescription(distDesc2);
    _requestPtr->setDistinctClause(distinctClause);

    bool ret = _requestValidator->validate(_requestPtr);
    ASSERT_TRUE(ret);
    ASSERT_EQ(ERROR_NONE, _requestValidator->getErrorCode());
    ASSERT_EQ(vt_int32, syntaxExpr1->getExprResultType());
    ASSERT_EQ(vt_int32, syntaxExpr2->getExprResultType());
}

TEST_F(RequestValidatorTest, testMultiDistinctClauses2) {
    HA3_LOG(DEBUG, "Begin Test!");

    _requestPtr.reset(prepareSimpleRequest());
    ASSERT_TRUE(_requestPtr);

    DistinctDescription *distDesc1 = NULL;

    AtomicSyntaxExpr *syntaxExpr2
        = new AtomicSyntaxExpr("attr_int32", vt_unknown, ATTRIBUTE_NAME);
    DistinctDescription *distDesc2 = new DistinctDescription(DEFAULT_DISTINCT_MODULE_NAME,
            "", 1, 1, 0, true, false, syntaxExpr2, NULL);

    DistinctClause *distinctClause = new DistinctClause;
    distinctClause->addDistinctDescription(distDesc1);
    distinctClause->addDistinctDescription(distDesc2);
    _requestPtr->setDistinctClause(distinctClause);

    bool ret = _requestValidator->validate(_requestPtr);
    ASSERT_TRUE(ret);
    ASSERT_EQ(ERROR_NONE, _requestValidator->getErrorCode());
    ASSERT_EQ(vt_int32, syntaxExpr2->getExprResultType());
}

TEST_F(RequestValidatorTest, testMultiDistinctClausesWithError) {
    HA3_LOG(DEBUG, "Begin Test!");

    _requestPtr.reset(prepareSimpleRequest());
    ASSERT_TRUE(_requestPtr);

    AtomicSyntaxExpr *syntaxExpr1
        = new AtomicSyntaxExpr("price", vt_unknown, ATTRIBUTE_NAME);
    DistinctDescription *distDesc1 = new DistinctDescription(DEFAULT_DISTINCT_MODULE_NAME,
            "", 1, 1, 0, true, false, syntaxExpr1, NULL);
    AtomicSyntaxExpr *syntaxExpr2
        = new AtomicSyntaxExpr("attr_float", vt_unknown, ATTRIBUTE_NAME);
    DistinctDescription *distDesc2 = new DistinctDescription(DEFAULT_DISTINCT_MODULE_NAME,
            "", 1, 1, 0, true, false, syntaxExpr2, NULL);

    DistinctClause *distinctClause = new DistinctClause;
    distinctClause->addDistinctDescription(distDesc1);
    distinctClause->addDistinctDescription(distDesc2);

    _requestPtr->setDistinctClause(distinctClause);

    bool ret = _requestValidator->validate(_requestPtr);
    ASSERT_TRUE(!ret);
    ASSERT_EQ(ERROR_SYNTAX_EXPRESSION_TYPE_NOT_SUPPORT,
                         _requestValidator->getErrorCode());
    ASSERT_EQ(string("syntax location: DISTINCT_SYNTAX_LOCATION"),
                         _requestValidator->getErrorMsg());
}

TEST_F(RequestValidatorTest, testMultiDistinctClausesWithError2) {
    HA3_LOG(DEBUG, "Begin Test!");

    _requestPtr.reset(prepareSimpleRequest());
    ASSERT_TRUE(_requestPtr);

    DistinctDescription *distDesc1 = NULL;
    AtomicSyntaxExpr *syntaxExpr2
        = new AtomicSyntaxExpr("attr_float", vt_unknown, ATTRIBUTE_NAME);
    DistinctDescription *distDesc2 = new DistinctDescription(DEFAULT_DISTINCT_MODULE_NAME,
            "", 1, 1, 0, true, false, syntaxExpr2, NULL);

    DistinctClause *distinctClause = new DistinctClause;
    distinctClause->addDistinctDescription(distDesc1);
    distinctClause->addDistinctDescription(distDesc2);

    _requestPtr->setDistinctClause(distinctClause);

    bool ret = _requestValidator->validate(_requestPtr);
    ASSERT_TRUE(!ret);
    ASSERT_EQ(ERROR_SYNTAX_EXPRESSION_TYPE_NOT_SUPPORT,
                         _requestValidator->getErrorCode());
    ASSERT_EQ(string("syntax location: DISTINCT_SYNTAX_LOCATION"),
                         _requestValidator->getErrorMsg());
}

TEST_F(RequestValidatorTest, testMultiDistinctClausesWithDistDescCountError1) {
    HA3_LOG(DEBUG, "Begin Test!");

    _requestPtr.reset(prepareSimpleRequest());
    ASSERT_TRUE(_requestPtr);
    DistinctClause *distinctClause = new DistinctClause;
    _requestPtr->setDistinctClause(distinctClause);

    bool ret = _requestValidator->validate(_requestPtr);
    ASSERT_TRUE(!ret);

    ASSERT_EQ(ERROR_DISTINCT_DESC_COUNT,
                         _requestValidator->getErrorCode());
    ASSERT_EQ(string("wrong distinct description count, "
                                "it must be >0 and <=2."),
                         _requestValidator->getErrorMsg());

    distinctClause->addDistinctDescription(NULL);
    ret = _requestValidator->validate(_requestPtr);
    ASSERT_TRUE(!ret);
    ASSERT_EQ(ERROR_DISTINCT_DESC_COUNT,
                         _requestValidator->getErrorCode());
    ASSERT_EQ(string("no effective distinct description , "
                                "it must be one at least."),
                         _requestValidator->getErrorMsg());

    distinctClause->addDistinctDescription(NULL);
    ret = _requestValidator->validate(_requestPtr);
    ASSERT_TRUE(!ret);
    ASSERT_EQ(ERROR_DISTINCT_DESC_COUNT,
                         _requestValidator->getErrorCode());
    ASSERT_EQ(string("no effective distinct description , "
                                "it must be one at least."),
                         _requestValidator->getErrorMsg());

    distinctClause->addDistinctDescription(NULL);
    ret = _requestValidator->validate(_requestPtr);
    ASSERT_TRUE(!ret);
    ASSERT_EQ(ERROR_DISTINCT_DESC_COUNT,
                         _requestValidator->getErrorCode());
    ASSERT_EQ(string("wrong distinct description count, "
                                "it must be >0 and <=2."),
                         _requestValidator->getErrorMsg());
}

TEST_F(RequestValidatorTest, testDistinctClauseInvalidDistCountError) {
    HA3_LOG(DEBUG, "Begin Test!");

    _requestPtr.reset(prepareSimpleRequest());
    ASSERT_TRUE(_requestPtr);

    AtomicSyntaxExpr *syntaxExpr
        = new AtomicSyntaxExpr("type", vt_unknown, ATTRIBUTE_NAME);
    DistinctDescription *distDesc = new DistinctDescription(DEFAULT_DISTINCT_MODULE_NAME,
            "", 1, 1, 0, true, false, syntaxExpr, NULL);
    distDesc->setDistinctCount(-1);
    distDesc->setDistinctTimes(1);

    DistinctClause *distinctClause = new DistinctClause;
    distinctClause->addDistinctDescription(distDesc);
    _requestPtr->setDistinctClause(distinctClause);

    bool ret = _requestValidator->validate(_requestPtr);
    ASSERT_TRUE(!ret);
    ASSERT_EQ(ERROR_DISTINCT_COUNT,
                         _requestValidator->getErrorCode());

    distDesc->setDistinctCount(0);
    distDesc->setDistinctTimes(1);
    ret = _requestValidator->validate(_requestPtr);
    ASSERT_TRUE(!ret);
    ASSERT_EQ(ERROR_DISTINCT_COUNT,
                         _requestValidator->getErrorCode());
}

TEST_F(RequestValidatorTest, testDistinctClauseInvalidDistTimesError) {
    HA3_LOG(DEBUG, "Begin Test!");

    _requestPtr.reset(prepareSimpleRequest());
    ASSERT_TRUE(_requestPtr);

    AtomicSyntaxExpr *syntaxExpr
        = new AtomicSyntaxExpr("type", vt_unknown, ATTRIBUTE_NAME);
    DistinctDescription *distDesc = new DistinctDescription(DEFAULT_DISTINCT_MODULE_NAME,
            "", 1, 1, 0, true, false, syntaxExpr, NULL);
    distDesc->setDistinctCount(1);
    distDesc->setDistinctTimes(-1);

    DistinctClause *distinctClause = new DistinctClause;
    distinctClause->addDistinctDescription(distDesc);
    _requestPtr->setDistinctClause(distinctClause);

    bool ret = _requestValidator->validate(_requestPtr);
    ASSERT_TRUE(!ret);
    ASSERT_EQ(ERROR_DISTINCT_TIMES,
                         _requestValidator->getErrorCode());

    distDesc->setDistinctCount(1);
    distDesc->setDistinctTimes(0);
    ret = _requestValidator->validate(_requestPtr);
    ASSERT_TRUE(!ret);
    ASSERT_EQ(ERROR_DISTINCT_TIMES,
                         _requestValidator->getErrorCode());
}

TEST_F(RequestValidatorTest, testDistinctClauseWithConstValueExpr) {
    HA3_LOG(DEBUG, "Begin Test!");

    _requestPtr.reset(prepareSimpleRequest());
    ASSERT_TRUE(_requestPtr);

    AtomicSyntaxExpr *syntaxExpr
        = new AtomicSyntaxExpr("111", vt_unknown, INTEGER_VALUE);
    DistinctDescription *distDesc = new DistinctDescription(DEFAULT_DISTINCT_MODULE_NAME,
            "", 1, 1, 0, true, false, syntaxExpr, NULL);
    DistinctClause *distinctClause = new DistinctClause;
    distinctClause->addDistinctDescription(distDesc);
    _requestPtr->setDistinctClause(distinctClause);

    bool ret = _requestValidator->validate(_requestPtr);
    ASSERT_TRUE(ret);
}

TEST_F(RequestValidatorTest, testDistinctClauseWithBoolExprError) {
    HA3_LOG(DEBUG, "Begin Test!");

    _requestPtr.reset(prepareSimpleRequest());
    ASSERT_TRUE(_requestPtr);

    AtomicSyntaxExpr *leftExpr
        = new AtomicSyntaxExpr("price", vt_unknown, ATTRIBUTE_NAME);
    AtomicSyntaxExpr *rightExpr
        = new AtomicSyntaxExpr("222", vt_unknown, INTEGER_VALUE);
    SyntaxExpr *syntaxExpr = new EqualSyntaxExpr(leftExpr, rightExpr);

    DistinctDescription *distDesc = new DistinctDescription(DEFAULT_DISTINCT_MODULE_NAME,
            "", 1, 1, 0, true, false, syntaxExpr, NULL);
    DistinctClause *distinctClause = new DistinctClause;
    distinctClause->addDistinctDescription(distDesc);
    _requestPtr->setDistinctClause(distinctClause);

    bool ret = _requestValidator->validate(_requestPtr);
    ASSERT_TRUE(!ret);
    ASSERT_EQ(ERROR_SYNTAX_EXPRESSION_TYPE_NOT_SUPPORT,
                         _requestValidator->getErrorCode());
    ASSERT_EQ(string("syntax location: DISTINCT_SYNTAX_LOCATION"),
                         _requestValidator->getErrorMsg());
}

TEST_F(RequestValidatorTest, testAggregateClauseGroupKeyExprError) {
    HA3_LOG(DEBUG, "Begin Test!");

    _requestPtr.reset(prepareSimpleRequest());
    ASSERT_TRUE(_requestPtr);

    AtomicSyntaxExpr *syntaxExpr
        = new AtomicSyntaxExpr("not_exist_attr", vt_unknown, ATTRIBUTE_NAME);
    AggregateDescription* aggDes = new AggregateDescription;
    aggDes->setGroupKeyExpr(syntaxExpr);

    AggregateClause *aggregateClause = new AggregateClause();
    aggregateClause->addAggDescription(aggDes);

    _requestPtr->setAggregateClause(aggregateClause);

    bool ret = _requestValidator->validate(_requestPtr);
    ASSERT_TRUE(!ret);
    ASSERT_EQ(ERROR_ATTRIBUTE_NOT_EXIST,
                         _requestValidator->getErrorCode());
    string errorMsg = "attribute [not_exist_attr] not exist";
    ASSERT_EQ(errorMsg, _requestValidator->getErrorMsg());
}

TEST_F(RequestValidatorTest, testAggregateClauseSubDocGroupKey) {
    HA3_LOG(DEBUG, "Begin Test!");

    _requestPtr.reset(prepareSimpleRequest());
    ASSERT_TRUE(_requestPtr);

    AtomicSyntaxExpr *syntaxExpr
        = new AtomicSyntaxExpr("sub_attr_int8", vt_unknown, ATTRIBUTE_NAME);
    ConfigClause *configClause = _requestPtr->getConfigClause();
    configClause->setSubDocDisplayType(SUB_DOC_DISPLAY_FLAT);
    ASSERT_TRUE(_requestValidator->validate(_requestPtr));

    AggregateDescription* aggDes = new AggregateDescription;
    aggDes->setGroupKeyExpr(syntaxExpr);
    AtomicSyntaxExpr *aggFunSyntax = new AtomicSyntaxExpr(
            "sub_attr_int8", vt_unknown, ATTRIBUTE_NAME);
    AggFunDescription *aggFuncDes = new AggFunDescription("min", aggFunSyntax);
    aggDes->appendAggFunDescription(aggFuncDes);

    AggregateClause *aggregateClause = new AggregateClause();
    aggregateClause->addAggDescription(aggDes);

    _requestPtr->setAggregateClause(aggregateClause);

    bool ret = _requestValidator->validate(_requestPtr);
    ASSERT_TRUE(ret);
    ASSERT_EQ(SYNTAX_EXPR_TYPE_ATOMIC_ATTR, syntaxExpr->getSyntaxExprType());
    ASSERT_EQ(vt_int8, syntaxExpr->getExprResultType());
    ASSERT_TRUE(syntaxExpr->isSubExpression());
}

TEST_F(RequestValidatorTest, testAggregateClauseAggFunResultTypeError) {
   HA3_LOG(DEBUG, "Begin Test!");

    _requestPtr.reset(prepareSimpleRequest());
    ASSERT_TRUE(_requestPtr);

    //make AggregateDescription
    AggregateDescription* aggDes = new AggregateDescription;
    AtomicSyntaxExpr *groupKeyExpr
        = new AtomicSyntaxExpr("type", vt_unknown, ATTRIBUTE_NAME);
    aggDes->setGroupKeyExpr(groupKeyExpr);

    //make AggFunDescription
    AtomicSyntaxExpr *funLeftExpr = new AtomicSyntaxExpr("price", vt_unknown, ATTRIBUTE_NAME);
    AtomicSyntaxExpr *funRightExpr = new AtomicSyntaxExpr("111", vt_unknown, INTEGER_VALUE);
    GreaterSyntaxExpr *aggFunExpr = new GreaterSyntaxExpr(funLeftExpr, funRightExpr);
    AggFunDescription *aggFunDes = new AggFunDescription("min", aggFunExpr);
    aggDes->appendAggFunDescription(aggFunDes);

    AggregateClause *aggregateClause = new AggregateClause();
    aggregateClause->addAggDescription(aggDes);
    _requestPtr->setAggregateClause(aggregateClause);

    bool ret = _requestValidator->validate(_requestPtr);
    ASSERT_TRUE(!ret);

    ASSERT_EQ(ERROR_SYNTAX_EXPRESSION_TYPE_NOT_SUPPORT,
                         _requestValidator->getErrorCode());
    ASSERT_EQ(string("syntax location: AGGFUN_SYNTAX_LOCATION"), _requestValidator->getErrorMsg());
}

TEST_F(RequestValidatorTest, testAggregateClauseAggFunUseSubDoc) {
   HA3_LOG(DEBUG, "Begin Test!");

    _requestPtr.reset(prepareSimpleRequest());
    ASSERT_TRUE(_requestPtr);
    ConfigClause *configClause = _requestPtr->getConfigClause();
    configClause->setSubDocDisplayType(SUB_DOC_DISPLAY_GROUP);

    //make AggregateDescription
    AggregateDescription* aggDes = new AggregateDescription;
    AtomicSyntaxExpr *groupKeyExpr
        = new AtomicSyntaxExpr("sub_attr_int8", vt_unknown, ATTRIBUTE_NAME);
    aggDes->setGroupKeyExpr(groupKeyExpr);

    //make AggFunDescription
    AtomicSyntaxExpr *aggFunExpr = new AtomicSyntaxExpr("sub_attr_int8", vt_unknown, ATTRIBUTE_NAME);
    AggFunDescription *aggFunDes = new AggFunDescription("min", aggFunExpr);
    aggDes->appendAggFunDescription(aggFunDes);

    AggregateClause *aggregateClause = new AggregateClause();
    aggregateClause->addAggDescription(aggDes);
    _requestPtr->setAggregateClause(aggregateClause);

    bool ret = _requestValidator->validate(_requestPtr);
    ASSERT_TRUE(ret);
    ASSERT_EQ(SYNTAX_EXPR_TYPE_ATOMIC_ATTR, aggFunExpr->getSyntaxExprType());
    ASSERT_EQ(vt_int8, aggFunExpr->getExprResultType());
    ASSERT_TRUE(aggFunExpr->isSubExpression());
}

TEST_F(RequestValidatorTest, testAggregateClauseAggFunNameError) {
   HA3_LOG(DEBUG, "Begin Test!");

    _requestPtr.reset(prepareSimpleRequest());
    ASSERT_TRUE(_requestPtr);

    //make AggregateDescription
    AggregateDescription* aggDes = new AggregateDescription;
    AtomicSyntaxExpr *groupKeyExpr
        = new AtomicSyntaxExpr("type", vt_unknown, ATTRIBUTE_NAME);
    aggDes->setGroupKeyExpr(groupKeyExpr);

    //make AggFunDescription
    AtomicSyntaxExpr *aggFunExpr = new AtomicSyntaxExpr("price", vt_unknown, ATTRIBUTE_NAME);
    AggFunDescription *aggFunDes = new AggFunDescription("no_such_fun", aggFunExpr);
    aggDes->appendAggFunDescription(aggFunDes);

    AggregateClause *aggregateClause = new AggregateClause();
    aggregateClause->addAggDescription(aggDes);
    _requestPtr->setAggregateClause(aggregateClause);

    bool ret = _requestValidator->validate(_requestPtr);
    ASSERT_TRUE(!ret);

    ASSERT_EQ(ERROR_AGG_FUN_NAME, _requestValidator->getErrorCode());
    string errorMsg = string(":not supported aggregate function name[no_such_fun]");
    ASSERT_EQ(errorMsg, _requestValidator->getErrorMsg());
}

TEST_F(RequestValidatorTest, testAggregateClauseRangeOrderError) {
   HA3_LOG(DEBUG, "Begin Test!");

    _requestPtr.reset(prepareSimpleRequest());
    ASSERT_TRUE(_requestPtr);

    //make AggregateDescription
    AggregateDescription* aggDes = new AggregateDescription;
    AtomicSyntaxExpr *groupKeyExpr
        = new AtomicSyntaxExpr("type", vt_unknown, ATTRIBUTE_NAME);
    aggDes->setGroupKeyExpr(groupKeyExpr);

    //make AggFunDescription
    AtomicSyntaxExpr *aggFunExpr = new AtomicSyntaxExpr("price", vt_unknown, ATTRIBUTE_NAME);
    AggFunDescription *aggFunDes = new AggFunDescription("min", aggFunExpr);
    aggDes->appendAggFunDescription(aggFunDes);

    //make range
    vector<string> ranges;
    ranges.push_back("222");
    ranges.push_back("111");
    aggDes->setRange(ranges);

    AggregateClause *aggregateClause = new AggregateClause();
    aggregateClause->addAggDescription(aggDes);
    _requestPtr->setAggregateClause(aggregateClause);

    bool ret = _requestValidator->validate(_requestPtr);
    ASSERT_TRUE(!ret);

    ASSERT_EQ(ERROR_AGG_RANGE_ORDER,
                         _requestValidator->getErrorCode());
    ASSERT_EQ(string(), _requestValidator->getErrorMsg());
}

TEST_F(RequestValidatorTest, testAggregateClauseRangeTypeError) {
   HA3_LOG(DEBUG, "Begin Test!");

    _requestPtr.reset(prepareSimpleRequest());
    ASSERT_TRUE(_requestPtr);

    //make AggregateDescription
    AggregateDescription* aggDes = new AggregateDescription;
    AtomicSyntaxExpr *groupKeyExpr
        = new AtomicSyntaxExpr("string_attr", vt_unknown, ATTRIBUTE_NAME);
    aggDes->setGroupKeyExpr(groupKeyExpr);

    //make AggFunDescription
    AtomicSyntaxExpr *aggFunExpr =
        new AtomicSyntaxExpr("price", vt_unknown, ATTRIBUTE_NAME);
    AggFunDescription *aggFunDes =
        new AggFunDescription("min", aggFunExpr);
    aggDes->appendAggFunDescription(aggFunDes);

    //make range
    vector<string> ranges;
    ranges.push_back("111");
    ranges.push_back("222");
    aggDes->setRange(ranges);

    AggregateClause *aggregateClause = new AggregateClause();
    aggregateClause->addAggDescription(aggDes);
    _requestPtr->setAggregateClause(aggregateClause);

    bool ret = _requestValidator->validate(_requestPtr);
    ASSERT_TRUE(!ret);

    ASSERT_EQ(ERROR_SYNTAX_EXPRESSION_TYPE_NOT_SUPPORT,
                         _requestValidator->getErrorCode());

    ASSERT_EQ(string("syntax location: RANGE_AGGKEY_SYNTAX_LOCATION"),
                         _requestValidator->getErrorMsg());
}

TEST_F(RequestValidatorTest, testAggregateClauseWithAggFilterAndErrorAttrName) {
   HA3_LOG(DEBUG, "Begin Test!");

    _requestPtr.reset(prepareSimpleRequest());
    ASSERT_TRUE(_requestPtr);

    //make AggregateDescription
    AggregateDescription* aggDes = new AggregateDescription;
    AtomicSyntaxExpr *groupKeyExpr
        = new AtomicSyntaxExpr("type", vt_unknown, ATTRIBUTE_NAME);
    aggDes->setGroupKeyExpr(groupKeyExpr);

    //make AggFunDescription
    AtomicSyntaxExpr *aggFunExpr = new AtomicSyntaxExpr("price", vt_unknown, ATTRIBUTE_NAME);
    AggFunDescription *aggFunDes = new AggFunDescription("max", aggFunExpr);
    aggDes->appendAggFunDescription(aggFunDes);

    FilterClause *filterClause = new FilterClause;
    AtomicSyntaxExpr *funLeftExpr =
        new AtomicSyntaxExpr("no_such_attr", vt_unknown, ATTRIBUTE_NAME);
    AtomicSyntaxExpr *funRightExpr =
        new AtomicSyntaxExpr("111", vt_unknown, INTEGER_VALUE);
    GreaterSyntaxExpr *aggFilterExpr =
        new GreaterSyntaxExpr(funLeftExpr, funRightExpr);
    filterClause->setRootSyntaxExpr(aggFilterExpr);
    aggDes->setFilterClause(filterClause);

    AggregateClause *aggregateClause = new AggregateClause();
    aggregateClause->addAggDescription(aggDes);
    _requestPtr->setAggregateClause(aggregateClause);

    bool ret = _requestValidator->validate(_requestPtr);
    ASSERT_TRUE(!ret);

    ASSERT_EQ(ERROR_ATTRIBUTE_NOT_EXIST,
                         _requestValidator->getErrorCode());
    string errorMsg = string("attribute [no_such_attr] not exist");
    ASSERT_EQ(errorMsg, _requestValidator->getErrorMsg());
}

TEST_F(RequestValidatorTest, testAggregateClauseWithAggFilterUseSubDoc) {
    HA3_LOG(DEBUG, "Begin Test!");
    _requestPtr.reset(prepareSimpleRequest());
    ASSERT_TRUE(_requestPtr);
    ConfigClause *configClause = _requestPtr->getConfigClause();
    configClause->setSubDocDisplayType(SUB_DOC_DISPLAY_FLAT);
    // make AggregateDescription
    AggregateDescription* aggDes = new AggregateDescription;
    AtomicSyntaxExpr *groupKeyExpr
        = new AtomicSyntaxExpr("sub_attr_int8", vt_unknown, ATTRIBUTE_NAME);
    aggDes->setGroupKeyExpr(groupKeyExpr);

    // make AggFunDescription
    AtomicSyntaxExpr *aggFunExpr = new AtomicSyntaxExpr("price", vt_unknown, ATTRIBUTE_NAME);
    AggFunDescription *aggFunDes = new AggFunDescription("max", aggFunExpr);
    aggDes->appendAggFunDescription(aggFunDes);

    FilterClause *filterClause = new FilterClause;
    AtomicSyntaxExpr *funLeftExpr =
        new AtomicSyntaxExpr("sub_attr_int8", vt_unknown, ATTRIBUTE_NAME);
    AtomicSyntaxExpr *funRightExpr =
        new AtomicSyntaxExpr("111", vt_unknown, INTEGER_VALUE);
    GreaterSyntaxExpr *aggFilterExpr =
        new GreaterSyntaxExpr(funLeftExpr, funRightExpr);
    filterClause->setRootSyntaxExpr(aggFilterExpr);
    aggDes->setFilterClause(filterClause);

    AggregateClause *aggregateClause = new AggregateClause();
    aggregateClause->addAggDescription(aggDes);
    _requestPtr->setAggregateClause(aggregateClause);

    bool ret = _requestValidator->validate(_requestPtr);
    ASSERT_TRUE(ret);
    ASSERT_TRUE(aggFilterExpr->isSubExpression());
}

TEST_F(RequestValidatorTest, testGroupKeyNotSubAggFuncSub) {
    HA3_LOG(DEBUG, "Begin Test!");
    _requestPtr.reset(prepareSimpleRequest());
    ASSERT_TRUE(_requestPtr);
    ConfigClause *configClause = _requestPtr->getConfigClause();
    configClause->setSubDocDisplayType(SUB_DOC_DISPLAY_FLAT);
    // make AggregateDescription
    AggregateDescription* aggDes = new AggregateDescription;
    AtomicSyntaxExpr *groupKeyExpr
        = new AtomicSyntaxExpr("type", vt_unknown, ATTRIBUTE_NAME);
    aggDes->setGroupKeyExpr(groupKeyExpr);

    // make AggFunDescription
    AtomicSyntaxExpr *aggFunExpr = new AtomicSyntaxExpr("sub_attr_int8", vt_unknown, ATTRIBUTE_NAME);
    AggFunDescription *aggFunDes = new AggFunDescription("max", aggFunExpr);
    aggDes->appendAggFunDescription(aggFunDes);

    AggregateClause *aggregateClause = new AggregateClause();
    aggregateClause->addAggDescription(aggDes);
    _requestPtr->setAggregateClause(aggregateClause);

    bool ret = _requestValidator->validate(_requestPtr);
    ASSERT_TRUE(!ret);
    ASSERT_EQ(ERROR_SYNTAX_EXPRESSION_SUB_DOC_NOT_SUPPORT,
                         _requestValidator->getErrorCode());
}

TEST_F(RequestValidatorTest, testGroupKeyNotSubAggFilterSub) {
    HA3_LOG(DEBUG, "Begin Test!");
    _requestPtr.reset(prepareSimpleRequest());
    ASSERT_TRUE(_requestPtr);
    ConfigClause *configClause = _requestPtr->getConfigClause();
    configClause->setSubDocDisplayType(SUB_DOC_DISPLAY_FLAT);
    // make AggregateDescription
    AggregateDescription* aggDes = new AggregateDescription;
    AtomicSyntaxExpr *groupKeyExpr
        = new AtomicSyntaxExpr("type", vt_unknown, ATTRIBUTE_NAME);
    aggDes->setGroupKeyExpr(groupKeyExpr);

    // make AggFunDescription
    AtomicSyntaxExpr *aggFunExpr = new AtomicSyntaxExpr("price", vt_unknown, ATTRIBUTE_NAME);
    AggFunDescription *aggFunDes = new AggFunDescription("max", aggFunExpr);
    aggDes->appendAggFunDescription(aggFunDes);

    FilterClause *filterClause = new FilterClause;
    AtomicSyntaxExpr *funLeftExpr =
        new AtomicSyntaxExpr("sub_attr_int8", vt_unknown, ATTRIBUTE_NAME);
    AtomicSyntaxExpr *funRightExpr =
        new AtomicSyntaxExpr("111", vt_unknown, INTEGER_VALUE);
    GreaterSyntaxExpr *aggFilterExpr =
        new GreaterSyntaxExpr(funLeftExpr, funRightExpr);
    filterClause->setRootSyntaxExpr(aggFilterExpr);
    aggDes->setFilterClause(filterClause);

    AggregateClause *aggregateClause = new AggregateClause();
    aggregateClause->addAggDescription(aggDes);
    _requestPtr->setAggregateClause(aggregateClause);

    bool ret = _requestValidator->validate(_requestPtr);
    ASSERT_TRUE(!ret);
    ASSERT_EQ(ERROR_SYNTAX_EXPRESSION_SUB_DOC_NOT_SUPPORT,
                         _requestValidator->getErrorCode());
}

TEST_F(RequestValidatorTest, testHealthCheckClauseWithMinusCheckTimes) {
   _requestPtr.reset(prepareSimpleRequest());
    ASSERT_TRUE(_requestPtr);

    HealthCheckClause *healthCheckClause = new HealthCheckClause();
    _requestPtr->setHealthCheckClause(healthCheckClause);
    healthCheckClause->setCheckTimes(-1);

    bool ret = _requestValidator->validate(_requestPtr);
    ASSERT_TRUE(!ret);
    ASSERT_EQ(ERROR_HEALTHCHECK_CHECK_TIMES,
                         _requestValidator->getErrorCode());
}

TEST_F(RequestValidatorTest, testFetchSummaryClauseNormal) {
    _requestPtr.reset(prepareSimpleRequest());
    ASSERT_TRUE(_requestPtr);

    FetchSummaryClause *fetchSummaryClause = new FetchSummaryClause();
    _requestPtr->setFetchSummaryClause(fetchSummaryClause);
    ASSERT_TRUE(!_requestValidator->validate(_requestPtr));
    ASSERT_EQ(ERROR_FETCH_SUMMARY_CLAUSE,
                         _requestValidator->getErrorCode());

    _requestValidator->reset();
    fetchSummaryClause->addGid("cluster1", GlobalIdentifier(4, 3, 2, 0, 0, 1));
    ASSERT_TRUE(_requestValidator->validate(_requestPtr));
    ASSERT_EQ(ERROR_NONE, _requestValidator->getErrorCode());
}

TEST_F(RequestValidatorTest, testFetchSummaryClauseInvalidFormat) {
    _requestPtr.reset(prepareSimpleRequest());
    ASSERT_TRUE(_requestPtr);

    _requestPtr->getConfigClause()
        ->setResultFormatSetting(RESULT_FORMAT_FB_SUMMARY);
    ASSERT_TRUE(!_requestValidator->validate(_requestPtr));
    ASSERT_EQ(ERROR_UNSUPPORT_RESULT_FORMAT, _requestValidator->getErrorCode());

    _requestValidator->reset();
    FetchSummaryClause *fetchSummaryClause = new FetchSummaryClause();
      _requestPtr->setFetchSummaryClause(fetchSummaryClause);
      fetchSummaryClause->addGid("cluster1", GlobalIdentifier(4, 3, 2, 0, 0, 1));
    ASSERT_TRUE(_requestValidator->validate(_requestPtr));
    ASSERT_EQ(ERROR_NONE, _requestValidator->getErrorCode());
}

TEST_F(RequestValidatorTest, testFetchSummaryClauseWithErrorFormat) {
    _requestPtr.reset(prepareSimpleRequest());
    ASSERT_TRUE(_requestPtr);

    FetchSummaryClause *fetchSummaryClause = new FetchSummaryClause();
    _requestPtr->setFetchSummaryClause(fetchSummaryClause);
    ASSERT_TRUE(!_requestValidator->validate(_requestPtr));
    ASSERT_EQ(ERROR_FETCH_SUMMARY_CLAUSE,
                         _requestValidator->getErrorCode());

    _requestValidator->reset();
    FetchSummaryClause *fetchSummaryClause2 = new FetchSummaryClause();
    fetchSummaryClause2->addGid("1_2", GlobalIdentifier());
    _requestPtr->setFetchSummaryClause(fetchSummaryClause2);
    ASSERT_TRUE(_requestValidator->validate(_requestPtr));
    ASSERT_EQ(ERROR_NONE, _requestValidator->getErrorCode());

    _requestValidator->reset();
    FetchSummaryClause *fetchSummaryClause3 = new FetchSummaryClause();
    fetchSummaryClause3->addGid("not_exist_cluster", GlobalIdentifier());
    _requestPtr->setFetchSummaryClause(fetchSummaryClause3);
    ASSERT_TRUE(!_requestValidator->validate(_requestPtr));
    ASSERT_EQ(ERROR_CLUSTER_NAME_NOT_EXIST,
                         _requestValidator->getErrorCode());

    _requestValidator->reset();
    FetchSummaryClause *fetchSummaryClause4 = new FetchSummaryClause();
    fetchSummaryClause4->addGid("cluster1", GlobalIdentifier(-1, 0));
    _requestPtr->setFetchSummaryClause(fetchSummaryClause4);
    ASSERT_TRUE(!_requestValidator->validate(_requestPtr));
    ASSERT_EQ(ERROR_FETCH_SUMMARY_GID_FORMAT,
                         _requestValidator->getErrorCode());

    _requestValidator->reset();
    FetchSummaryClause *fetchSummaryClause5 = new FetchSummaryClause();
    fetchSummaryClause5->addGid("cluster1", GlobalIdentifier(0, 0, -1));
    _requestPtr->setFetchSummaryClause(fetchSummaryClause5);
    ASSERT_TRUE(!_requestValidator->validate(_requestPtr));
    ASSERT_EQ(ERROR_FETCH_SUMMARY_GID_FORMAT,
                         _requestValidator->getErrorCode());
}

TEST_F(RequestValidatorTest, testFetchSummaryClauseWithPrimaryKey) {
    _requestPtr.reset(prepareSimpleRequest());
    _requestPtr->getConfigClause()->setFetchSummaryType(BY_PK);

    FetchSummaryClause *fetchSummaryClause = new FetchSummaryClause();
    _requestPtr->setFetchSummaryClause(fetchSummaryClause);
    fetchSummaryClause->addGid("cluster1", GlobalIdentifier(4, 3, 2, 0, 0, 1));
    ASSERT_TRUE(!_requestValidator->validate(_requestPtr));
    ASSERT_EQ(ERROR_FETCH_SUMMARY_CLAUSE,
                         _requestValidator->getErrorCode());

    _requestValidator->reset();
    _requestPtr->getConfigClause()->setFetchSummaryType(BY_RAWPK);
    FetchSummaryClause *fetchSummaryClause2 = new FetchSummaryClause();
    fetchSummaryClause2->addRawPk("cluster1", "rawpk1");
    _requestPtr->setFetchSummaryClause(fetchSummaryClause2);
    ASSERT_TRUE(!_requestValidator->validate(_requestPtr));
    ASSERT_EQ(ERROR_FETCH_SUMMARY_CLAUSE,
                         _requestValidator->getErrorCode());

    _requestValidator->reset();
    _requestPtr->getConfigClause()->setFetchSummaryType(BY_RAWPK);
    FetchSummaryClause *fetchSummaryClause3 = new FetchSummaryClause();
    fetchSummaryClause3->addRawPk("pk_cluster", "rawpk1");
    _requestPtr->setFetchSummaryClause(fetchSummaryClause3);
    ASSERT_EQ(ERROR_NONE, _requestValidator->getErrorCode());
    ASSERT_TRUE(_requestValidator->validate(_requestPtr));
}

TEST_F(RequestValidatorTest, testValidateMultiClusterNormal) {
    {
        TableInfoPtr tableInfoPtr(new TableInfo("table1"));
        IndexInfos *indexInfos = createFakeIndexInfos();
        AttributeInfos *attrInfos = createFakeAttributeInfos();
        tableInfoPtr->setIndexInfos(indexInfos);
        tableInfoPtr->setAttributeInfos(attrInfos);
        (*_clusterTableInfoMapPtr)["cluster1.default"] = tableInfoPtr;
    }
    {
        TableInfoPtr tableInfoPtr(new TableInfo("table2"));
        IndexInfos *indexInfos = createFakeIndexInfos();
        AttributeInfos *attrInfos = createFakeAttributeInfos();
        tableInfoPtr->setIndexInfos(indexInfos);
        tableInfoPtr->setAttributeInfos(attrInfos);
        (*_clusterTableInfoMapPtr)["cluster2.default"] = tableInfoPtr;
    }
    DELETE_AND_SET_NULL(_requestValidator);
    ClusterConfigMapPtr clusterConfigMapPtr(new ClusterConfigMap);
    {
        ClusterConfigInfo clusterInfo;
        clusterConfigMapPtr->insert(make_pair("cluster1.default", clusterInfo));
    }
    {
        ClusterConfigInfo clusterInfo;
        clusterConfigMapPtr->insert(make_pair("cluster2.default", clusterInfo));
    }
    _requestValidator = new RequestValidator(_clusterTableInfoMapPtr, QRS_RETURN_HITS_LIMIT,
            clusterConfigMapPtr, ClusterFuncMapPtr(new ClusterFuncMap), CavaPluginManagerMapPtr(),
            ClusterSorterNamesPtr(new ClusterSorterNames));

    _requestPtr.reset(prepareSimpleRequest());
    _requestPtr->getConfigClause()->addClusterName("cluster1");
    _requestPtr->getConfigClause()->addClusterName("cluster2");

    AttributeClause *attributeClause = new AttributeClause();
    attributeClause->addAttribute("price");
    attributeClause->addAttribute("multi_attr_1");
    _requestPtr->setAttributeClause(attributeClause);
    ASSERT_TRUE(_requestValidator->validate(_requestPtr));
}

TEST_F(RequestValidatorTest, testValidateMultiClusterFailWithIndexInfo) {
    {
        TableInfoPtr tableInfoPtr(new TableInfo("table1"));
        IndexInfos *indexInfos = createFakeIndexInfos(2);
        AttributeInfos *attrInfos = createFakeAttributeInfos();
        tableInfoPtr->setIndexInfos(indexInfos);
        tableInfoPtr->setAttributeInfos(attrInfos);
        (*_clusterTableInfoMapPtr)["cluster1.default"] = tableInfoPtr;
    }
    {
        TableInfoPtr tableInfoPtr(new TableInfo("table2"));
        IndexInfos *indexInfos = createFakeIndexInfos(3);
        AttributeInfos *attrInfos = createFakeAttributeInfos();
        tableInfoPtr->setIndexInfos(indexInfos);
        tableInfoPtr->setAttributeInfos(attrInfos);
        (*_clusterTableInfoMapPtr)["cluster2.default"] = tableInfoPtr;
    }
    ClusterConfigMapPtr clusterConfigMapPtr(new ClusterConfigMap);
    {
        ClusterConfigInfo clusterInfo;
        clusterConfigMapPtr->insert(make_pair("cluster1.default", clusterInfo));
    }
    {
        ClusterConfigInfo clusterInfo;
        clusterConfigMapPtr->insert(make_pair("cluster2.default", clusterInfo));
    }

    DELETE_AND_SET_NULL(_requestValidator);
    _requestValidator = new RequestValidator(_clusterTableInfoMapPtr, QRS_RETURN_HITS_LIMIT,
            clusterConfigMapPtr, ClusterFuncMapPtr(new ClusterFuncMap), CavaPluginManagerMapPtr(),
            ClusterSorterNamesPtr(new ClusterSorterNames));

    Request *request = new Request;
    RequiredFields requiredField;
    QueryClause *queryClause = new QueryClause(new TermQuery("a", "default2", requiredField, ""));
    request->setQueryClause(queryClause);
    ConfigClause *configClause = new ConfigClause();
    request->setConfigClause(configClause);
    _requestPtr.reset(request);
    _requestPtr->getConfigClause()->addClusterName("cluster1");
    _requestPtr->getConfigClause()->addClusterName("cluster2");

    AttributeClause *attributeClause = new AttributeClause();
    attributeClause->addAttribute("price");
    attributeClause->addAttribute("multi_attr_1");
    _requestPtr->setAttributeClause(attributeClause);
    ASSERT_TRUE(!_requestValidator->validate(_requestPtr));
}

TEST_F(RequestValidatorTest, testValidateMultiClusterFailWithAttributeInfo) {
    {
        TableInfoPtr tableInfoPtr(new TableInfo("table1"));
        IndexInfos *indexInfos = createFakeIndexInfos();
        AttributeInfos *attrInfos = createFakeAttributeInfos(false);
        tableInfoPtr->setIndexInfos(indexInfos);
        tableInfoPtr->setAttributeInfos(attrInfos);
        (*_clusterTableInfoMapPtr)["cluster1.default"] = tableInfoPtr;
    }
    {
        TableInfoPtr tableInfoPtr(new TableInfo("table2"));
        IndexInfos *indexInfos = createFakeIndexInfos();
        AttributeInfos *attrInfos = createFakeAttributeInfos();
        tableInfoPtr->setIndexInfos(indexInfos);
        tableInfoPtr->setAttributeInfos(attrInfos);
        (*_clusterTableInfoMapPtr)["cluster2.default"] = tableInfoPtr;
    }
    ClusterConfigMapPtr clusterConfigMapPtr(new ClusterConfigMap);
    {
        ClusterConfigInfo clusterInfo;
        clusterConfigMapPtr->insert(make_pair("cluster1.default", clusterInfo));
    }
    {
        ClusterConfigInfo clusterInfo;
        clusterConfigMapPtr->insert(make_pair("cluster2.default", clusterInfo));
    }

    DELETE_AND_SET_NULL(_requestValidator);
    _requestValidator = new RequestValidator(_clusterTableInfoMapPtr, QRS_RETURN_HITS_LIMIT,
            clusterConfigMapPtr, ClusterFuncMapPtr(new ClusterFuncMap), CavaPluginManagerMapPtr(),
            ClusterSorterNamesPtr(new ClusterSorterNames));

    _requestPtr.reset(prepareSimpleRequest());
    _requestPtr->getConfigClause()->addClusterName("cluster1");
    _requestPtr->getConfigClause()->addClusterName("cluster2");

    AttributeClause *attributeClause = new AttributeClause();
    attributeClause->addAttribute("price");
    attributeClause->addAttribute("multi_attr_1");
    _requestPtr->setAttributeClause(attributeClause);
    ASSERT_TRUE(!_requestValidator->validate(_requestPtr));
}

TEST_F(RequestValidatorTest, testValidateMultiClusterFailWithLevelClause) {
    {
        TableInfoPtr tableInfoPtr(new TableInfo("table1"));
        IndexInfos *indexInfos = createFakeIndexInfos();
        AttributeInfos *attrInfos = createFakeAttributeInfos(true);
        tableInfoPtr->setIndexInfos(indexInfos);
        tableInfoPtr->setAttributeInfos(attrInfos);
        (*_clusterTableInfoMapPtr)["cluster1.default"] = tableInfoPtr;
    }
    {
        TableInfoPtr tableInfoPtr(new TableInfo("table2"));
        IndexInfos *indexInfos = createFakeIndexInfos();
        AttributeInfos *attrInfos = createFakeAttributeInfos(false);
        tableInfoPtr->setIndexInfos(indexInfos);
        tableInfoPtr->setAttributeInfos(attrInfos);
        (*_clusterTableInfoMapPtr)["cluster2.default"] = tableInfoPtr;
    }
    ClusterConfigMapPtr clusterConfigMapPtr(new ClusterConfigMap);
    {
        ClusterConfigInfo clusterInfo;
        clusterConfigMapPtr->insert(make_pair("cluster1.default", clusterInfo));
    }
    {
        ClusterConfigInfo clusterInfo;
        clusterConfigMapPtr->insert(make_pair("cluster2.default", clusterInfo));
    }
    DELETE_AND_SET_NULL(_requestValidator);
    _requestValidator = new RequestValidator(_clusterTableInfoMapPtr,
            QRS_RETURN_HITS_LIMIT, clusterConfigMapPtr,
            ClusterFuncMapPtr(new ClusterFuncMap), CavaPluginManagerMapPtr(),
            ClusterSorterNamesPtr(new ClusterSorterNames));

    _requestPtr.reset(prepareSimpleRequest());
    _requestPtr->getConfigClause()->addClusterName("cluster1");

    AttributeClause *attributeClause = new AttributeClause();
    attributeClause->addAttribute("price");
    attributeClause->addAttribute("multi_attr_1");
    _requestPtr->setAttributeClause(attributeClause);

    LevelClause *levelClause = new LevelClause;
    levelClause->setLevelQueryType(BOTH_LEVEL);
    levelClause->setSecondLevelCluster("cluster2");
    _requestPtr->setLevelClause(levelClause);

    ASSERT_TRUE(!_requestValidator->validate(_requestPtr));
}

Request* RequestValidatorTest::prepareSimpleRequest() {
    Request *request = new Request;

    RequiredFields requiredField;
    QueryClause *queryClause = new QueryClause(
            new TermQuery("a", "default0", requiredField, ""));
    request->setQueryClause(queryClause);

    ConfigClause *configClause = new ConfigClause();
    configClause->addClusterName("cluster1");
    request->setConfigClause(configClause);
    return request;
}

IndexInfos* RequestValidatorTest::createFakeIndexInfos(uint32_t count, bool useSubSchema) {
    IndexInfos* indexInfos = new IndexInfos();
    for (uint32_t i = 0; i < count; ++i) {
        IndexInfo* indexInfo = new IndexInfo();
        indexInfo->setIndexName((string("default") +
                        StringUtil::toString(i)).c_str());
        indexInfo->addField("title", 100);
        indexInfo->addField("body", 50);
        indexInfo->setIndexType(it_text);
        indexInfo->setAnalyzerName("SimpleAnalyzer");
        indexInfos->addIndexInfo(indexInfo);
    }
    {
        IndexInfo* indexInfo = new IndexInfo();
        indexInfo->setIndexName("expack_index");
        indexInfo->addField("title", 100);
        indexInfo->addField("body", 50);
        indexInfo->setIndexType(it_expack);
        indexInfo->setAnalyzerName("SimpleAnalyzer");
        indexInfos->addIndexInfo(indexInfo);
    }
    if(useSubSchema) {
        for (uint32_t i = 0; i < count; ++i) {
            IndexInfo* indexInfo = new IndexInfo();
            indexInfo->setIndexName((string("sub_default") +
                            StringUtil::toString(i)).c_str());
            indexInfo->addField("sub_title", 100);
            indexInfo->addField("sub_body", 50);
            indexInfo->setIndexType(it_text);
            indexInfo->setAnalyzerName("SimpleAnalyzer");
            indexInfo->isSubDocIndex = true;
            indexInfos->addIndexInfo(indexInfo);
        }
        {
            IndexInfo* indexInfo = new IndexInfo();
            indexInfo->setIndexName("sub_expack_index");
            indexInfo->addField("sub_title", 100);
            indexInfo->addField("sub_body", 50);
            indexInfo->setIndexType(it_expack);
            indexInfo->setAnalyzerName("SimpleAnalyzer");
            indexInfo->isSubDocIndex = true;
            indexInfos->addIndexInfo(indexInfo);
        }
    }
    return indexInfos;
}

AttributeInfos* RequestValidatorTest::createFakeAttributeInfos(bool createMulti, bool useSubSchema) {
    AttributeInfos *attrInfos = new AttributeInfos;

#define ADD_ATTR_TYPE_HELPER(name, ft_type, isMulti, isSub)     \
    do {                                                        \
        AttributeInfo *attrInfo = new AttributeInfo();          \
        attrInfo->setAttrName(name);                            \
        attrInfo->setMultiValueFlag(isMulti);                   \
        attrInfo->setSubDocAttributeFlag(isSub);                \
        FieldInfo fieldInfo(name, ft_type);                     \
        attrInfo->setFieldInfo(fieldInfo);                      \
        attrInfos->addAttributeInfo(attrInfo);                  \
    } while(0)

    ADD_ATTR_TYPE_HELPER("price", ft_integer, false, false);
    ADD_ATTR_TYPE_HELPER("type", ft_long, false, false);
    ADD_ATTR_TYPE_HELPER("string_attr", ft_string, false, false);
    ADD_ATTR_TYPE_HELPER("attr_int8", ft_int8, false, false);
    ADD_ATTR_TYPE_HELPER("attr_uint8", ft_uint8, false, false);
    ADD_ATTR_TYPE_HELPER("attr_int16", ft_int16, false, false);
    ADD_ATTR_TYPE_HELPER("attr_uint16", ft_uint16, false, false);
    ADD_ATTR_TYPE_HELPER("attr_int32", ft_integer, false, false);
    ADD_ATTR_TYPE_HELPER("attr_uint32", ft_uint32, false, false);
    ADD_ATTR_TYPE_HELPER("attr_int64", ft_long, false, false);
    ADD_ATTR_TYPE_HELPER("attr_uint64", ft_uint64, false, false);
    ADD_ATTR_TYPE_HELPER("attr_float", ft_float, false, false);
    ADD_ATTR_TYPE_HELPER("attr_double", ft_double, false, false);
    if(useSubSchema) {
        ADD_ATTR_TYPE_HELPER("sub_price", ft_integer, false, true);
        ADD_ATTR_TYPE_HELPER("sub_type", ft_long, false, true);
        ADD_ATTR_TYPE_HELPER("sub_string_attr", ft_string, false, true);
        ADD_ATTR_TYPE_HELPER("sub_attr_int8", ft_int8, false, true);
        ADD_ATTR_TYPE_HELPER("sub_attr_uint8", ft_uint8, false, true);
        ADD_ATTR_TYPE_HELPER("sub_attr_int16", ft_int16, false, true);
        ADD_ATTR_TYPE_HELPER("sub_attr_uint16", ft_uint16, false, true);
        ADD_ATTR_TYPE_HELPER("sub_attr_int32", ft_integer, false, true);
        ADD_ATTR_TYPE_HELPER("sub_attr_uint32", ft_uint32, false, true);
        ADD_ATTR_TYPE_HELPER("sub_attr_int64", ft_long, false, true);
        ADD_ATTR_TYPE_HELPER("sub_attr_uint64", ft_uint64, false, true);
        ADD_ATTR_TYPE_HELPER("sub_attr_float", ft_float, false, true);
        ADD_ATTR_TYPE_HELPER("sub_attr_double", ft_double, false, true);
    }
    if (createMulti) {
        ADD_ATTR_TYPE_HELPER("multi_attr_1", ft_integer, true, false);
        ADD_ATTR_TYPE_HELPER("multi_attr_2", ft_integer, true, false);
        ADD_ATTR_TYPE_HELPER("multi_string_attr", ft_string, true, false);
        if (useSubSchema) {
            ADD_ATTR_TYPE_HELPER("sub_multi_attr_1", ft_integer, true, true);
            ADD_ATTR_TYPE_HELPER("sub_multi_attr_2", ft_integer, true, true);
            ADD_ATTR_TYPE_HELPER("sub_multi_string_attr", ft_string, true, true);
        }
    }

#undef ADD_ATTR_TYPE_HELPER

    return attrInfos;
}

AttributeInfos* RequestValidatorTest::createFakeAuxAttributeInfos(bool createMulti, bool useSubSchema) {
    AttributeInfos *attrInfos = new AttributeInfos;

#define ADD_ATTR_TYPE_HELPER(name, ft_type, isMulti, isSub)     \
    do {                                                        \
        AttributeInfo *attrInfo = new AttributeInfo();          \
        attrInfo->setAttrName(name);                            \
        attrInfo->setMultiValueFlag(isMulti);                   \
        attrInfo->setSubDocAttributeFlag(isSub);                \
        FieldInfo fieldInfo(name, ft_type);                     \
        attrInfo->setFieldInfo(fieldInfo);                      \
        attrInfos->addAttributeInfo(attrInfo);                  \
    } while(0)

    ADD_ATTR_TYPE_HELPER("company_id", ft_integer, false, false);
    ADD_ATTR_TYPE_HELPER("aux_price", ft_integer, false, false);
    ADD_ATTR_TYPE_HELPER("aux_string_attr", ft_string, false, false);

#undef ADD_ATTR_TYPE_HELPER

    return attrInfos;
}

AttributeInfos* RequestValidatorTest::createFakeMainAttributeInfos(bool createMulti, bool useSubSchema) {
    AttributeInfos *attrInfos = new AttributeInfos;

#define ADD_ATTR_TYPE_HELPER(name, ft_type, isMulti, isSub)     \
    do {                                                        \
        AttributeInfo *attrInfo = new AttributeInfo();          \
        attrInfo->setAttrName(name);                            \
        attrInfo->setMultiValueFlag(isMulti);                   \
        attrInfo->setSubDocAttributeFlag(isSub);                \
        FieldInfo fieldInfo(name, ft_type);                     \
        attrInfo->setFieldInfo(fieldInfo);                      \
        attrInfos->addAttributeInfo(attrInfo);                  \
    } while(0)

    ADD_ATTR_TYPE_HELPER("company_id", ft_integer, false, false);
    ADD_ATTR_TYPE_HELPER("price", ft_integer, false, false);
    ADD_ATTR_TYPE_HELPER("string_attr", ft_string, false, false);

#undef ADD_ATTR_TYPE_HELPER

    return attrInfos;
}

TEST_F(RequestValidatorTest, testBug42726) {
   HA3_LOG(DEBUG, "Begin Test!");

    _requestPtr.reset(prepareSimpleRequest());
    ASSERT_TRUE(_requestPtr);

    //make AggregateDescription
    AggregateDescription* aggDes = new AggregateDescription;
    AtomicSyntaxExpr *groupKeyExpr
        = new AtomicSyntaxExpr("type", vt_unknown, ATTRIBUTE_NAME);
    aggDes->setGroupKeyExpr(groupKeyExpr);

    //make AggFunDescription
    AtomicSyntaxExpr *aggFunExpr = new AtomicSyntaxExpr("price", vt_unknown, ATTRIBUTE_NAME);
    AggFunDescription *aggFunDes = new AggFunDescription("min", aggFunExpr);
    aggDes->appendAggFunDescription(aggFunDes);

    //make range
    vector<string> ranges;
    ranges.push_back("111");
    ranges.push_back("111");
    aggDes->setRange(ranges);

    AggregateClause *aggregateClause = new AggregateClause();
    aggregateClause->addAggDescription(aggDes);
    _requestPtr->setAggregateClause(aggregateClause);

    bool ret = _requestValidator->validate(_requestPtr);
    ASSERT_TRUE(!ret);
    ASSERT_EQ(ERROR_AGG_RANGE_ORDER, _requestValidator->getErrorCode());
    ASSERT_EQ(string(), _requestValidator->getErrorMsg());
}

TEST_F(RequestValidatorTest, testCheckErrorResult) {
    map<string, ErrorCode> errMap;
    errMap["config=cluster:cluster1&&query=\"\""] = ERROR_NULL_PHRASE_QUERY;
    errMap["config=cluster:cluster1"] = ERROR_NO_QUERY_CLAUSE;
    errMap["config=cluster:cluster1&&query=phrasenotexist:xx"] = ERROR_INDEX_NOT_EXIST;
    errMap["config=cluster:cluster1&&query=abc&&filter=2"] = ERROR_SYNTAX_EXPRESSION_TYPE_NOT_SUPPORT;
    errMap["config=cluster:cluster1&&query=abc&&sort=price>2"] = ERROR_SYNTAX_EXPRESSION_TYPE_NOT_SUPPORT;
    errMap["config=cluster:cluster1&&query=xx&&aggregate=group_key:price,range:2~1,agg_fun:count()"] = ERROR_AGG_RANGE_ORDER;
    errMap["config=cluster:cluster1&&query=xx&&aggregate=group_key:attr_not_exist,agg_fun:count()"] = ERROR_ATTRIBUTE_NOT_EXIST;
    errMap["config=cluster:cluster1&&query=xx&&filter=2>1"] = ERROR_EXPRESSION_LR_ALL_CONST_VALUE;
    errMap["config=cluster:cluster1&&query=xx&&filter=string_attr>1"] = ERROR_CONST_EXPRESSION_TYPE;
    errMap["config=cluster:cluster1&&query=xx&&filter=multi_string_attr>\"abc\""] = ERROR_EXPRESSION_WITH_MULTI_VALUE;
    errMap["config=cluster:cluster1&&query=xx&&aggregate=group_key:price,agg_fun:min(type+price)"] = ERROR_EXPRESSION_LR_TYPE_NOT_MATCH;
    errMap["config=cluster:cluster1&&query=xx&&aggregate=group_key:string_attr,agg_fun:min(price)"] = ERROR_NONE;
    errMap["config=cluster:cluster1&&query=xx&&aggregate=group_key:multi_string_attr,agg_fun:min(price)"] = ERROR_NONE;

    // distinct with string/multi_value/multi_string
    errMap["config=cluster:cluster1&&query=xx&&distinct=dist_key:string_attr,dist_count:1"] = ERROR_NONE;
    errMap["config=cluster:cluster1&&query=xx&&distinct=dist_key:multi_string_attr,dist_count:1"] = ERROR_NONE;
    errMap["config=cluster:cluster1&&query=xx&&distinct=dist_key:multi_attr_1,dist_count:1"] = ERROR_NONE;

    // sort with string/multi_value/multi_string
    errMap["config=cluster:cluster1&&query=xx&&sort=string_attr"] = ERROR_NONE;
    errMap["config=cluster:cluster1&&query=xx&&sort=-multi_string_attr"] = ERROR_SYNTAX_EXPRESSION_TYPE_NOT_SUPPORT;
    errMap["config=cluster:cluster1&&query=xx&&sort=multi_attr_1"] = ERROR_SYNTAX_EXPRESSION_TYPE_NOT_SUPPORT;

    // rankSort with string/multi_value/multi_string
    errMap["config=cluster:cluster1&&query=xx&&rank_sort=sort:string_attr,percent:100"] = ERROR_NONE;
    errMap["config=cluster:cluster1&&query=xx&&rank_sort=sort:multi_string_attr,percent:100"] = ERROR_SYNTAX_EXPRESSION_TYPE_NOT_SUPPORT;
    errMap["config=cluster:cluster1&&query=xx&&rank_sort=sort:multi_attr_1,percent:100"] = ERROR_SYNTAX_EXPRESSION_TYPE_NOT_SUPPORT;

    for (map<string, ErrorCode>::const_iterator it = errMap.begin();
         it != errMap.end(); ++it)
    {
        checkErrorResult( it->first, it->second);
    }
}

TEST_F(RequestValidatorTest, testValidateMultiQuery) {
    {
        RequestPtr requestPtr = RequestCreator::prepareRequest(
                "config=cluster:cluster1&&query=not_exist_index_name:a;b");
        ASSERT_TRUE(requestPtr);
        bool ret = _requestValidator->validate(requestPtr);
        ASSERT_TRUE(!ret);
        ASSERT_EQ(ERROR_INDEX_NOT_EXIST, _requestValidator->getErrorCode());
    }
    {
        RequestPtr requestPtr = RequestCreator::prepareRequest(
                "config=cluster:cluster1&&query=a;not_exist_index_name:b");
        ASSERT_TRUE(requestPtr);
        bool ret = _requestValidator->validate(requestPtr);
        ASSERT_TRUE(!ret);
        ASSERT_EQ(ERROR_INDEX_NOT_EXIST, _requestValidator->getErrorCode());
    }
}

TEST_F(RequestValidatorTest, testValidateSearcherCacheClause){
    HA3_LOG(DEBUG, "Begin Test!");
    RequestPtr requestPtr = RequestCreator::prepareRequest(
            "config=cluster:cluster1&&query=a&&searcher_cache=use:yes,expire_time:attr_uint32,refresh_attributes:attr_uint32;attr_uint32");
    ASSERT_TRUE(requestPtr);
    bool ret = _requestValidator->validate(requestPtr);
    ASSERT_TRUE(ret);

    requestPtr = RequestCreator::prepareRequest(
            "config=cluster:cluster1&&query=a&&searcher_cache=use:yes,expire_time:attr_uint32,cache_filter:attr_uint32=1");
    ASSERT_TRUE(requestPtr);
    ASSERT_TRUE(requestPtr->getSearcherCacheClause());
    ASSERT_TRUE(requestPtr->getSearcherCacheClause()->getExpireExpr());
    ASSERT_TRUE(requestPtr->getSearcherCacheClause()->getFilterClause());
    ret = _requestValidator->validate(requestPtr);
    ASSERT_TRUE(ret);

    requestPtr = RequestCreator::prepareRequest(
            "config=cluster:cluster1&&query=a&&searcher_cache=use:yes,cache_filter:attr_uint32=1");
    ASSERT_TRUE(requestPtr);
    ASSERT_TRUE(requestPtr->getSearcherCacheClause());
    ASSERT_TRUE(requestPtr->getSearcherCacheClause()->getFilterClause());
    ASSERT_TRUE(!requestPtr->getSearcherCacheClause()->getExpireExpr());
    ret = _requestValidator->validate(requestPtr);
    ASSERT_TRUE(ret);

    requestPtr = RequestCreator::prepareRequest(
            "config=cluster:cluster1&&query=a&&searcher_cache=use:yes,expire_time:attr_int32,cache_filter:attr_uint32=1");
    ASSERT_TRUE(requestPtr);
    ASSERT_TRUE(requestPtr->getSearcherCacheClause());
    ASSERT_TRUE(requestPtr->getSearcherCacheClause()->getExpireExpr());
    ASSERT_TRUE(requestPtr->getSearcherCacheClause()->getFilterClause());
    ret = _requestValidator->validate(requestPtr);
    ASSERT_TRUE(!ret);

    requestPtr = RequestCreator::prepareRequest(
            "config=cluster:cluster1&&query=a&&searcher_cache=use:yes,expire_time:attr_uint32,cache_filter:attr_uint32");
    ASSERT_TRUE(requestPtr);
    ASSERT_TRUE(requestPtr->getSearcherCacheClause()->getExpireExpr());
    ASSERT_TRUE(requestPtr->getSearcherCacheClause()->getFilterClause());
    ret = _requestValidator->validate(requestPtr);
    ASSERT_TRUE(!ret);

    // fix bug: should validate cache_filter when expire_time absent
    requestPtr = RequestCreator::prepareRequest(
            "config=cluster:cluster1&&query=a&&searcher_cache=use:yes,cache_filter:attr_uint32");
    ASSERT_TRUE(requestPtr);
    ASSERT_TRUE(!requestPtr->getSearcherCacheClause()->getExpireExpr());
    ASSERT_TRUE(requestPtr->getSearcherCacheClause()->getFilterClause());
    ret = _requestValidator->validate(requestPtr);
    ASSERT_TRUE(!ret);

    requestPtr = RequestCreator::prepareRequest(
            "config=cluster:cluster1&&query=a&&searcher_cache=use:yes,expire_time:sub_attr_uint32,cache_filter:attr_uint32=1");
    ASSERT_TRUE(requestPtr);
    ret = _requestValidator->validate(requestPtr);
    ASSERT_TRUE(!ret);
    ASSERT_EQ(ERROR_SYNTAX_EXPRESSION_SUB_DOC_NOT_SUPPORT,
                         _requestValidator->getErrorCode());

    requestPtr = RequestCreator::prepareRequest(
            "config=cluster:cluster1&&query=a&&searcher_cache=use:yes,expire_time:attr_uint32,cache_filter:sub_attr_uint32=1");
    ASSERT_TRUE(requestPtr);
    ret = _requestValidator->validate(requestPtr);
    ASSERT_TRUE(!ret);
    ASSERT_EQ(ERROR_SYNTAX_EXPRESSION_SUB_DOC_NOT_SUPPORT,
                         _requestValidator->getErrorCode());

    requestPtr = RequestCreator::prepareRequest(
            "config=cluster:cluster1,sub_doc:group&&query=a&&searcher_cache=use:yes,expire_time:attr_uint32,cache_filter:sub_attr_uint32=1");
    ASSERT_TRUE(requestPtr);
    ret = _requestValidator->validate(requestPtr);
    ASSERT_TRUE(!ret);
    ASSERT_EQ(ERROR_SYNTAX_EXPRESSION_SUB_DOC_NOT_SUPPORT,
                         _requestValidator->getErrorCode());

    requestPtr = RequestCreator::prepareRequest(
            "config=cluster:cluster1,sub_doc:group&&query=a&&searcher_cache=use:yes,expire_time:sub_attr_uint32,cache_filter:attr_uint32=1");
    ASSERT_TRUE(requestPtr);
    ret = _requestValidator->validate(requestPtr);
    ASSERT_TRUE(!ret);
    ASSERT_EQ(ERROR_SYNTAX_EXPRESSION_SUB_DOC_NOT_SUPPORT,
                         _requestValidator->getErrorCode());

    requestPtr = RequestCreator::prepareRequest(
            "config=cluster:cluster1,sub_doc:flat&&query=a&&searcher_cache=use:yes,expire_time:attr_uint32,cache_filter:sub_attr_uint32=1");
    ASSERT_TRUE(requestPtr);
    ret = _requestValidator->validate(requestPtr);
    ASSERT_TRUE(ret);

    requestPtr = RequestCreator::prepareRequest(
            "config=cluster:cluster1,sub_doc:flat&&query=a&&searcher_cache=use:yes,expire_time:attr_uint32,cache_filter:sub_attr_uint32=1,refresh_attributes:not_exist;attr_int32");
    ASSERT_TRUE(requestPtr);
    ret = _requestValidator->validate(requestPtr);
    ASSERT_TRUE(!ret);

    requestPtr = RequestCreator::prepareRequest(
            "config=cluster:cluster1&&query=a&&virtual_attribute=va1:price+1&&searcher_cache=use:yes,expire_time:attr_uint32,cache_filter:sub_attr_uint32=1,refresh_attributes:va1;attr_int32");
    ASSERT_TRUE(requestPtr);
    ret = _requestValidator->validate(requestPtr);
    ASSERT_TRUE(!ret);
}

TEST_F(RequestValidatorTest, testQueryLayerClauseWithSubDoc) {
    {
        RequestPtr requestPtr = RequestCreator::prepareRequest(
                "config=cluster:cluster1&&query=mp3&&layer=range:%other,quota:10,range:sub_price{1,2,3}*type{[2,5]}");
        ASSERT_TRUE(requestPtr);
        ASSERT_TRUE(!_requestValidator->validate(requestPtr));
        ASSERT_EQ(ERROR_ATTRIBUTE_EXIST_IN_SUB_DOC,
                             _requestValidator->getErrorCode());
    }
    {
        RequestPtr requestPtr = RequestCreator::prepareRequest(
                "config=cluster:cluster1&&query=mp3&&layer=range:%other,quota:10,range:price{1,2,3}*sub_type{[2,5]}");
        ASSERT_TRUE(requestPtr);
        ASSERT_TRUE(!_requestValidator->validate(requestPtr));
        ASSERT_EQ(ERROR_ATTRIBUTE_EXIST_IN_SUB_DOC,
                             _requestValidator->getErrorCode());
    }
    {
        RequestPtr requestPtr = RequestCreator::prepareRequest(
                "config=cluster:cluster1,sub_doc:group&&query=mp3&&layer=range:%other,quota:10,range:sub_price{1,2,3}*type{[2,5]}");
        ASSERT_TRUE(requestPtr);
        ASSERT_TRUE(!_requestValidator->validate(requestPtr));
        ASSERT_EQ(ERROR_ATTRIBUTE_EXIST_IN_SUB_DOC,
                             _requestValidator->getErrorCode());
    }
    {
        RequestPtr requestPtr = RequestCreator::prepareRequest(
                "config=cluster:cluster1,sub_doc:group&&query=mp3&&layer=range:%other,quota:10,range:price{1,2,3}*sub_type{[2,5]}");
        ASSERT_TRUE(requestPtr);
        ASSERT_TRUE(!_requestValidator->validate(requestPtr));
        ASSERT_EQ(ERROR_ATTRIBUTE_EXIST_IN_SUB_DOC,
                             _requestValidator->getErrorCode());
    }
    {
        RequestPtr requestPtr = RequestCreator::prepareRequest(
                "config=cluster:cluster1,sub_doc:flat&&query=mp3&&layer=range:%other,quota:10,range:sub_price{1,2,3}*type{[2,5]}");
        ASSERT_TRUE(requestPtr);
        ASSERT_TRUE(!_requestValidator->validate(requestPtr));
        ASSERT_EQ(ERROR_ATTRIBUTE_EXIST_IN_SUB_DOC,
                             _requestValidator->getErrorCode());
    }
    {
        RequestPtr requestPtr = RequestCreator::prepareRequest(
                "config=cluster:cluster1,sub_doc:flat&&query=mp3&&layer=range:%other,quota:10,range:price{1,2,3}*sub_type{[2,5]}");
        ASSERT_TRUE(requestPtr);
        ASSERT_TRUE(!_requestValidator->validate(requestPtr));
        ASSERT_EQ(ERROR_ATTRIBUTE_EXIST_IN_SUB_DOC,
                             _requestValidator->getErrorCode());
    }
}

TEST_F(RequestValidatorTest, testQueryLayerClause) {
    {
        RequestPtr requestPtr = RequestCreator::prepareRequest(
                "config=cluster:cluster1&&query=mp3&&layer=range:%other,quota:10,range:price{1,2,3}*type{[2,5]}");
        ASSERT_TRUE(requestPtr);
        ASSERT_TRUE(_requestValidator->validate(requestPtr));
    }
    {
        RequestPtr requestPtr = RequestCreator::prepareRequest(
                "config=cluster:cluster1&&query=mp3&&layer=range:%other,quota:10,range:no_exist{1,2,3}*type{[2,5]}");
        ASSERT_TRUE(requestPtr);
        ASSERT_TRUE(!_requestValidator->validate(requestPtr));
        ASSERT_EQ(ERROR_ATTRIBUTE_NOT_EXIST,
                             _requestValidator->getErrorCode());
    }
    {
        RequestPtr requestPtr = RequestCreator::prepareRequest(
                "config=cluster:cluster1&&query=mp3&&layer=range:%other,quota:10,range:price{1,2,3}*no_exist{[2,5]}");
        ASSERT_TRUE(requestPtr);
        ASSERT_TRUE(!_requestValidator->validate(requestPtr));
        ASSERT_EQ(ERROR_ATTRIBUTE_NOT_EXIST,
                             _requestValidator->getErrorCode());
    }
    {
        RequestPtr requestPtr = RequestCreator::prepareRequest(
                "config=cluster:cluster1&&query=mp3&&layer=range:%docid{1},quota:10");
        ASSERT_TRUE(requestPtr);
        ASSERT_TRUE(_requestValidator->validate(requestPtr));
    }
    {
        RequestPtr requestPtr = RequestCreator::prepareRequest(
                "config=cluster:cluster1&&query=mp3&&layer=range:%other,quota:10,range:sub_type{[2,5]}");
        ASSERT_TRUE(requestPtr);
        ASSERT_TRUE(!_requestValidator->validate(requestPtr));
        ASSERT_EQ(ERROR_ATTRIBUTE_EXIST_IN_SUB_DOC,
                             _requestValidator->getErrorCode());
    }
    {
        RequestPtr requestPtr = RequestCreator::prepareRequest(
                "config=cluster:cluster1&&query=mp3&&layer=range:%other,quota:10,range:price{1,2,3}*sub_type{[2,5]}");
        ASSERT_TRUE(requestPtr);
        ASSERT_TRUE(!_requestValidator->validate(requestPtr));
        ASSERT_EQ(ERROR_ATTRIBUTE_EXIST_IN_SUB_DOC,
                             _requestValidator->getErrorCode());
    }

}

TEST_F(RequestValidatorTest, testValidateFinalSortClause) {
    HA3_LOG(DEBUG, "Begin Test!");
    ConfigClause configClause;
    ASSERT_TRUE(
            _requestValidator->validateFinalSortClause(NULL, &configClause));
    FinalSortClause finalSortClause;
    finalSortClause.setSortName(FinalSortClause::DEFAULT_SORTER);
    ASSERT_TRUE(_requestValidator->validateFinalSortClause(
                    &finalSortClause, &configClause));

    set<string> sorterNames;
    sorterNames.insert("sort1");
    sorterNames.insert("DEFAULT");
    (*(_requestValidator->_clusterSorterNamesPtr))["cluster1.default"] = sorterNames;
    finalSortClause.setSortName("sort1");
    configClause.addClusterName("cluster1");
    ASSERT_TRUE(_requestValidator->validateFinalSortClause(
                    &finalSortClause, &configClause));

    configClause.addClusterName("cluster2");
    ASSERT_TRUE(!_requestValidator->validateFinalSortClause(
                    &finalSortClause, &configClause));

    set<string> sorterNames2;
    sorterNames2.insert("sort1");
    sorterNames2.insert("sort2");
    sorterNames2.insert("DEFAULT");
    (*(_requestValidator->_clusterSorterNamesPtr))["cluster2.default"] = sorterNames2;
    ASSERT_TRUE(_requestValidator->validateFinalSortClause(
                    &finalSortClause, &configClause));

    (*(_requestValidator->_clusterSorterNamesPtr))["cluster2.default"].erase("sort1");
    ASSERT_TRUE(!_requestValidator->validateFinalSortClause(
                    &finalSortClause, &configClause));
}

void RequestValidatorTest::checkErrorResult(string queryStr, ErrorCode errorCode) {
    std::tr1::shared_ptr<RequestParser> requestParserPtr(new RequestParser());
    config::ClusterConfigInfoPtr clusterConfigInfoPtr;
    config::ClusterConfigMapPtr clusterConfigMapPtr;
    clusterConfigInfoPtr.reset(new ClusterConfigInfo());
    clusterConfigInfoPtr->_tableName = "table1";
    clusterConfigInfoPtr->setQueryInfo(QueryInfo("default0", OP_AND));
    clusterConfigMapPtr.reset(new ClusterConfigMap);
    clusterConfigMapPtr->insert(make_pair("cluster1.default", *clusterConfigInfoPtr));

    HA3_LOG(DEBUG, "begin check[%s]", queryStr.c_str());
    RequestPtr requestPtr(new Request);
    requestPtr->setOriginalString(queryStr);
    HA3_LOG(INFO, "queryStr is [%s]", queryStr.c_str());
    bool ret = requestParserPtr->parseRequest(requestPtr, clusterConfigMapPtr);
    (void)ret;
    assert(ret);

    ret = _requestValidator->validate(requestPtr);
    if (errorCode != ERROR_NONE) {
        assert(!ret);
    } else {
        assert(ret);
    }
    string expectStr = string("Expected: ") + haErrorCodeToString(errorCode) +
                       "\n- Actual: " + _requestValidator->getErrorMsg() +
                       "\n- QueryString: " + queryStr;
    HA3_LOG(INFO, "expectStr is [%s]", expectStr.c_str());
    assert(errorCode == _requestValidator->getErrorCode());
}

END_HA3_NAMESPACE(qrs);
