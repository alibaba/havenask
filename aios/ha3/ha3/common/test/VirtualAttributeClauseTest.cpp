#include<unittest/unittest.h>
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/common/VirtualAttributeClause.h>
#include <suez/turing/expression/syntax/AtomicSyntaxExpr.h>
#include <ha3_sdk/testlib/qrs/RequestCreator.h>

using namespace std;
using namespace suez::turing;
USE_HA3_NAMESPACE(queryparser);
USE_HA3_NAMESPACE(qrs);
USE_HA3_NAMESPACE(util);

BEGIN_HA3_NAMESPACE(common);

class VirtualAttributeClauseTest : public TESTBASE {
public:
    VirtualAttributeClauseTest();
    ~VirtualAttributeClauseTest();
public:
    void setUp();
    void tearDown();
protected:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(common, VirtualAttributeClauseTest);


VirtualAttributeClauseTest::VirtualAttributeClauseTest() { 
}

VirtualAttributeClauseTest::~VirtualAttributeClauseTest() { 
}

void VirtualAttributeClauseTest::setUp() { 
    HA3_LOG(DEBUG, "setUp!");
}

void VirtualAttributeClauseTest::tearDown() { 
    HA3_LOG(DEBUG, "tearDown!");
}

TEST_F(VirtualAttributeClauseTest, testSerializeAndDeserialize) { 
    HA3_LOG(DEBUG, "Begin Test!");
    
    VirtualAttributeClause virtualAttributeClause;
    virtualAttributeClause.setOriginalString("original string");
    SyntaxExpr* syntaxExpr = new AtomicSyntaxExpr("a",
            vt_int32, ATTRIBUTE_NAME);
    ASSERT_TRUE(virtualAttributeClause.addVirtualAttribute("a", 
                    syntaxExpr));
    syntaxExpr = new AtomicSyntaxExpr("b",
            vt_unknown, ATTRIBUTE_NAME);
    ASSERT_TRUE(virtualAttributeClause.addVirtualAttribute("b", 
                    syntaxExpr));
        
    //serialize and deserialize
    VirtualAttributeClause virtualAttributeClause2;
    autil::DataBuffer buffer;
    virtualAttributeClause.serialize(buffer);
    virtualAttributeClause2.deserialize(buffer);

    ASSERT_EQ(virtualAttributeClause.getOriginalString(), 
                         virtualAttributeClause2.getOriginalString());
    ASSERT_EQ(virtualAttributeClause._virtualAttributes.size(),
                         virtualAttributeClause2._virtualAttributes.size());
    for (size_t i = 0; 
         i < virtualAttributeClause._virtualAttributes.size(); ++i) 
    {
        VirtualAttribute *virtualAttribute1 = 
            virtualAttributeClause._virtualAttributes[i];
        VirtualAttribute *virtualAttribute2 = 
            virtualAttributeClause2._virtualAttributes[i];
        ASSERT_EQ(virtualAttribute1->getVirtualAttributeName(), 
                             virtualAttribute2->getVirtualAttributeName());
        ASSERT_TRUE(*virtualAttribute1->getVirtualAttributeSyntaxExpr() 
                       == virtualAttribute2->getVirtualAttributeSyntaxExpr());
    }
}

TEST_F(VirtualAttributeClauseTest, testAddVirtualAttribute) { 
    HA3_LOG(DEBUG, "Begin Test!");
    VirtualAttributeClause virtualAttributeClause;
    string name1 = "name1";
    SyntaxExpr* syntaxExpr1 = new AtomicSyntaxExpr("a",
            vt_int32, ATTRIBUTE_NAME);
    string name2 = "name2";
    SyntaxExpr* syntaxExpr2 = new AtomicSyntaxExpr("b",
            vt_int32, ATTRIBUTE_NAME);
    ASSERT_TRUE(!virtualAttributeClause.addVirtualAttribute("", NULL));
    ASSERT_TRUE(!virtualAttributeClause.addVirtualAttribute(name1, NULL));
    ASSERT_TRUE(!virtualAttributeClause.addVirtualAttribute("", syntaxExpr1));
    ASSERT_TRUE(virtualAttributeClause.addVirtualAttribute(name1, syntaxExpr1));
    ASSERT_TRUE(!virtualAttributeClause.addVirtualAttribute(name1, syntaxExpr2));
    ASSERT_TRUE(virtualAttributeClause.addVirtualAttribute(name2, syntaxExpr2));
    ASSERT_EQ((size_t)2, 
                         virtualAttributeClause._virtualAttributes.size());
    ASSERT_EQ(syntaxExpr1, 
                         virtualAttributeClause._virtualAttributes[0]
                         ->getVirtualAttributeSyntaxExpr());
    ASSERT_EQ(syntaxExpr2, 
                         virtualAttributeClause._virtualAttributes[1]
                         ->getVirtualAttributeSyntaxExpr());
}

TEST_F(VirtualAttributeClauseTest, testToString) {
    string requestStr = "virtual_attribute=DISCNT_T:discnt_t(zk_time,zk_rate);ENDTIME:endtime(ends);PS:uvsum_suc";
    RequestPtr requestPtr = RequestCreator::prepareRequest(requestStr);
    ASSERT_TRUE(requestPtr);
    VirtualAttributeClause* virtualAttributeClause 
        = requestPtr->getVirtualAttributeClause();
    ASSERT_EQ(string("[DISCNT_T:discnt_t(zk_time , zk_rate)][ENDTIME:endtime(ends)][PS:uvsum_suc]"), 
                         virtualAttributeClause->toString());
}

END_HA3_NAMESPACE(common);

