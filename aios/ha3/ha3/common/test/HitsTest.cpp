#include<unittest/unittest.h>
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/common/Hits.h>
#include <ha3/common/Hits.h>
#include <ha3/test/test.h>

using namespace std;

USE_HA3_NAMESPACE(util);
USE_HA3_NAMESPACE(config);
BEGIN_HA3_NAMESPACE(common);

class HitsTest : public TESTBASE {
public:
    HitsTest();
    ~HitsTest();
public:
    void setUp();
    void tearDown();
protected:
    void testSerializeAndDeserializeByFlag(bool useJson);
    void checkHit(HitPtr resultHit, docid_t docid, bool useJson);
    Hit* prepareHit(docid_t docid, 
                    config::HitSummarySchema *hitSummarySchema);
    Hits* prepareHits(int docIdBase, bool diffSchema = false, uint32_t pos=0);
protected:
    autil::mem_pool::Pool *_pool;
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(common, HitsTest);


HitsTest::HitsTest() { 
}

HitsTest::~HitsTest() { 
}

void HitsTest::setUp() { 
    HA3_LOG(DEBUG, "setUp!");
    _pool = new autil::mem_pool::Pool(10 * 1024 * 1024);
}

void HitsTest::tearDown() { 
    HA3_LOG(DEBUG, "tearDown!");
    delete _pool;
    _pool = NULL;
}

TEST_F(HitsTest, testSerializeAndDeserialize) { 
    testSerializeAndDeserializeByFlag(true);
    testSerializeAndDeserializeByFlag(false);
}
void HitsTest::testSerializeAndDeserializeByFlag(bool useJson) { 
    HA3_LOG(DEBUG, "Begin Test!");

    Hits hits;

    config::HitSummarySchema *hitSummarySchema = new config::HitSummarySchema(NULL);
    hitSummarySchema->declareSummaryField("price1", ft_string, false);
    hitSummarySchema->declareSummaryField("ttt", ft_string, false);
    hitSummarySchema->declareSummaryField("price2", ft_string, false);

    hits.addHitSummarySchema(hitSummarySchema);

    Hit *hit1 = prepareHit(1, hitSummarySchema);
    Hit *hit2 = prepareHit(2, hitSummarySchema);
    Hit *hit3 = prepareHit(3, hitSummarySchema);

    hits.addHit(HitPtr(hit1));
    hits.addHit(HitPtr(hit2));
    hits.addHit(HitPtr(hit3));

    hits.setSummaryFilled();
    hits.setTotalHits(100);

    Hits resultHits;         
    {
        autil::DataBuffer dataBuffer;
        hits.serialize(dataBuffer);
        resultHits.deserialize(dataBuffer, _pool);
    }
    {
        autil::DataBuffer dataBuffer;
        hits.serialize(dataBuffer);
        resultHits.deserialize(dataBuffer, _pool);
    }
    ASSERT_EQ((uint32_t)3, resultHits.size());
    ASSERT_TRUE(resultHits.hasSummary());

    resultHits.adjustHitSummarySchemaInHit();

    HitPtr hitPtr1 = resultHits.getHit(0);
    HitPtr hitPtr2 = resultHits.getHit(1);
    HitPtr hitPtr3 = resultHits.getHit(2);
    checkHit(hitPtr1, 1, useJson);
    checkHit(hitPtr2, 2, useJson);
    checkHit(hitPtr3, 3, useJson);


    ASSERT_EQ((uint32_t)100, resultHits.totalHits());
}

TEST_F(HitsTest, testStealAndMergeHits) { 
    HA3_LOG(DEBUG, "Begin Test!");

    Hits *hits = prepareHits(100);
    Hits *otherHits = prepareHits(200);
    unique_ptr<Hits> ptr_hit1(hits);
    unique_ptr<Hits> ptr_hit2(otherHits);

    bool stealResult = hits->stealAndMergeHits(*otherHits);
    ASSERT_TRUE(stealResult);
    ASSERT_EQ((uint32_t)6, hits->size());

    for(int i = 0; i < 3; i++) {
        HitPtr hitPtr = hits->getHit(i);
        docid_t docId = hitPtr->getDocId();
        ASSERT_EQ((docid_t)(i + 100), docId);
    }
    for(int i = 3; i < 6; i++) {
        HitPtr hitPtr = hits->getHit(i);
        docid_t docId = hitPtr->getDocId();
        ASSERT_EQ((docid_t)(i - 3 + 200), docId);
    }
}

TEST_F(HitsTest, testStealAndMergeHitsByDedup) { 
    HA3_LOG(DEBUG, "Begin Test!");

    Hits *hits = prepareHits(100);
    Hits *otherHits = prepareHits(200);
    unique_ptr<Hits> ptr_hit1(hits);
    unique_ptr<Hits> ptr_hit2(otherHits);

    hits->getHit(0)->setPrimaryKey(1);
    hits->getHit(1)->setPrimaryKey(2);
    hits->getHit(2)->setPrimaryKey(3);

    otherHits->getHit(0)->setPrimaryKey(2);
    otherHits->getHit(1)->setPrimaryKey(1);
    otherHits->getHit(2)->setPrimaryKey(4);

    bool stealResult = hits->stealAndMergeHits(*otherHits, true);
    ASSERT_TRUE(stealResult);
    ASSERT_EQ((uint32_t)4, hits->size());

    for(int i = 0; i < 3; i++) {
        HitPtr hitPtr = hits->getHit(i);
        docid_t docId = hitPtr->getDocId();
        ASSERT_EQ((docid_t)(i + 100), docId);
    }
    ASSERT_EQ((docid_t)202, hits->getHit(3)->getDocId());
}

TEST_F(HitsTest, testStealAndMergeHitsByDedupWithVersion) { 
    HA3_LOG(DEBUG, "Begin Test!");

    Hits *hits = prepareHits(100);
    Hits *otherHits = prepareHits(200);
    Hits *otherHits1 = prepareHits(300);
    unique_ptr<Hits> ptr_hit1(hits);
    unique_ptr<Hits> ptr_hit2(otherHits);
    unique_ptr<Hits> ptr_hit3(otherHits1);

    hits->getHit(0)->setPrimaryKey(1);
    hits->getHit(1)->setPrimaryKey(2);
    hits->getHit(2)->setPrimaryKey(3);
    hits->getHit(0)->setFullIndexVersion(1);
    hits->getHit(1)->setFullIndexVersion(1);
    hits->getHit(2)->setFullIndexVersion(1);

    otherHits->getHit(0)->setPrimaryKey(2);
    otherHits->getHit(1)->setPrimaryKey(1);
    otherHits->getHit(2)->setPrimaryKey(4);
    otherHits->getHit(0)->setFullIndexVersion(2);
    otherHits->getHit(1)->setFullIndexVersion(2);
    otherHits->getHit(2)->setFullIndexVersion(2);

    otherHits1->getHit(0)->setPrimaryKey(5);
    otherHits1->getHit(1)->setPrimaryKey(1);
    otherHits1->getHit(2)->setPrimaryKey(5);
    otherHits1->getHit(0)->setFullIndexVersion(2);
    otherHits1->getHit(1)->setFullIndexVersion(2);
    otherHits1->getHit(2)->setFullIndexVersion(2);

    bool stealResult = hits->stealAndMergeHits(*otherHits, true);
    ASSERT_TRUE(stealResult);
    ASSERT_EQ((uint32_t)4, hits->size());

    ASSERT_EQ((docid_t)201, hits->getHit(0)->getDocId());
    ASSERT_EQ((docid_t)200, hits->getHit(1)->getDocId());
    ASSERT_EQ((docid_t)102, hits->getHit(2)->getDocId());
    ASSERT_EQ((docid_t)202, hits->getHit(3)->getDocId());
    
    stealResult = hits->stealAndMergeHits(*otherHits1, true);
    ASSERT_TRUE(stealResult);
    ASSERT_EQ((uint32_t)5, hits->size());
    ASSERT_EQ((docid_t)300, hits->getHit(4)->getDocId());
}

TEST_F(HitsTest, testStealSummary)
{
    HA3_LOG(DEBUG, "Begin Test!");
    vector<Hits*> hitsVec;
    std::string text = "summary_textx";
    std::string value = "summary_valuex";

    uint32_t pos = 0;
    for(int i = 0; i < 5; i++) {
        Hits *hits = prepareHits((int32_t)pos, false, pos);
        pos += hits->size();
        HitSummarySchema *hitSummarySchema = hits->_hitSummarySchemas.begin()->second;
        for (uint32_t j = 0; j < hits->size(); ++j) {
            SummaryHit *summaryHit = new SummaryHit(hitSummarySchema, _pool);
            summaryHit->setFieldValue(1, value.data(), value.size());
            hits->getHit(j)->setSummaryHit(summaryHit);
            summaryHit->summarySchemaToSignature();
        }
        hitsVec.push_back(hits);
    }
    Hits hits;
    for(int i = 0; i < (int)pos; i++) {
        HitPtr hitPtr(prepareHit(i, NULL));
        hitPtr->setHashId(0);
        hitPtr->setPosition(i);
        hits.addHit(hitPtr);
    }
    ASSERT_EQ(size_t(0), hits.stealSummary(hitsVec));

    hits.adjustHitSummarySchemaInHit();

    for(int i = 0; i < (int)pos; i++)
    {
        HitPtr hitPtr = hits.getHit(i);
        ASSERT_EQ(uint32_t(3), hitPtr->getSummaryCount());
        ASSERT_EQ(value, hitPtr->getSummaryValue(text));
    }

    for (vector<Hits*>::iterator it = hitsVec.begin();
         it != hitsVec.end(); ++it)
    {
        delete *it;
    }
}

TEST_F(HitsTest, testStealSummaryWithDiffSchema)
{
    HA3_LOG(DEBUG, "Begin Test!");
    vector<Hits*> hitsVec;
    std::string text = "summary_textx";
    std::string value = "summary_valuex";

    uint32_t pos = 0;
    for(int i = 0; i < 5; i++) {
        bool diffSchema = i % 2;
        Hits *hits = prepareHits((int)pos, diffSchema, pos);
        pos += hits->size();
        HitSummarySchema *hitSummarySchema = hits->_hitSummarySchemas.begin()->second;
        for (uint32_t j = 0; j < hits->size(); ++j) {
            SummaryHit *summaryHit = new SummaryHit(hitSummarySchema, _pool);
            int32_t valueFieldId = diffSchema ? 2 : 1;
            summaryHit->setFieldValue(valueFieldId, value.data(), value.size());
            hits->getHit(j)->setSummaryHit(summaryHit);
            summaryHit->summarySchemaToSignature();
        }
        hitsVec.push_back(hits);
    }
    Hits hits;
    for(int i = 0; i < (int)pos; i++) {
        HitPtr hitPtr(prepareHit(i, NULL));
        hitPtr->setHashId(0);
        hitPtr->setPosition(i);
        hits.addHit(hitPtr);
    }
    ASSERT_EQ(size_t(0), hits.stealSummary(hitsVec));
    hits.adjustHitSummarySchemaInHit();
    for(int i = 0; i < (int)pos; i++)
    {
        HitPtr hitPtr = hits.getHit(i);
        ASSERT_EQ(uint32_t(3), hitPtr->getSummaryCount());
        ASSERT_EQ(value, hitPtr->getSummaryValue(text));
    }

    for (vector<Hits*>::iterator it = hitsVec.begin();
         it != hitsVec.end(); ++it)
    {
        delete *it;
    }
}

TEST_F(HitsTest, testStealSummaryWithEmptySummary)
{
    HA3_LOG(DEBUG, "Begin Test!");
    vector<Hits*> hitsVec;
    std::string text = "summary_textx";
    std::string value = "summary_valuex";

    uint32_t pos = 0;
    for(int i = 0; i < 5; i++) {
        Hits *hits = prepareHits((int)pos, false, pos);
        pos += hits->size();
        HitSummarySchema *hitSummarySchema = hits->_hitSummarySchemas.begin()->second;
        for (uint32_t j = 0; j < hits->size(); ++j) {
            if (i == 0 && j == 1) {
                continue;
            }
            SummaryHit *summaryHit = new SummaryHit(hitSummarySchema, _pool);
            summaryHit->setFieldValue(1, value.data(), value.size());
            hits->getHit(j)->setSummaryHit(summaryHit);
            summaryHit->summarySchemaToSignature();
        }
        hitsVec.push_back(hits);
    }
    Hits hits;
    for(int i = 0; i < (int)pos; i++) {
        HitPtr hitPtr(prepareHit(i, NULL));
        hitPtr->setHashId(0);
        hitPtr->setPosition(i);
        hits.addHit(hitPtr);
    }
    ASSERT_EQ(size_t(1), hits.stealSummary(hitsVec));

    hits.adjustHitSummarySchemaInHit();

    for(int i = 0; i < (int)pos; i++)
    {
        HitPtr hitPtr = hits.getHit(i);
        if (i == 1) {
            ASSERT_EQ((uint32_t)0, hitPtr->getSummaryCount());
        } else {
            ASSERT_EQ((uint32_t)3, hitPtr->getSummaryCount());
            ASSERT_EQ(value, hitPtr->getSummaryValue(text));
        }
    }

    for (vector<Hits*>::iterator it = hitsVec.begin();
         it != hitsVec.end(); ++it)
    {
        delete *it;
    }
}

TEST_F(HitsTest, testSortHits) {
    Hits *hits = prepareHits(100);
    unique_ptr<Hits> ptr_hit1(hits);

    hits->setSortAscendFlag(false);
    hits->sortHits();
    ASSERT_EQ(docid_t(102), hits->getHit(0)->getDocId());
    ASSERT_EQ(docid_t(101), hits->getHit(1)->getDocId());
    ASSERT_EQ(docid_t(100), hits->getHit(2)->getDocId());

    hits->setSortAscendFlag(true);
    hits->sortHits();
    ASSERT_EQ(docid_t(100), hits->getHit(0)->getDocId());
    ASSERT_EQ(docid_t(101), hits->getHit(1)->getDocId());
    ASSERT_EQ(docid_t(102), hits->getHit(2)->getDocId());
}

Hit* HitsTest::prepareHit(docid_t docid, config::HitSummarySchema *hitSummarySchema) {
    Hit *hit = new Hit(docid);
    hit->setHashId(12345);
    if (hitSummarySchema) {
        SummaryHit *summaryHit = new SummaryHit(hitSummarySchema, _pool);
        summaryHit->setFieldValue(0, "100", 3);
        summaryHit->setFieldValue(2, "200", 3);
        hit->setSummaryHit(summaryHit);
    }
    hit->addAttribute<int32_t>(string("attr_int32"), int32_t(1000));
    hit->addAttribute<float>(string("attr_float"), float(1.005));
    hit->addAttribute<string>(string("attr_string"), string("attr_string_value"));
    hit->addAttribute<double>(string("attr_double"), double(10000));

    hit->insertProperty("red", "111");
    return hit;
}

void HitsTest::checkHit(HitPtr resultHit, docid_t docid, bool useJson) {

    ASSERT_EQ(docid, resultHit->getDocId());
    if (!useJson) {
        ASSERT_EQ((uint32_t)3, resultHit->getSummaryCount());
        ASSERT_EQ(string("100"), resultHit->getSummaryValue("price1"));
        ASSERT_EQ(string(""), resultHit->getSummaryValue("summary_textx"));
        ASSERT_EQ(string("200"), resultHit->getSummaryValue("price2"));
    }
    int32_t int32Value;
    bool ret = resultHit->getAttribute<int32_t>(string("attr_int32"), int32Value); 
    ASSERT_TRUE(ret);
    ASSERT_EQ(int32_t(1000), int32Value);

    float floatValue;
    ret = resultHit->getAttribute<float>(string("attr_float"), floatValue); 
    ASSERT_TRUE(ret);
    ASSERT_EQ(float(1.005), floatValue);
    ret = resultHit->getAttribute<float>(string("attr_int32"), floatValue); 
    ASSERT_FALSE(ret);

    string stringValue;
    ret = resultHit->getAttribute<string>(string("attr_string"), stringValue); 
    ASSERT_TRUE(ret);
    ASSERT_EQ(string("attr_string_value"), stringValue);

    ret = resultHit->getAttribute<string>(string("attr_string2"), stringValue); 
    ASSERT_FALSE(ret);

    double doubleValue;
    ret = resultHit->getAttribute<double>(string("attr_double"), doubleValue); 
    ASSERT_TRUE(ret);
    ASSERT_EQ(10000.0, doubleValue);

    ASSERT_EQ((hashid_t)12345, resultHit->getHashId());
    const PropertyMap& propertyMap = resultHit->getPropertyMap();
    ASSERT_EQ((size_t)1, propertyMap.size());
    PropertyMap::const_iterator it = propertyMap.find("red");
    ASSERT_TRUE(it != propertyMap.end());
    ASSERT_EQ(string("111"), (*it).second);
}

Hits* HitsTest::prepareHits(int docIdBase, bool diffSchema, uint32_t startPos) {
/**
  * i = 0 - 3
  * j = 0 - 4
  * attr_100:  100, 101, 102
  * attr_101:  101, 102, 103
  * attr_102:  102, 103, 104
  * attr_103:  103, 104, 105
  */

    Hits* hits = new Hits();
    vector<string> attrVect;
    for (int i = 0; i < 4; i++) {
        stringstream ss;
        ss<<"attr_"<< (i + docIdBase);
        attrVect.push_back(ss.str());
    }

    for(int i = 0; i < 3; i++) {
        HitPtr hitPtr(new Hit(docIdBase + i));
        for (size_t j = 0; j < attrVect.size(); j++ ) {
            hitPtr->addAttribute<int32_t>(attrVect[j], docIdBase + j + i);
        }
        hitPtr->setPosition(startPos++);
        hitPtr->setSortValue(i * 1.0);
        hits->addHit(hitPtr);
    }

    config::HitSummarySchema *hitSummarySchema = new config::HitSummarySchema(NULL);
    if (diffSchema) {
        hitSummarySchema->declareSummaryField("price1", ft_string, false);
        hitSummarySchema->declareSummaryField("price2", ft_string, false);        
        hitSummarySchema->declareSummaryField("summary_textx", ft_string, false);
    } else {
        hitSummarySchema->declareSummaryField("price1", ft_string, false);
        hitSummarySchema->declareSummaryField("summary_textx", ft_string, false);
        hitSummarySchema->declareSummaryField("price2", ft_string, false);
    }
    hitSummarySchema->calcSignature();
    hits->addHitSummarySchema(hitSummarySchema);

    return hits;
}

TEST_F(HitsTest, testInsertAndDeleteHit) {
    HitPtr hitPtr1(new Hit(1));
    HitPtr hitPtr2(new Hit(2));

    Hits *hits = new Hits();
    config::HitSummarySchema *hitSummarySchema = new config::HitSummarySchema(NULL);
    hitSummarySchema->declareSummaryField("price1", ft_string, false);
    hitSummarySchema->declareSummaryField("ttt", ft_string, false);
    hitSummarySchema->declareSummaryField("price2", ft_string, false);

    hits->addHitSummarySchema(hitSummarySchema);

    unique_ptr<Hits> ptr_hit1(hits);

    hits->addHit(hitPtr1);
    hits->addHit(hitPtr2);

    ASSERT_EQ((uint32_t)2, hits->size());
    ASSERT_EQ((docid_t)1, hits->getHit(0)->getDocId());
    ASSERT_EQ((docid_t)2, hits->getHit(1)->getDocId());

    HitPtr hitPtr3(new Hit(3));
    hits->insertHit(1, hitPtr3);
    ASSERT_EQ((uint32_t)3, hits->size());
    ASSERT_EQ((docid_t)1, hits->getHit(0)->getDocId());
    ASSERT_EQ((docid_t)3, hits->getHit(1)->getDocId());
    ASSERT_EQ((docid_t)2, hits->getHit(2)->getDocId());

    hits->deleteHit(1);
    ASSERT_EQ((uint32_t)2, hits->size());
    ASSERT_EQ((docid_t)1, hits->getHit(0)->getDocId());
    ASSERT_EQ((docid_t)2, hits->getHit(1)->getDocId());
}


END_HA3_NAMESPACE(common);

