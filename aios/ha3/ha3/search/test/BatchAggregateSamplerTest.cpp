#include<unittest/unittest.h>
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/search/BatchAggregateSampler.h>
#include <ha3/common/Ha3MatchDocAllocator.h>
#include <matchdoc/MatchDoc.h>
#include <ha3_sdk/testlib/common/MatchDocCreator.h>
#include <autil/StringTokenizer.h>
#include <autil/StringUtil.h>
#include <algorithm>
using namespace std;
using namespace autil;

BEGIN_HA3_NAMESPACE(search);

class BatchAggregateSamplerTest : public TESTBASE {
public:
    BatchAggregateSamplerTest();
    ~BatchAggregateSamplerTest();
public:
    void setUp();
    void tearDown();
protected:
    void initSampler(const std::string& docidStr, 
                     uint32_t aggThreshold,
                     uint32_t sampleMaxCount);
    void checkDocids(const std::string& resultDocidStr);
protected:
    BatchAggregateSampler _sampler;
    autil::mem_pool::Pool _pool;
    common::Ha3MatchDocAllocator *_allocator;
protected:
    HA3_LOG_DECLARE();
};

USE_HA3_NAMESPACE(common);
HA3_LOG_SETUP(search, BatchAggregateSamplerTest);


BatchAggregateSamplerTest::BatchAggregateSamplerTest() { 
}

BatchAggregateSamplerTest::~BatchAggregateSamplerTest() { 
}

void BatchAggregateSamplerTest::setUp() { 
    HA3_LOG(DEBUG, "setUp!");

    _allocator = new common::Ha3MatchDocAllocator(&_pool, true);
}

void BatchAggregateSamplerTest::tearDown() { 
    HA3_LOG(DEBUG, "tearDown!");

    DELETE_AND_SET_NULL(_allocator);
}

TEST_F(BatchAggregateSamplerTest, testSimpleProcess) { 
    const string docStr = "0 ; 1:1,2,3 ; 2 ; 3:5,6,7 # 4 ; 5 ; 6";
    initSampler(docStr, 10, 10);
    checkDocids("0; 1:1,2,3 ; 2 ; 3:5,6,7 #4;5;6");

    initSampler(docStr, 5, 5);
    checkDocids("0; 2 #4;6");

    initSampler(docStr, 4, 4);
    checkDocids("0; 2 # 4;6");

    initSampler(docStr, 10, 5);
    checkDocids("0; 1:1,2,3 ; 2 ; 3:5,6,7#4;5;6");

    initSampler(docStr, 5, 3);
    checkDocids("0; 3:5,6,7 #6");

    initSampler(docStr, 4, 2);
    checkDocids("0#4");

    initSampler(docStr, 0, 2);
    checkDocids("0#4");

    initSampler(docStr, 4, 0);
    checkDocids("");

    initSampler(docStr, 0, 0);
    checkDocids("0; 1:1,2,3 ; 2 ; 3:5,6,7 #4;5;6");
}

void BatchAggregateSamplerTest::checkDocids(const string& resultDocidStr) {
    vector<vector<pair<docid_t, vector<docid_t> > > > layers;    
    int32_t stMode = StringTokenizer::TOKEN_TRIM | StringTokenizer::TOKEN_IGNORE_EMPTY;
    StringTokenizer layerStr(resultDocidStr, "#", stMode);
    for (size_t i = 0; i < layerStr.getNumTokens(); ++i) {
        StringTokenizer docs(layerStr[i], ";", stMode);
        layers.resize(layers.size() + 1);
        for (size_t j = 0; j < docs.getNumTokens(); ++j) {
            StringTokenizer docStr(docs[j], ":", stMode);
            pair<docid_t, vector<docid_t> > doc;
            StringUtil::fromString(docStr[0], doc.first);
            if (1 != docStr.getNumTokens()) {
                StringTokenizer subdocs(docStr[1], ",", stMode);
                for (size_t k = 0; k < subdocs.getNumTokens(); k++) {
                    docid_t subdoc;
                    StringUtil::fromString(subdocs[k], subdoc);
                    doc.second.push_back(subdoc);
                }
            }
            layers.back().push_back(doc);
        }
    }
    uint32_t count = 0;
    for (size_t i = 0; i < layers.size(); ++i) {
        ASSERT_TRUE(_sampler.hasNextLayer());
        for (size_t j = 0; j < layers[i].size(); ++j) {
            ASSERT_TRUE(_sampler.hasNext());
            docid_t docid = _sampler.getCurDocId();
            ++count;
            ASSERT_EQ(layers[i][j].first, docid);
            _sampler.beginSubDoc();
            size_t pos = 0;
            while (true) {
                docid_t subdocid = _sampler.nextSubDocId();
                if (subdocid == INVALID_DOCID) {
                    break;
                }
                ASSERT_EQ(layers[i][j].second[pos++], subdocid);
            }
            _sampler.next();
        }
        ASSERT_TRUE(!_sampler.hasNext());
        ASSERT_EQ(double(i+1), _sampler.getLayerFactor());
    }
    ASSERT_TRUE(!_sampler.hasNextLayer());
    ASSERT_EQ(count, _sampler.getAggregateCount());
}

void BatchAggregateSamplerTest::initSampler(
        const string& docidStr,
        uint32_t aggThreshold,
        uint32_t sampleMaxCount) {
    _sampler.reset();
    _sampler.setAggThreshold(aggThreshold);
    _sampler.setSampleMaxCount(sampleMaxCount);
    int32_t stMode = StringTokenizer::TOKEN_TRIM | StringTokenizer::TOKEN_IGNORE_EMPTY;
    StringTokenizer layers(docidStr, "#", stMode);
    for (size_t i = 0; i < layers.getNumTokens(); ++i) {
        SimpleDocIdIterator iterator = 
            MatchDocCreator::createDocIterator(layers[i]);
        while (iterator.hasNext()) {
            matchdoc::MatchDoc matchDoc = _allocator->allocateWithSubDoc(&iterator);
            _sampler.collect(matchDoc, _allocator);
            iterator.next();
        }
        _sampler.endLayer(double(i+1));
    }
    _sampler.calculateSampleStep();
}

END_HA3_NAMESPACE(search);

