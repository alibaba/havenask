#include<unittest/unittest.h>
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <autil/mem_pool/Pool.h>
#include <ha3/common/Tracer.h>
#include <ha3/rank/ScoringProvider.h>
#include <string>
#include <ha3/common/Ha3MatchDocAllocator.h>
#include <suez/turing/common/KvTracerMacro.h>
using namespace std;
using namespace autil::legacy;
BEGIN_HA3_NAMESPACE(common);

class TracerTest : public TESTBASE {
public:
    TracerTest();
    ~TracerTest();
public:
    void setUp();
    void tearDown();
protected:
    autil::mem_pool::Pool _pool;
protected:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(common, TracerTest);
USE_HA3_NAMESPACE(search);

TracerTest::TracerTest() { 
}

TracerTest::~TracerTest() { 
}

void TracerTest::setUp() { 
    HA3_LOG(DEBUG, "setUp!");
}

void TracerTest::tearDown() { 
    HA3_LOG(DEBUG, "tearDown!");
}

TEST_F(TracerTest, testSimpleProcess) { 
    HA3_LOG(DEBUG, "Begin Test!");

    Tracer tracer;
    tracer.setLevel("DEBUG");
    ASSERT_TRUE(tracer.isLevelEnabled(ISEARCH_TRACE_INFO));
    ASSERT_TRUE(!tracer.isLevelEnabled(ISEARCH_TRACE_TRACE1));
    tracer.trace("trace info 1");
    tracer.trace("trace info 2");
    autil::DataBuffer dataBuffer;
    tracer.serialize(dataBuffer);
    Tracer tracer1;
    tracer1.deserialize(dataBuffer);

    string xmlStr = tracer.toXMLString();
    string xmlStr1 = tracer1.toXMLString();
//    ASSERT_EQ(xmlStr1, string(""));
    ASSERT_EQ(xmlStr, xmlStr1);
}

TEST_F(TracerTest, testRankTrace)
{
    HA3_LOG(DEBUG, "Begin Test!");
    
    common::Ha3MatchDocAllocator allocator(&_pool);
    matchdoc::Reference<Tracer>* ref = allocator.declare<Tracer>("ref");
    matchdoc::MatchDoc matchDoc = allocator.allocate(100);
    matchdoc::Reference<Tracer>* _traceRefer;
    _traceRefer = ref;
    common::Tracer *tracer1 = _traceRefer->getPointer(matchDoc); 
    tracer1->setLevel("DEBUG");
    const char *buf = "test buf name";
    RANK_TRACE(DEBUG, matchDoc, "%s", buf);
    string resultStr=tracer1->getTraceInfo();
    int len=strlen(buf);
    string substr=resultStr.substr(resultStr.length()-len-1,len); 
    ASSERT_EQ(string(buf),substr );
    allocator.deallocate(matchDoc);
}

TEST_F(TracerTest, testMergeTrace) {    
    HA3_LOG(DEBUG, "Begin Test!");

    Tracer tracer1;
    tracer1.setLevel("DEBUG");
    tracer1.trace("trace info 1");
    tracer1.trace("trace info 2");

    Tracer tracer2;
    tracer2.setLevel("DEBUG");
    tracer2.trace("trace info 5");
    tracer2.trace("trace info 6");
    
    tracer1.merge(&tracer2);
    std::string expecrtTraceInfo = "trace info 1\ntrace info "
                                   "2\ntrace info 5\ntrace info 6\n";
    ASSERT_EQ(expecrtTraceInfo, tracer1.getTraceInfo());

    Tracer tracer3;
    tracer1.merge(&tracer3);
    ASSERT_EQ(expecrtTraceInfo, tracer1.getTraceInfo());

    tracer3.merge(&tracer1);
    ASSERT_EQ(expecrtTraceInfo, tracer3.getTraceInfo());

}

TEST_F(TracerTest, testTrace) {
    HA3_LOG(DEBUG, "Begin Test!");

    Tracer *_tracer = new Tracer;
    _tracer->setLevel("TRACE3");
    _tracer->trace("trace info 1");
    _tracer->trace("trace info 2");
    SET_TRACE_POS_FRONT();
    _tracer->trace("trace info 3");
    _tracer->trace("trace info 4");
    UNSET_TRACE_POS_FRONT();    
    _tracer->trace("trace info 5");
    ASSERT_EQ(
            string("trace info 4\ntrace info 3\ntrace info 1\ntrace info 2\ntrace info 5\n"), 
            _tracer->getTraceInfo());
    delete _tracer;
}

TEST_F(TracerTest, testMacroRequestTrace) {
    Tracer *_tracer = new Tracer;
    unique_ptr<Tracer> tracerPtr(_tracer);
    _tracer->setLevel("TRACE3");
    REQUEST_TRACE(DEBUG, "TestCase [%d] [%s] [%d] [%s]",
                  1, "hello", 2, "world");
    string traceInfo = _tracer->getTraceInfo();
    ASSERT_TRUE(traceInfo.find("TestCase [1] [hello] [2] [world]") 
                   != string::npos);
}

TEST_F(TracerTest, testEnableMacro) {
    matchdoc::MatchDoc doc = matchdoc::INVALID_MATCHDOC;
    
    matchdoc::Reference<HA3_NS(common)::Tracer>* _traceRefer = NULL;
    ASSERT_TRUE(!RANK_TRACE_LEVEL_ENABLED(DEBUG, doc));
    
    common::Ha3MatchDocAllocator allocator(&_pool);
    matchdoc::Reference<Tracer>* ref = allocator.declare<Tracer>("ref");
    doc = allocator.allocate(100);
    _traceRefer = ref;
    common::Tracer *tracer = _traceRefer->getPointer(doc); 
    tracer->setLevel("DEBUG");

    ASSERT_TRUE(RANK_TRACE_LEVEL_ENABLED(DEBUG, doc));
    ASSERT_TRUE(RANK_TRACE_LEVEL_ENABLED(INFO, doc));
    ASSERT_TRUE(RANK_TRACE_LEVEL_ENABLED(ERROR, doc));

    ASSERT_TRUE(!RANK_TRACE_LEVEL_ENABLED(TRACE3, doc));
    ASSERT_TRUE(!RANK_TRACE_LEVEL_ENABLED(TRACE2, doc));
    ASSERT_TRUE(!RANK_TRACE_LEVEL_ENABLED(TRACE1, doc));

    allocator.deallocate(doc);
}

TEST_F(TracerTest, testKVTracer) {
    Tracer tracer(Tracer::TM_TREE);
    tracer.setLevel("INFO");
    REQUEST_TRACE_WITH_TRACER((&tracer), INFO, "test");
    int testNum = 100;
    REQUEST_TRACE_WITH_TRACER((&tracer), INFO, "num:[%d]", testNum);
    KVTRACE_MODULE_SCOPE_WITH_TRACER((&tracer), "ha3");
    double testDouble = 10.2;
    REQUEST_TRACE_WITH_TRACER((&tracer), INFO, "double:[%.2lf]", testDouble);
        
    KVTRACE_MODULE_SCOPE_WITH_TRACER((&tracer),"rerank");
    REQUEST_TRACE_WITH_TRACER((&tracer), INFO, "test_child_module");
    string expectStr = "{\n"
                       "    \"__arr\": [\n"
                       "        \"test\",\n"
                       "        \"num:[100]\"\n"
                       "    ],\n"
                       "    \"ha3\": {\n"
                       "        \"__arr\": [\n"
                       "            \"double:[10.20]\"\n"
                       "        ],\n"
                       "        \"rerank\": {\n"
                       "            \"__arr\": [\n"
                       "                \"test_child_module\"\n"
                       "            ]\n"
                       "        }\n"
                       "    }\n"
                       "}";
    ASSERT_EQ(expectStr, tracer.getTraceInfo());
}

TEST_F(TracerTest, testMergeKVTrace) {
    Tracer tracer1(Tracer::TM_TREE);
    tracer1.setLevel("INFO");
    tracer1.setPartitionId("simple_searcher_0_32767");
    tracer1.setAddress("10.10.10.10");
    REQUEST_TRACE_WITH_TRACER((&tracer1), INFO, "part_info_1");

    KVTRACE_MODULE_SCOPE_WITH_TRACER((&tracer1), "rank");
    REQUEST_TRACE_WITH_TRACER((&tracer1), INFO, "rank_info_1");

    Tracer tracer2(Tracer::TM_TREE);
    tracer2.setLevel("INFO");
    tracer2.setPartitionId("simple_searcher_32768_65535");
    tracer2.setAddress("10.10.10.11");
    REQUEST_TRACE_WITH_TRACER((&tracer2), INFO, "part_info_2");

    KVTRACE_MODULE_SCOPE_WITH_TRACER((&tracer2), "rank");
    REQUEST_TRACE_WITH_TRACER((&tracer2), INFO, "rank_info_2");

    Tracer tracer3(Tracer::TM_TREE);
    tracer3.setLevel("INFO");

    KVTRACE_MODULE_SCOPE_WITH_TRACER((&tracer3), "qrs");
    REQUEST_TRACE_WITH_TRACER((&tracer3), INFO, "qrs_info");

    tracer3.merge(&tracer1);
    tracer3.merge(&tracer2);

    string expectStr = "{\n"
                       "    \"qrs\": {\n"
                       "        \"__arr\": [\n"
                       "            \"qrs_info\"\n"
                       "        ],\n"
                       "        \"simple_searcher_0_32767[10.10.10.10]\": {\n"
                       "            \"__arr\": [\n"
                       "                \"part_info_1\"\n"
                       "            ],\n"
                       "            \"rank\": {\n"
                       "                \"__arr\": [\n"
                       "                    \"rank_info_1\"\n"
                       "                ]\n"
                       "            }\n"
                       "        },\n"
                       "        \"simple_searcher_32768_65535[10.10.10.11]\": {\n"
                       "            \"__arr\": [\n"
                       "                \"part_info_2\"\n"
                       "            ],\n"
                       "            \"rank\": {\n"
                       "                \"__arr\": [\n"
                       "                    \"rank_info_2\"\n"
                       "                ]\n"
                       "            }\n"
                       "        }\n"
                       "    }\n"
                       "}";
    ASSERT_EQ(expectStr, tracer3.getTraceInfo());
}

TEST_F(TracerTest, testKVTraceSerialize) {
    Tracer tracer(Tracer::TM_TREE);
    tracer.setLevel("INFO");
    REQUEST_TRACE_WITH_TRACER((&tracer), INFO, "test");
    int testNum = 100;
    REQUEST_TRACE_WITH_TRACER((&tracer), INFO, "num:[%d]", testNum);
    KVTRACE_MODULE_SCOPE_WITH_TRACER((&tracer), "ha3");
    double testDouble = 10.2;
    REQUEST_TRACE_WITH_TRACER((&tracer), INFO, "double:[%.2lf]", testDouble);
        
    KVTRACE_MODULE_SCOPE_WITH_TRACER((&tracer),"rerank");
    REQUEST_TRACE_WITH_TRACER((&tracer), INFO, "test_child_module");
    autil::DataBuffer dataBuffer;
    dataBuffer.write(tracer);
    Tracer newTracer;
    dataBuffer.read(newTracer);
    string expectStr = "{\n"
                       "    \"__arr\": [\n"
                       "        \"test\",\n"
                       "        \"num:[100]\"\n"
                       "    ],\n"
                       "    \"ha3\": {\n"
                       "        \"__arr\": [\n"
                       "            \"double:[10.20]\"\n"
                       "        ],\n"
                       "        \"rerank\": {\n"
                       "            \"__arr\": [\n"
                       "                \"test_child_module\"\n"
                       "            ]\n"
                       "        }\n"
                       "    }\n"
                       "}";
    ASSERT_EQ(expectStr, newTracer.getTraceInfo());
}

TEST_F(TracerTest, testLongString) {
    Tracer tracer;
    tracer.setLevel("INFO");
    string longStr(suez::turing::ISEARCH_TRACE_MAX_LEN * 100, 'a');
    REQUEST_TRACE_WITH_TRACER((&tracer), INFO, "%s", longStr.c_str());

    autil::DataBuffer dataBuffer;
    dataBuffer.write(tracer);
    Tracer newTracer;
    dataBuffer.read(newTracer);
    string expectStr(suez::turing::ISEARCH_TRACE_MAX_LEN - 50, 'a');
    string traceinfo = newTracer.getTraceInfo();
    ASSERT_EQ(size_t(suez::turing::ISEARCH_TRACE_MAX_LEN), traceinfo.size());
    ASSERT_TRUE(traceinfo.find(expectStr) != string::npos);
}

END_HA3_NAMESPACE(common);

