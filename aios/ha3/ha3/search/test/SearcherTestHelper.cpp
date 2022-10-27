#include <ha3/search/test/SearcherTestHelper.h>
#include <autil/StringUtil.h>
#include <autil/StringTokenizer.h>
#include <ha3/queryparser/QueryParser.h>
#include <ha3/qrs/QueryFlatten.h>
#include <ha3/qrs/QueryTokenizer.h>
#include <ha3/test/test.h>
#include <ha3_sdk/testlib/index/FakeTextIndexReader.h>
#include <ha3_sdk/testlib/index/FakeBitmapIndexReader.h>
#include <ha3_sdk/testlib/index/FakeIndexReader.h>
#include <ha3_sdk/testlib/index/FakePostingMaker.h>
#include <ha3/common/QueryTermVisitor.h>
#include <suez/turing/common/FileUtil.h>

using namespace std;
using namespace autil;
using namespace suez::turing;
IE_NAMESPACE_USE(index);

using namespace build_service::analyzer;
using namespace build_service::config;

USE_HA3_NAMESPACE(common);
USE_HA3_NAMESPACE(util);
USE_HA3_NAMESPACE(config);
USE_HA3_NAMESPACE(queryparser);
USE_HA3_NAMESPACE(qrs);

BEGIN_HA3_NAMESPACE(search);
HA3_LOG_SETUP(search, SearcherTestHelper);

static const string CONFIG_FILE = TEST_DATA_PATH"/test_analyzer.json";
static const string ALITOKENIZER_CONFIG_FILE = TEST_DATA_PATH"/AliTokenizer.conf";


SearcherTestHelper::SearcherTestHelper() { 
}

SearcherTestHelper::~SearcherTestHelper() { 
}

TermDFMap SearcherTestHelper::createTermDFMap(const string &str) {
    TermDFMap termDFMap;
    StringTokenizer st1(str, ",", StringTokenizer::TOKEN_IGNORE_EMPTY
                        | StringTokenizer::TOKEN_TRIM);
    for (size_t i = 0; i < st1.getNumTokens(); ++i) {
        StringTokenizer st2(st1[i], ":", StringTokenizer::TOKEN_IGNORE_EMPTY
                            | StringTokenizer::TOKEN_TRIM);
        string indexName = "default";
        size_t pos = 0;
        if (st2.getNumTokens() == 3) {
            indexName = st2[pos++];
        }
        Term term(st2[pos++].c_str(), indexName.c_str(), RequiredFields());
        df_t df = StringUtil::fromString<df_t>(st2[pos++].c_str());
        termDFMap[term] = df;
    }
    return termDFMap;
}

Query *SearcherTestHelper::createQuery(const string &queryStr, uint32_t pos,
                                       const string &defaultIndexName,
                                       bool needAnalyzer)
{
    vector<Query*> querys = createQuerys(queryStr, defaultIndexName, needAnalyzer);
    if(querys.size() > 0 ){
        return querys[pos];
    }else{
        return NULL;
    }
}

vector<Query*> SearcherTestHelper::createQuerys(const string &queryStr,
        const string &defaultIndexName, bool needAnalyzer)
{
    QueryParser parser(defaultIndexName.c_str(), OP_AND, true);
    ParserContext *context = parser.parse(queryStr.c_str());
    vector<Query*> querys = context->stealQuerys();
    delete context;
    if (!needAnalyzer) {
        return querys;
    }
    string configPath = TEST_DATA_PATH"/analyzer_common";
    ResourceReaderPtr resoureReaderPtr(new ResourceReader(configPath));
    AnalyzerFactoryPtr factory =  AnalyzerFactory::create(resoureReaderPtr);
    assert(factory);
    QueryTokenizer tokenizer(factory.get());
    ConfigClause configClause;
    configClause.setAnalyzerName("simple_analyzer");
    tokenizer.setConfigClause(&configClause);
    IndexInfos indexInfos;
    QueryTermVisitor termVisitor(QueryTermVisitor::VT_ALL);
    for (size_t i = 0; i < querys.size(); ++i) {
        querys[i]->accept(&termVisitor);
    }
    const TermVector& termVec = termVisitor.getTermVector();
    set<string> indexNames;
    for (size_t i = 0; i < termVec.size(); ++i) {
        indexNames.insert(termVec[i].getIndexName());
    }
    set<string>::const_iterator iter = indexNames.begin();
    for (; iter != indexNames.end(); ++iter) {
        IndexInfo *indexInfo = new IndexInfo;
        indexInfo->indexName = *iter;
        indexInfo->indexType = it_pack;
        indexInfos.addIndexInfo(indexInfo);
    }

    QueryFlatten flatten;
    for (size_t i = 0; i < querys.size(); ++i) {
        tokenizer.tokenizeQuery(querys[i], &indexInfos);
        delete querys[i];
        querys[i] = tokenizer.stealQuery();

        querys[i]->accept(&flatten);
        delete querys[i];
        querys[i] = flatten.stealQuery();
    }

    return querys;
}

vector<docid_t> SearcherTestHelper::covertToResultDocIds(const string &docIdStr) {
    vector<docid_t> results;
    StringTokenizer st(docIdStr, ",", StringTokenizer::TOKEN_IGNORE_EMPTY
                       | StringTokenizer::TOKEN_TRIM);
    for (size_t i = 0; i < st.getNumTokens(); ++i) {
        StringTokenizer st1(st[i], "-", StringTokenizer::TOKEN_IGNORE_EMPTY
                            | StringTokenizer::TOKEN_TRIM);
        if (st1.getNumTokens() == 1) {
            results.push_back(StringUtil::fromString<docid_t>(st1[0].c_str()));
        } else {
            assert (st1.getNumTokens() == 2);
            docid_t end = autil::StringUtil::fromString<docid_t>(st1[0].c_str());
            docid_t begin = autil::StringUtil::fromString<docid_t>(st1[1].c_str());
            for (docid_t docId = end; docId >= begin; --docId) {
                results.push_back(docId);
            }
        }
    }
    return results;
}

vector<vector<matchvalue_t> > SearcherTestHelper::convertToMatchValues(
        const string &matchValueStr) {
    vector<vector<matchvalue_t> > results;
    StringTokenizer st(matchValueStr, ";", StringTokenizer::TOKEN_IGNORE_EMPTY
                       | StringTokenizer::TOKEN_TRIM);
    for (size_t i = 0; i < st.getNumTokens(); ++i) {
        vector<matchvalue_t> oneDocMatchValues;
        StringTokenizer st2(st[i], ",", StringTokenizer::TOKEN_IGNORE_EMPTY
                           | StringTokenizer::TOKEN_TRIM);
        for (size_t j = 0; j < st2.getNumTokens(); ++j) {
            matchvalue_t mvt;
            mvt.SetUint16(autil::StringUtil::fromString<uint16_t>(st2[j]));
            oneDocMatchValues.push_back(mvt);
        }
        results.push_back(oneDocMatchValues);
    }
    return results;
}

void SearcherTestHelper::checkAggregatorResult(const common::AggregateResults &aggResults, const string &resultStr) {
    StringTokenizer st(resultStr, "\n", StringTokenizer::TOKEN_IGNORE_EMPTY
                       | StringTokenizer::TOKEN_TRIM);
    assert(aggResults.size() == (size_t)st.getNumTokens());
    for (size_t i = 0; i < aggResults.size(); ++i) {
        AggregateResultPtr aggResultPtr = aggResults[i];
        aggResultPtr->constructGroupValueMap();
        StringTokenizer st1(st[i], ";", StringTokenizer::TOKEN_IGNORE_EMPTY
                            | StringTokenizer::TOKEN_TRIM);
        assert((uint32_t)st1.getNumTokens() == aggResultPtr->getAggExprValueCount());
        for (size_t j = 0; j < st1.getNumTokens(); ++j) {
            size_t pos = st1[j].find(":");
            string groupKey = st1[j].substr(0, pos);
            StringTokenizer st2(st1[j].substr(pos + 1), ",", StringTokenizer::TOKEN_IGNORE_EMPTY
                    | StringTokenizer::TOKEN_TRIM);
            assert(uint32_t(st2.getNumTokens()) == aggResultPtr->getAggFunCount());
            const AggregateResult::AggExprValueMap &valueMap = aggResultPtr->getAggExprValueMap();
            AggregateResult::AggExprValueMap::const_iterator it = valueMap.find(groupKey);
            assert(it != valueMap.end());
            for (size_t k = 0; k < st2.getNumTokens(); ++k) {
                const matchdoc::ReferenceBase *ref = aggResultPtr->getAggFunResultReferBase(k);
                string value = ref->toString(it->second);
                assert(st2[k] == value);
            }
        }
    }
}

END_HA3_NAMESPACE(search);

