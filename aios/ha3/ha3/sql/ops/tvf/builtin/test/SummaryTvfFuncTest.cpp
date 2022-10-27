#include <ha3/sql/ops/test/OpTestBase.h>
#include "ha3/sql/ops/tvf/builtin/SummaryTvfFunc.h"
#include <ha3/sql/data/TableUtil.h>
#include <ha3/sql/ops/tvf/TvfSummaryResource.h>
#include <ha3/config/QueryInfo.h>
#include <autil/StringUtil.h>

using namespace std;
using namespace testing;
using namespace matchdoc;
using namespace autil;

USE_HA3_NAMESPACE(summary);
USE_HA3_NAMESPACE(config);
USE_HA3_NAMESPACE(common);

BEGIN_HA3_NAMESPACE(sql);

class FakeSummaryExtractor : public SummaryExtractor
{
public:
    void extractSummary(common::SummaryHit &summaryHit) override {
        summaryHit.setSummaryValue("id", "10");
        summaryHit.setSummaryValue("x1", "5");
        summaryHit.setSummaryValue("x2", "abc");
        summaryHit.setSummaryValue("x3", "1.1");
    }

    SummaryExtractor* clone() override {
        return new FakeSummaryExtractor(*this);
    }
    void destory() override {
        delete this;
    }
};

class SummaryTvfFuncTest : public OpTestBase {
public:
    ~SummaryTvfFuncTest() {
    }
public:
    void setUp();
    void tearDown();
private:
    void prepareContext(const std::vector<std::string> &params = {"", ""}) {
        _context.params = params;
        _context.queryPool = &_pool;
        _context.outputFields = {"id", "a", "b", "c",
                                 "x1", "x2", "x3"};
        _context.outputFieldsType = {"INTEGER", "BIGINT", "VARCHAR", "ARRAY",
                                      "INTEGER", "VARCHAR", "DOUBLE"};
        _tvfResourceContainer.reset(new TvfResourceContainer());
        _context.tvfResourceContainer = _tvfResourceContainer.get();
        _context.queryResource = _sqlQueryResource.get();
        _queryInfoPtr.reset(new HA3_NS(config)::QueryInfo("phrase"));
        _context.queryInfo = _queryInfoPtr.get();

        SummaryProfileInfo summaryProfileInfo;
        summaryProfileInfo._summaryProfileName = "DefaultProfile";
        _hitSummarySchema.reset(new HitSummarySchema());
        SummaryProfile *summaryProfile(
                new SummaryProfile(summaryProfileInfo, _hitSummarySchema.get()));
        summaryProfile->_summaryExtractorChain = new SummaryExtractorChain;
        summaryProfile->_summaryExtractorChain->addSummaryExtractor(
                new FakeSummaryExtractor());
        _summaryProfileManager.reset(new SummaryProfileManager({}));
        _summaryProfileManager->addSummaryProfile(summaryProfile);
        ASSERT_TRUE(_context.tvfResourceContainer->put(
                        new TvfSummaryResource(_summaryProfileManager)));
    }
    void prepareTable() {
        vector<MatchDoc> docs = getMatchDocs(_allocator, 4);
        ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator<uint32_t>(
                        _allocator, docs, "id", {1, 2, 3, 4}));
        ASSERT_NO_FATAL_FAILURE(extendMultiValueMatchDocAllocator<int64_t>(
                        _allocator, docs, "a", {{5}, {6, 1}, {}, {8, 3}}));
        ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator(
                        _allocator, docs, "b", {"b1", "b2", "b3", "b4"}));
        ASSERT_NO_FATAL_FAILURE(extendMultiValueMatchDocAllocator<string>(
                        _allocator, docs, "c", {{"c", "a1"}, {"a", "", "b"}, {}, {"", ""}}));
        _table.reset(new Table(docs, _allocator));
    }

private:
    HA3_NS(config)::QueryInfoPtr _queryInfoPtr;
    TvfFuncInitContext _context;
    TvfResourceContainerPtr _tvfResourceContainer;
    SummaryProfileManagerPtr _summaryProfileManager;
    HitSummarySchemaPtr _hitSummarySchema;
    MatchDocAllocatorPtr _allocator;
    TablePtr _table;
private:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(builtin, SummaryTvfFuncTest);

void SummaryTvfFuncTest::setUp() {
}

void SummaryTvfFuncTest::tearDown() {
}

TEST_F(SummaryTvfFuncTest, testInit) {
    prepareContext();
    SummaryTvfFunc summaryTvfFunc;
    summaryTvfFunc.setName("DefaultProfile");
    ASSERT_TRUE(summaryTvfFunc.init(_context));
}

TEST_F(SummaryTvfFuncTest, testConstruct) {
    SummaryTvfFunc summaryTvfFunc;
    ASSERT_FALSE(summaryTvfFunc._summaryExtractorChain);
    ASSERT_FALSE(summaryTvfFunc._summaryExtractorProvider);
    ASSERT_FALSE(summaryTvfFunc._summaryProfile);
    ASSERT_FALSE(summaryTvfFunc._summaryQueryInfo);
    ASSERT_FALSE(summaryTvfFunc._resource);
}

TEST_F(SummaryTvfFuncTest, testPrepareResourceEmptyParams) {
    prepareContext();
    SummaryTvfFunc summaryTvfFunc;
    ASSERT_TRUE(summaryTvfFunc.prepareResource(_context));
}

TEST_F(SummaryTvfFuncTest, testPrepareResourceErrorParamsSize) {
    navi::NaviLoggerProvider provider;
    prepareContext({"", "", ""});
    SummaryTvfFunc summaryTvfFunc;
    ASSERT_FALSE(summaryTvfFunc.prepareResource(_context));
    ASSERT_EQ("summary tvf need 2 params", provider.getErrorMessage());
}

TEST_F(SummaryTvfFuncTest, testPrepareResourceParseKvPairFailed) {
    navi::NaviLoggerProvider provider;
    prepareContext({"aa", ""});
    SummaryTvfFunc summaryTvfFunc;
    ASSERT_FALSE(summaryTvfFunc.prepareResource(_context));
    ASSERT_EQ("parse kvpair failed, error msg [Parse kvpair clause failed, " \
              "Invalid kvpairs: [aa]]",
              provider.getErrorMessage());
}

TEST_F(SummaryTvfFuncTest, testPrepareResourceParseQueryFailed) {
    navi::NaviLoggerProvider provider;
    prepareContext({"aa:bb", "OR OR"});
    SummaryTvfFunc summaryTvfFunc;
    ASSERT_FALSE(summaryTvfFunc.prepareResource(_context));
    ASSERT_EQ("parse query failed, error msg [Parse query failed! error description: " \
              "syntax error, unexpected OR, queryClauseStr:OR OR]",
              provider.getErrorMessage());
}

TEST_F(SummaryTvfFuncTest, testPrepareResourceSuccess) {
    navi::NaviLoggerProvider provider;
    prepareContext({"aa:bb, cc:dd", "xx AND yy:1"});
    SummaryTvfFunc summaryTvfFunc;
    ASSERT_TRUE(summaryTvfFunc.prepareResource(_context));
    auto request = summaryTvfFunc._request;
    EXPECT_EQ("bb", request->getConfigClause()->getKVPairValue("aa"));
    EXPECT_EQ("dd", request->getConfigClause()->getKVPairValue("cc"));
    EXPECT_EQ("AndQuery:[TermQuery:[Term:[phrase||xx|100|]], " \
              "TermQuery:[Term:[yy||1|100|]], ]|",
              request->getQueryClause()->toString());
    EXPECT_EQ("xx AND yy:1", request->getDocIdClause()->getQueryString());
    auto termVec = request->getDocIdClause()->getTermVector();
    EXPECT_EQ(2, termVec.size());
    EXPECT_EQ("Term:[phrase||xx|100|]", termVec[0].toString());
    EXPECT_EQ("Term:[yy||1|100|]", termVec[1].toString());
    EXPECT_TRUE(summaryTvfFunc._summaryQueryInfo);
    EXPECT_TRUE(summaryTvfFunc._resource);
}

TEST_F(SummaryTvfFuncTest, testToSummarySchema) {
    navi::NaviLoggerProvider provider;
    SummaryTvfFunc summaryTvfFunc;
    prepareTable();
    summaryTvfFunc.toSummarySchema(_table);
    HitSummarySchema *hitSummarySchema = summaryTvfFunc._hitSummarySchema.get();
    ASSERT_TRUE(hitSummarySchema);
    EXPECT_EQ(4, hitSummarySchema->getFieldCount());

    const SummaryFieldInfo *fieldInfo = hitSummarySchema->getSummaryFieldInfo(0);
    ASSERT_TRUE(fieldInfo);
    EXPECT_EQ(false, fieldInfo->isMultiValue);
    EXPECT_EQ(ft_uint32, fieldInfo->fieldType);
    EXPECT_EQ("id", fieldInfo->fieldName);

    fieldInfo = hitSummarySchema->getSummaryFieldInfo(1);
    ASSERT_TRUE(fieldInfo);
    EXPECT_EQ(true, fieldInfo->isMultiValue);
    EXPECT_EQ(ft_int64, fieldInfo->fieldType);
    EXPECT_EQ("a", fieldInfo->fieldName);

    fieldInfo = hitSummarySchema->getSummaryFieldInfo(2);
    ASSERT_TRUE(fieldInfo);
    EXPECT_EQ(false, fieldInfo->isMultiValue);
    EXPECT_EQ(ft_string, fieldInfo->fieldType);
    EXPECT_EQ("b", fieldInfo->fieldName);

    fieldInfo = hitSummarySchema->getSummaryFieldInfo(3);
    ASSERT_TRUE(fieldInfo);
    EXPECT_EQ(true, fieldInfo->isMultiValue);
    EXPECT_EQ(ft_string, fieldInfo->fieldType);
    EXPECT_EQ("c", fieldInfo->fieldName);
}

TEST_F(SummaryTvfFuncTest, testToSummaryHits) {
    navi::NaviLoggerProvider provider;
    SummaryTvfFunc summaryTvfFunc;
    prepareTable();
    vector<SummaryHit *> summaryHits;
    summaryTvfFunc.toSummarySchema(_table);
    summaryTvfFunc.toSummaryHits(_table, summaryHits);
    EXPECT_EQ(4, summaryHits.size());
    auto summaryDoc = summaryHits[0]->getSummaryDocument();
    EXPECT_EQ("1", summaryDoc->GetFieldValue(0)->toString());
    EXPECT_EQ("5", summaryDoc->GetFieldValue(1)->toString());
    EXPECT_EQ("b1", summaryDoc->GetFieldValue(2)->toString());
    EXPECT_EQ("ca1", summaryDoc->GetFieldValue(3)->toString());

    summaryDoc = summaryHits[1]->getSummaryDocument();
    EXPECT_EQ("2", summaryDoc->GetFieldValue(0)->toString());
    EXPECT_EQ("61", summaryDoc->GetFieldValue(1)->toString());
    EXPECT_EQ("b2", summaryDoc->GetFieldValue(2)->toString());
    EXPECT_EQ("ab", summaryDoc->GetFieldValue(3)->toString());

    summaryDoc = summaryHits[2]->getSummaryDocument();
    EXPECT_EQ("3", summaryDoc->GetFieldValue(0)->toString());
    EXPECT_EQ("", summaryDoc->GetFieldValue(1)->toString());
    EXPECT_EQ("b3", summaryDoc->GetFieldValue(2)->toString());
    EXPECT_EQ("", summaryDoc->GetFieldValue(3)->toString());

    summaryDoc = summaryHits[3]->getSummaryDocument();
    EXPECT_EQ("4", summaryDoc->GetFieldValue(0)->toString());
    EXPECT_EQ("83", summaryDoc->GetFieldValue(1)->toString());
    EXPECT_EQ("b4", summaryDoc->GetFieldValue(2)->toString());
    EXPECT_EQ("", summaryDoc->GetFieldValue(3)->toString());
    for (auto summaryHit : summaryHits) {
        DELETE_AND_SET_NULL(summaryHit);
    }
}

TEST_F(SummaryTvfFuncTest, testFromSummaryHits) {
    navi::NaviLoggerProvider provider;
    SummaryTvfFunc summaryTvfFunc;
    summaryTvfFunc._queryPool = &_pool;
    prepareTable();
    vector<SummaryHit *> summaryHits;
    summaryTvfFunc.toSummarySchema(_table);
    summaryTvfFunc.toSummaryHits(_table, summaryHits);
    for (size_t i = 0; i < summaryHits.size(); ++i) {
        summaryHits[i]->setSummaryValue(
                "id", StringUtil::toString(i + 10));
        summaryHits[i]->setSummaryValue(
                "b", "c" + StringUtil::toString(i + 1));
        summaryHits[i]->setSummaryValue(
                "x1", StringUtil::toString(i));
        summaryHits[i]->setSummaryValue(
                "x2", "aa_" + StringUtil::toString(i));
        summaryHits[i]->setSummaryValue(
                "x3", "1." + StringUtil::toString(i));
    }
    summaryTvfFunc._outputFields = {"id", "a", "b", "c",
                                    "x1", "x2", "x3"};
    summaryTvfFunc._outputFieldsType = {"INTEGER", "BIGINT", "VARCHAR", "ARRAY",
                                        "INTEGER", "VARCHAR", "DOUBLE"};
    ASSERT_TRUE(summaryTvfFunc.fromSummaryHits(_table, summaryHits));
    ASSERT_EQ(4, _table->getRowCount());
    ASSERT_EQ(7, _table->getColumnCount());
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<uint32_t>(_table, "id", {10, 11, 12, 13}));
    ASSERT_NO_FATAL_FAILURE(
            checkOutputMultiColumn<int64_t>(_table, "a", {{5}, {6, 1}, {}, {8, 3}}));
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn(_table, "b", {"c1", "c2", "c3", "c4"}));
    ASSERT_NO_FATAL_FAILURE(
            checkOutputMultiColumn(_table, "c", {{"c", "a1"}, {"a", "", "b"}, {}, {"", ""}}));
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<int32_t>(_table, "x1", {0, 1, 2, 3}));
    ASSERT_NO_FATAL_FAILURE(
            checkOutputColumn(_table, "x2", {"aa_0", "aa_1", "aa_2", "aa_3"}));
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<double>(_table, "x3", {1.0, 1.1, 1.2, 1.3}));
    for (auto summaryHit : summaryHits) {
        DELETE_AND_SET_NULL(summaryHit);
    }
}

TEST_F(SummaryTvfFuncTest, testFromSummaryHitsDeclareColFailed) {
    navi::NaviLoggerProvider provider;
    SummaryTvfFunc summaryTvfFunc;
    summaryTvfFunc._queryPool = &_pool;
    prepareTable();
    vector<SummaryHit *> summaryHits;
    summaryTvfFunc._outputFields = {"id", "a", "b", "c",
                                    "x1", "x2", "x3"};
    summaryTvfFunc._outputFieldsType = {"INTEGER", "BIGINT", "VARCHAR", "ARRAY",
                                        "INTEGER", "VARCHAR", "UINT32"};
    ASSERT_FALSE(summaryTvfFunc.fromSummaryHits(_table, summaryHits));
    ASSERT_EQ("declare column [x3] failed", provider.getErrorMessage());
}

TEST_F(SummaryTvfFuncTest, testFromSummaryHitsGetSummaryFieldFailed) {
    navi::NaviLoggerProvider provider;
    SummaryTvfFunc summaryTvfFunc;
    summaryTvfFunc._queryPool = &_pool;
    prepareTable();
    vector<SummaryHit *> summaryHits;
    summaryTvfFunc.toSummarySchema(_table);
    summaryTvfFunc.toSummaryHits(_table, summaryHits);
    for (size_t i = 0; i < summaryHits.size(); ++i) {
        summaryHits[i]->setSummaryValue(
                "id", StringUtil::toString(i + 10));
        summaryHits[i]->setSummaryValue(
                "b", "c" + StringUtil::toString(i + 1));
        summaryHits[i]->setSummaryValue(
                "x1", StringUtil::toString(i));
        summaryHits[i]->setSummaryValue(
                "x2", "aa_" + StringUtil::toString(i));
    }
    summaryTvfFunc._outputFields = {"id", "a", "b", "c",
                                    "x1", "x2", "x3"};
    summaryTvfFunc._outputFieldsType = {"INTEGER", "BIGINT", "VARCHAR", "ARRAY",
                                        "INTEGER", "VARCHAR", "DOUBLE"};
    ASSERT_FALSE(summaryTvfFunc.fromSummaryHits(_table, summaryHits));
    for (auto summaryHit : summaryHits) {
        DELETE_AND_SET_NULL(summaryHit);
    }
}

TEST_F(SummaryTvfFuncTest, testFromSummaryHitsFillSummaryDocFailed) {
    navi::NaviLoggerProvider provider;
    SummaryTvfFunc summaryTvfFunc;
    summaryTvfFunc._queryPool = &_pool;
    prepareTable();
    vector<SummaryHit *> summaryHits;
    summaryTvfFunc.toSummarySchema(_table);
    summaryTvfFunc.toSummaryHits(_table, summaryHits);
    for (size_t i = 0; i < summaryHits.size(); ++i) {
        summaryHits[i]->setSummaryValue(
                "id", StringUtil::toString(i + 10));
        summaryHits[i]->setSummaryValue(
                "b", "c" + StringUtil::toString(i + 1));
        summaryHits[i]->setSummaryValue(
                "x2", "aa_" + StringUtil::toString(i));
    }
    summaryTvfFunc._outputFields = {"id", "a", "b", "c", "x2"};
    summaryTvfFunc._outputFieldsType = {"INTEGER", "BIGINT", "VARCHAR", "ARRAY", "BIGINT"};
    ASSERT_TRUE(summaryTvfFunc.fromSummaryHits(_table, summaryHits));
    ASSERT_EQ(4, _table->getRowCount());
    ASSERT_EQ(5, _table->getColumnCount());
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<uint32_t>(_table, "id", {10, 11, 12, 13}));
    ASSERT_NO_FATAL_FAILURE(
            checkOutputMultiColumn<int64_t>(_table, "a", {{5}, {6, 1}, {}, {8, 3}}));
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn(_table, "b", {"c1", "c2", "c3", "c4"}));
    ASSERT_NO_FATAL_FAILURE(
            checkOutputMultiColumn(_table, "c", {{"c", "a1"}, {"a", "", "b"}, {}, {"", ""}}));
    ASSERT_NO_FATAL_FAILURE(
            checkOutputColumn<int64_t>(_table, "x2", {0, 0, 0, 0}));
    for (auto summaryHit : summaryHits) {
        DELETE_AND_SET_NULL(summaryHit);
    }
}

TEST_F(SummaryTvfFuncTest, testFromSummaryHitsLackCol) {
    navi::NaviLoggerProvider provider;
    SummaryTvfFunc summaryTvfFunc;
    summaryTvfFunc._queryPool = &_pool;
    prepareTable();
    vector<SummaryHit *> summaryHits;
    summaryTvfFunc.toSummarySchema(_table);
    summaryTvfFunc.toSummaryHits(_table, summaryHits);
    for (size_t i = 0; i < 2; ++i) {
        summaryHits[i]->setSummaryValue(
                "id", StringUtil::toString(i + 10));
        summaryHits[i]->setSummaryValue(
                "b", "c" + StringUtil::toString(i + 1));
        summaryHits[i]->setSummaryValue(
                "x1", StringUtil::toString(i));
        summaryHits[i]->setSummaryValue(
                "x2", "aa_" + StringUtil::toString(i));
        summaryHits[i]->setSummaryValue(
                "x3", "1." + StringUtil::toString(i));
    }
    summaryTvfFunc._outputFields = {"id", "a", "b", "c",
                                    "x1", "x2", "x3"};
    summaryTvfFunc._outputFieldsType = {"INTEGER", "BIGINT", "VARCHAR", "ARRAY",
                                        "INTEGER", "VARCHAR", "DOUBLE"};
    ASSERT_TRUE(summaryTvfFunc.fromSummaryHits(_table, summaryHits));
    ASSERT_EQ(4, _table->getRowCount());
    ASSERT_EQ(7, _table->getColumnCount());
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<uint32_t>(_table, "id", {10, 11, 3, 4}));
    ASSERT_NO_FATAL_FAILURE(
            checkOutputMultiColumn<int64_t>(_table, "a", {{5}, {6, 1}, {}, {8, 3}}));
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn(_table, "b", {"c1", "c2", "b3", "b4"}));
    ASSERT_NO_FATAL_FAILURE(
            checkOutputMultiColumn(_table, "c", {{"c", "a1"}, {"a", "", "b"}, {}, {"", ""}}));
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<int32_t>(_table, "x1", {0, 1, 0, 0}));
    ASSERT_NO_FATAL_FAILURE(
            checkOutputColumn(_table, "x2", {"aa_0", "aa_1", "", ""}));
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<double>(_table, "x3", {1.0, 1.1, 0, 0}));
    for (auto summaryHit : summaryHits) {
        DELETE_AND_SET_NULL(summaryHit);
    }
}

TEST_F(SummaryTvfFuncTest, testSummaryTvfFunc) {
    navi::NaviLoggerProvider provider;
    SummaryTvfFunc summaryTvfFunc;
    TablePtr output;
    prepareTable();
    prepareContext();
    summaryTvfFunc.setName("DefaultProfile");
    ASSERT_TRUE(summaryTvfFunc.init(_context));
    ASSERT_TRUE(summaryTvfFunc.compute(_table, true, output));
    ASSERT_EQ(4, output->getRowCount());
    ASSERT_EQ(7, output->getColumnCount());
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<uint32_t>(output, "id", {10, 10, 10, 10}));
    ASSERT_NO_FATAL_FAILURE(
            checkOutputMultiColumn<int64_t>(output, "a", {{5}, {6, 1}, {}, {8, 3}}));
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn(output, "b", {"b1", "b2", "b3", "b4"}));
    ASSERT_NO_FATAL_FAILURE(
            checkOutputMultiColumn(output, "c", {{"c", "a1"}, {"a", "", "b"}, {}, {"", ""}}));
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<int32_t>(output, "x1", {5, 5, 5, 5}));
    ASSERT_NO_FATAL_FAILURE(
            checkOutputColumn(output, "x2", {"abc", "abc", "abc", "abc"}));
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<double>(output, "x3", {1.1, 1.1, 1.1, 1.1}));

    prepareTable();
    _table->clearBackRows(1);
    ASSERT_TRUE(summaryTvfFunc.compute(_table, true, output));
    ASSERT_EQ(3, output->getRowCount());
    ASSERT_EQ(7, output->getColumnCount());
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<uint32_t>(output, "id", {10, 10, 10}));
    ASSERT_NO_FATAL_FAILURE(
            checkOutputMultiColumn<int64_t>(output, "a", {{5}, {6, 1}, {}}));
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn(output, "b", {"b1", "b2", "b3"}));
    ASSERT_NO_FATAL_FAILURE(
            checkOutputMultiColumn(output, "c", {{"c", "a1"}, {"a", "", "b"}, {}}));
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<int32_t>(output, "x1", {5, 5, 5}));
    ASSERT_NO_FATAL_FAILURE(
            checkOutputColumn(output, "x2", {"abc", "abc", "abc"}));
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<double>(output, "x3", {1.1, 1.1, 1.1}));
}

END_HA3_NAMESPACE();
