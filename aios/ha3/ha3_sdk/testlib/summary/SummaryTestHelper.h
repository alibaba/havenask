#ifndef ISEARCH_SUMMARYTESTHELPER_H
#define ISEARCH_SUMMARYTESTHELPER_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3_sdk/testlib/index/FakeIndexPartitionReaderCreator.h>
#include <ha3/summary/SummaryExtractor.h>
#include <ha3/common/Ha3MatchDocAllocator.h>
#include <ha3/common/Request.h>
#include <suez/turing/common/SuezCavaAllocator.h>
#include <suez/turing/expression/util/TableInfoConfigurator.h>
#include <indexlib/testlib/indexlib_partition_creator.h>
#include <indexlib/partition/index_application.h>
#include <suez/turing/test/IndexWrap.h>


BEGIN_HA3_NAMESPACE(summary);

struct SummaryTestParam {
    index::FakeIndex fakeIndex;
    std::string requestStr;

    // format of summary schema:
    //   fieldName:fieldType:isMulti;fieldName:fieldType:isMulti;...
    std::string summarySchemaStr;

    // format of fieldSummaryConfigStr:
    //fieldName:max_snippet,max_summary_length,highlight_prefix,highlight_suffix,snippet_delimiter;
    //fieldName:max_snippet,max_summary_length,highlight_prefix,highlight_suffix,snippet_delimiter;
    std::string fieldSummaryConfigStr;

    // format of docIdStr:
    //   docId1,docId2,...
    std::string docIdStr;

    // format of termStr:
    //   term1,term2,...
    std::string termStr;

    // query string
    std::string queryStr;

    // format:
    //  field1,field2,...,fieldN;
    //  field1,field2,...,fieldN;
    std::string expectedSummaryStr;
};

class SummaryTestHelper
{
public:
    SummaryTestHelper(const std::string &testPath);
    ~SummaryTestHelper();
private:
    SummaryTestHelper(const SummaryTestHelper &);
    SummaryTestHelper& operator=(const SummaryTestHelper &);
public:
    bool init(SummaryTestParam *param);

    bool beginRequest(SummaryExtractor *extractor);
    bool extractSummarys(SummaryExtractor *extractor);
private:
    bool createProvider();
    FieldType strToFieldType(const std::string &fieldStr);
    bool createFieldSummaryConfig(const std::string &configStr);
    bool createHitSummarySchema(const std::string &schemaStr);
    bool createSummaryQueryInfo();
    bool createAttrExprCreator();
    bool createTracer();
    bool checkResult(const std::vector<common::SummaryHit*> &hits);
    bool fillSummary(const std::vector<matchdoc::MatchDoc> &matchDocs,
                     const std::vector<common::SummaryHit*> &hits);
private:
    std::string _testPath;
    SummaryTestParam *_param;
    autil::mem_pool::Pool *_pool;
    common::Ha3MatchDocAllocator *_mdAllocator;
    common::RequestPtr _requestPtr;
    config::HitSummarySchema *_hitSummarySchema;
    config::FieldSummaryConfigVec _fieldSummaryConfigVec;
    SummaryQueryInfo *_queryInfo;
    suez::turing::AttributeExpressionCreator *_attrExprCreator;
    // provider
    SummaryExtractorProvider *_provider;
    common::Tracer *_tracer;
    suez::turing::SuezCavaAllocator *_ha3CavaAllocator;
    CavaCtx *_cavaCtx;
    search::SearchCommonResource* _resource;
    suez::turing::InvertedTableParam _invertedTableParam;
    suez::turing::IndexWrapPtr _indexWrap;
    IE_NAMESPACE(partition)::PartitionReaderSnapshotPtr _snapshotPtr;
    suez::turing::TableInfoPtr _tableInfoPtr;
private:
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(SummaryTestHelper);

END_HA3_NAMESPACE(summary);

#endif //ISEARCH_SUMMARYTESTHELPER_H
