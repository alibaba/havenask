#ifndef ISEARCH_HA3SCANCONDITIONVISITOR_H
#define ISEARCH_HA3SCANCONDITIONVISITOR_H

#include <ha3/sql/common/common.h>
#include <ha3/sql/ops/condition/ConditionVisitor.h>
#include <ha3/common/Query.h>
#include <ha3/config/QueryInfo.h>
#include <ha3/search/IndexPartitionReaderWrapper.h>
#include <ha3/search/LayerMetas.h>
#include <ha3/search/QueryExecutor.h>
#include <ha3/queryparser/ParserContext.h>
#include <suez/turing/expression/syntax/SyntaxExpr.h>
#include <autil/legacy/RapidJsonCommon.h>
#include <build_service/analyzer/AnalyzerFactory.h>
#include <suez/turing/expression/util/IndexInfos.h>
#include <suez/turing/expression/framework/AttributeExpressionCreator.h>
#include <suez/turing/common/TimeoutTerminator.h>
#include <ha3/sql/attr/IndexInfo.h>

BEGIN_HA3_NAMESPACE(sql);
struct Ha3ScanConditionVisitorParam {
    Ha3ScanConditionVisitorParam ()
        : analyzerFactory(NULL)
        , pool(NULL)
        , queryInfo(NULL)
        , indexInfos(NULL)
        , timeoutTerminator(NULL)
        , layerMeta(NULL)
    {}
    suez::turing::AttributeExpressionCreatorPtr attrExprCreator;
    search::IndexPartitionReaderWrapperPtr indexPartitionReaderWrapper;
    const build_service::analyzer::AnalyzerFactory *analyzerFactory;
    const std::map<std::string, IndexInfo> *indexInfo;
    autil::mem_pool::Pool *pool;
    const config::QueryInfo *queryInfo;
    const suez::turing::IndexInfos *indexInfos;
    std::string mainTableName;
    suez::turing::TimeoutTerminator *timeoutTerminator;
    search::LayerMeta *layerMeta;
};

class Ha3ScanConditionVisitor : public ConditionVisitor
{
public:
    Ha3ScanConditionVisitor(const Ha3ScanConditionVisitorParam &param);
    ~Ha3ScanConditionVisitor();
public:
    void visitAndCondition(AndCondition *condition) override;
    void visitOrCondition(OrCondition *condition) override;
    void visitNotCondition(NotCondition *condition) override;
    void visitLeafCondition(LeafCondition *condition) override;
public:
    common::Query *stealQuery();
    suez::turing::AttributeExpression *stealAttributeExpression();
    const common::Query *getQuery() const;
    const suez::turing::AttributeExpression *getAttributeExpression() const;
private:
    common::Query *toTermQuery(const autil_rapidjson::SimpleValue &condition);
    common::Query *toMatchQuery(const autil_rapidjson::SimpleValue &condition);
    common::Query *toQuery(const autil_rapidjson::SimpleValue &condition);
    common::Query *udfToQuery(const autil_rapidjson::SimpleValue &condition);
    common::Query *spToQuery(const autil_rapidjson::SimpleValue &condition);

    common::Query *createTermQuery(const autil_rapidjson::SimpleValue &left,
                                   const autil_rapidjson::SimpleValue &right,
                                   const std::string &op);
    common::Query *doCreateTermQuery(const autil_rapidjson::SimpleValue &attr,
                                   const autil_rapidjson::SimpleValue &value,
                                   const std::string &op);
    common::Query *tokenizeQuery(common::Query*query, QueryOperator qp,
                                 const std::set<std::string> &noTokenIndexes,
                                 const std::string &globalAnalyzer,
                                 const std::map<std::string, std::string> &indexAnalyzerMap);
    common::Query *postProcessQuery(bool tokenQuery,
                                    bool removeStopWords,
                                    QueryOperator op,
                                    const std::string &queryStr,
                                    const std::string &globalAnalyzer,
                                    const std::set<std::string> &noTokenIndexs,
                                    const std::map<std::string, std::string> &indexAnalyzerMap,
                                    common::Query *query);
    common::Query *reserveFirstQuery(std::vector<common::Query*> &querys);
private:
    static bool validateQuery(common::Query* query,
                              const suez::turing::IndexInfos *indexInfos);
    static common::Query *cleanStopWords(common::Query*query);
    static search::QueryExecutor *createQueryExecutor(const common::Query *query,
            search::IndexPartitionReaderWrapperPtr &indexPartitionReader,
            const std::string &mainTableName,
            autil::mem_pool::Pool *pool,
            suez::turing::TimeoutTerminator *timeoutTerminator,
            search::LayerMeta *layerMeta);
    static void parseKvPairInfo(const std::string &kvStr,
                                std::map<std::string, std::string> &indexAnalyzerMap,
                                std::string &globalAnalyzer,
                                std::string &defaultOpStr,
                                std::set<std::string> &noTokenIndexes,
                                bool &tokenizeQuery, bool &removeStopWords);
    static bool isMatchUdf(const autil_rapidjson::SimpleValue &simpleVal);
    static bool isQueryUdf(const autil_rapidjson::SimpleValue &simpleVal);
    static bool isSpQueryMatchUdf(const autil_rapidjson::SimpleValue &simpleVal);

private:
    common::Query *_query;
    suez::turing::AttributeExpression *_attrExpr;
    Ha3ScanConditionVisitorParam _param;
private:
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(Ha3ScanConditionVisitor);

END_HA3_NAMESPACE(sql);

#endif //ISEARCH_HA3SCANCONDITIONVISITOR_H
