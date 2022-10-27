#ifndef ISEARCH_SUMMARYEXTRACTORPROVIDER_H
#define ISEARCH_SUMMARYEXTRACTORPROVIDER_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/common/Request.h>
#include <ha3/config/SummaryProfileInfo.h>
#include <ha3/config/HitSummarySchema.h>
#include <suez/turing/expression/framework/AttributeExpressionCreator.h>
#include <ha3/search/SearchCommonResource.h>
#include <ha3/cava/SummaryProvider.h>

BEGIN_HA3_NAMESPACE(summary);

struct SummaryQueryInfo {
    SummaryQueryInfo() {}
    SummaryQueryInfo(const std::string &queryString,
                     const std::vector<common::Term> &terms)
        : queryString(queryString)
        , terms(terms)
    {}

    std::string queryString;
    std::vector<common::Term> terms;
};

class SummaryExtractorProvider
{
public:
    SummaryExtractorProvider(const SummaryQueryInfo *queryInfo,
                             const config::FieldSummaryConfigVec *fieldSummaryConfigVec,
                             const common::Request *request,
                             suez::turing::AttributeExpressionCreator *attrExprCreator,
                             config::HitSummarySchema *hitSummarySchema,
                             search::SearchCommonResource& resource);

    ~SummaryExtractorProvider();
private:
    SummaryExtractorProvider(const SummaryExtractorProvider &);
    SummaryExtractorProvider& operator=(const SummaryExtractorProvider &);
public:
    const SummaryQueryInfo *getQueryInfo() const { return _queryInfo; }
    const config::FieldSummaryConfigVec *getFieldSummaryConfig() const
    {
        return _fieldSummaryConfigVec;
    }
    const common::Request *getRequest() const { return _request;}

    config::HitSummarySchema *getHitSummarySchema() const { return _hitSummarySchema; }

    summaryfieldid_t declareSummaryField(const std::string &fieldName,
            FieldType fieldType = ft_string, bool isMultiValue = false)
    {
        return _hitSummarySchema->declareSummaryField(fieldName, fieldType, isMultiValue);
    }

    bool fillAttributeToSummary(const std::string& attrName);
    const std::map<summaryfieldid_t, suez::turing::AttributeExpression*>&
    getFilledAttributes() const
    {
        return _attributes;
    }
    common::Tracer *getRequestTracer() { return _tracer;}
    ha3::SummaryProvider *getCavaSummaryProvider() {return &_summaryProvider;}
    suez::turing::SuezCavaAllocator *getCavaAllocator() {return _cavaAllocator;}
private:
    const SummaryQueryInfo *_queryInfo;
    const config::FieldSummaryConfigVec *_fieldSummaryConfigVec;
    const common::Request *_request;
    suez::turing::AttributeExpressionCreator *_attrExprCreator;
    config::HitSummarySchema *_hitSummarySchema;
    std::map<summaryfieldid_t, suez::turing::AttributeExpression*> _attributes;
    common::Ha3MatchDocAllocator *_matchDocAllocator;
    common::Tracer* _tracer;
    ha3::SummaryProvider _summaryProvider;
    suez::turing::SuezCavaAllocator *_cavaAllocator;
private:
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(SummaryExtractorProvider);

END_HA3_NAMESPACE(summary);

#endif //ISEARCH_SUMMARYEXTRACTORPROVIDER_H
