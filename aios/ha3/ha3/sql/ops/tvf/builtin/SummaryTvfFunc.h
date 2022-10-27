#pragma once
#include <ha3/sql/ops/tvf/TvfFunc.h>
#include <ha3/sql/ops/tvf/TvfFuncCreator.h>
#include <ha3/summary/SummaryProfileManager.h>
#include <ha3/summary/SummaryExtractorProvider.h>

BEGIN_HA3_NAMESPACE(sql);

class SummaryTvfFunc : public TvfFunc
{
public:
    SummaryTvfFunc();
    ~SummaryTvfFunc();
    SummaryTvfFunc(const SummaryTvfFunc &) = delete;
    SummaryTvfFunc& operator=(const SummaryTvfFunc &) = delete;
public:
    bool init(TvfFuncInitContext &context) override;
    bool compute(const TablePtr &input, bool eof, TablePtr &output);
private:
    bool doCompute(const TablePtr &input);
    bool prepareResource(TvfFuncInitContext &context);
    void toSummarySchema(const TablePtr &input);
    void toSummaryHits(const TablePtr &input,
                       std::vector<HA3_NS(common)::SummaryHit *> &summaryHits);
    template <typename T>
    bool fillSummaryDocs(ColumnPtr &column,
                         std::vector<HA3_NS(common)::SummaryHit *> &summaryHits,
                         const TablePtr &input,
                         summaryfieldid_t summaryFieldId);
    bool fromSummaryHits(const TablePtr &input,
                         std::vector<HA3_NS(common)::SummaryHit *> &summaryHits);

private:
    HA3_NS(summary)::SummaryExtractorChain *_summaryExtractorChain;
    HA3_NS(summary)::SummaryExtractorProvider *_summaryExtractorProvider;
    const HA3_NS(summary)::SummaryProfile *_summaryProfile;
    HA3_NS(summary)::SummaryQueryInfo *_summaryQueryInfo;
    HA3_NS(common)::RequestPtr _request;
    HA3_NS(search)::SearchCommonResource *_resource;
    HA3_NS(config)::HitSummarySchemaPtr _hitSummarySchema;
    autil::mem_pool::Pool *_queryPool;
    std::vector<std::string> _outputFields;
    std::vector<std::string> _outputFieldsType;
};

HA3_TYPEDEF_PTR(SummaryTvfFunc);

class SummaryTvfFuncCreator : public TvfFuncCreator
{
public:
    SummaryTvfFuncCreator();
    ~SummaryTvfFuncCreator();
private:
    SummaryTvfFuncCreator(const SummaryTvfFuncCreator &) = delete;
    SummaryTvfFuncCreator& operator=(const SummaryTvfFuncCreator &) = delete;
private:
    TvfFunc *createFunction(const HA3_NS(config)::SqlTvfProfileInfo &info) override;
};

HA3_TYPEDEF_PTR(SummaryTvfFuncCreator);

END_HA3_NAMESPACE(sql);
