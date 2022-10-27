#pragma once
#include <autil/mem_pool/Pool.h>
#include <autil/mem_pool/PoolVector.h>
#include <tensorflow/core/framework/op_kernel.h>
#include <ha3/util/Log.h>
#include <ha3/turing/variant/Ha3RequestVariant.h>
#include <ha3/turing/variant/Ha3ResultVariant.h>
#include <ha3/proxy/VariableSlabComparator.h>
#include <ha3/proxy/AggFunResultMerger.h>
#include <ha3/proxy/AggResultSort.h>
#include <ha3/proxy/MatchDocDeduper.h>
#include <ha3/turing/ops/QrsQueryResource.h>
#include <ha3/turing/ops/QrsSessionResource.h>
#include <ha3/common/ha3_op_common.h>

BEGIN_HA3_NAMESPACE(turing);

class Ha3ResultMergeOp : public tensorflow::OpKernel
{
public:
    explicit Ha3ResultMergeOp(tensorflow::OpKernelConstruction* ctx) : tensorflow::OpKernel(ctx) {
    }
public:
    void Compute(tensorflow::OpKernelContext* ctx) override;
public:
    friend class Ha3ResultMergeOpTest;

private:
    struct MergeMatchDocResultMeta {
    public:
        MergeMatchDocResultMeta()
            : errorCode(ERROR_NONE)
            , actualMatchDocs(0)
            , totalMatchDocs(0)
        {
        }
    public:
        ErrorCode errorCode;
        uint32_t actualMatchDocs;
        uint32_t totalMatchDocs;
    };

private:
    void mergeResults(common::Result *outputResult,
                      const std::vector<common::ResultPtr> &inputResults,
                      const common::Request* request);

    void mergeErrors(common::Result *outputResult,
                     const std::vector<common::ResultPtr> &inputResults);

    void mergeTracer(const common::Request *request,
                     common::Result *outputResult,
                     const std::vector<common::ResultPtr> &inputResults);

    proxy::AggFunResultMerger *createAggFunResultMerger(
            const std::vector<std::string> &funNames,
            const matchdoc::MatchDocAllocatorPtr &allocatorPtr);

    common::AggregateResultPtr mergeOneAggregateResult(
            const std::vector<common::ResultPtr>& results, size_t aggOffset,
            const std::string& groupExprStr,
            autil::mem_pool::Pool* pool);

    bool checkAggResults(const common::AggregateClause *aggClause,
                         const std::vector<common::ResultPtr>& results);

    bool checkAggResult(const common::ResultPtr &resultPtr,
                        size_t expectSize);

    void mergeAggResults(common::Result *outputResult,
                         const std::vector<common::ResultPtr>& inputResults,
                         const common::Request *request);

    void mergeCoveredRanges(common::Result *outputResult,
                            const std::vector<common::ResultPtr> &inputResults);

    void mergeGlobalVariables(common::Result *outputResult,
                              const std::vector<common::ResultPtr> &inputResults);

    common::ResultPtr findFirstNoneEmptyResult(const std::vector<common::ResultPtr>& results,
            uint32_t &allMatchDocsSize);

    MergeMatchDocResultMeta doMergeMatchDocsMeta(
            const std::vector<common::ResultPtr> &inputResults,
            bool doDedup, const common::Ha3MatchDocAllocatorPtr &allocatorPtr,
            autil::mem_pool::PoolVector<matchdoc::MatchDoc> &matchDocVect);

    void mergeMatchDocs(common::Result *outputResult,
                        const std::vector<common::ResultPtr> &inputResults,
                        const common::Request *request,
                        bool doDedup);

    void mergePhaseOneSearchInfos(common::Result *outputResult,
                                  const std::vector<common::ResultPtr> &inputResults);

    void mergeHits(common::Result *outputResult,
                   const std::vector<common::ResultPtr> &inputResults,
                   bool doDedup);

    void mergePhaseTwoSearchInfos(common::Result *outputResult,
                                  const std::vector<common::ResultPtr> &inputResults);

    bool judgeHasMatchDoc(const common::ResultPtr &outputResult);

private:
    HA3_LOG_DECLARE();
};

END_HA3_NAMESPACE(turing);
