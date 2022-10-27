#ifndef ISEARCH_RELEASEREDUNDANTV2OP_H
#define ISEARCH_RELEASEREDUNDANTV2OP_H

#include <autil/Log.h>
#include <tensorflow/core/framework/op_kernel.h>
#include <iostream>
#include <ha3/common/ha3_op_common.h>
#include <suez/turing/common/SessionResource.h>
#include <ha3/util/Log.h>
#include <ha3/turing/ops/SearcherQueryResource.h>
#include <ha3/turing/ops/SearcherSessionResource.h>
#include <ha3/service/SearcherResource.h>
#include <ha3/search/MatchDocSearcher.h>
#include <ha3/turing/variant/Ha3RequestVariant.h>

BEGIN_HA3_NAMESPACE(turing);

class Ha3ReleaseRedundantV2Op : public tensorflow::OpKernel
{
public:
    explicit Ha3ReleaseRedundantV2Op(tensorflow::OpKernelConstruction* ctx) : tensorflow::OpKernel(ctx) {
    }
public:

    void Compute(tensorflow::OpKernelContext* ctx) override;

private:
    void releaseRedundantMatchDocs(std::vector<matchdoc::MatchDoc> &matchDocVec,
                                   uint32_t resultCount,
                                   const common::Ha3MatchDocAllocatorPtr &matchDocAllocator);
    uint32_t getResultCount(const common::Request *request, uint32_t requiredTopK, SearcherQueryResource *searcherQueryResource);

private:
    HA3_LOG_DECLARE();
};

END_HA3_NAMESPACE(turing);
#endif // ISEARCH_RELEASEREDUNDANTV2OP_H
