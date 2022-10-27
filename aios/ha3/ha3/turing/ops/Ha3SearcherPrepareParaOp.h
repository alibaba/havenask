#ifndef ISEARCH_HA3SEARCHERPREPAREPARAOP_H
#define ISEARCH_HA3SEARCHERPREPAREPARAOP_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <tensorflow/core/framework/op_kernel.h>

BEGIN_HA3_NAMESPACE(turing);

class Ha3SearcherPrepareParaOp : public tensorflow::OpKernel
{
public:
    explicit Ha3SearcherPrepareParaOp(tensorflow::OpKernelConstruction* ctx) : tensorflow::OpKernel(ctx) {}
    void Compute(tensorflow::OpKernelContext* ctx) override;

private:
    Ha3SearcherPrepareParaOp(const Ha3SearcherPrepareParaOp &);
    Ha3SearcherPrepareParaOp& operator=(const Ha3SearcherPrepareParaOp &);

private:
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(Ha3SearcherPrepareParaOp);

END_HA3_NAMESPACE(turing);

#endif //ISEARCH_HA3SEARCHERPREPAREPARAOP_H
