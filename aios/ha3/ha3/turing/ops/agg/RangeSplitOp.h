#ifndef ISEARCH_RANGESPLITOP_H
#define ISEARCH_RANGESPLITOP_H

#include <vector>
#include <deque>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/common/ha3_op_common.h>
#include <tensorflow/core/framework/op_kernel.h>
#include <ha3/turing/variant/LayerMetasVariant.h>

BEGIN_HA3_NAMESPACE(turing);
class RangeSplitOp : public tensorflow::OpKernel {
public:
    explicit RangeSplitOp(tensorflow::OpKernelConstruction* ctx);
public:
    void Compute(tensorflow::OpKernelContext* ctx) override;
public:
    static std::vector<search::LayerMetasPtr> splitLayer(
            const search::LayerMetasPtr &layerMetas,
            int count, autil::mem_pool::Pool *pool);
    static search::LayerMetasPtr splitLayerMeta(
            std::deque<std::pair<int32_t, int32_t> > &ranges,
            int32_t capacity,
            autil::mem_pool::Pool *pool);
    static void updateQuota(std::vector<search::LayerMetasPtr> &layerMetasVec);
    static bool mergeLayerMeta(const search::LayerMetasPtr &layerMetas, search::LayerMeta &layerMeta);
private:
    int32_t _outputNum;
private:
    HA3_LOG_DECLARE();
};

END_HA3_NAMESPACE(turing);

#endif //ISEARCH_RANGESPLITOP_H
