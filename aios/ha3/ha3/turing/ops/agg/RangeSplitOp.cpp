#include <ha3/turing/ops/agg/RangeSplitOp.h>

using namespace tensorflow;
using namespace suez::turing;
using namespace std;

USE_HA3_NAMESPACE(search);

BEGIN_HA3_NAMESPACE(turing);
HA3_LOG_SETUP(turing, RangeSplitOp);

RangeSplitOp::RangeSplitOp(tensorflow::OpKernelConstruction* ctx)
    : tensorflow::OpKernel(ctx)
{
    OP_REQUIRES_OK(ctx, ctx->GetAttr("N", &_outputNum));
}

void RangeSplitOp::Compute(tensorflow::OpKernelContext* ctx) {
    auto sessionResource = GET_SESSION_RESOURCE();
    auto queryResource = GET_QUERY_RESOURCE(sessionResource);

    auto layerMetasVariant = ctx->input(0).scalar<Variant>()().get<LayerMetasVariant>();
    OP_REQUIRES(ctx, layerMetasVariant, errors::Unavailable("get layer metas variant faield"));

    LayerMetasPtr layerMetas = layerMetasVariant->getLayerMetas();
    OP_REQUIRES(ctx, layerMetas, errors::Unavailable("get layer meta failed"));
    OP_REQUIRES(ctx, layerMetas->size() > 0, errors::Unavailable("layer metas count is 0"));

    vector<LayerMetasPtr> splitLayerMetas;
    splitLayerMetas = splitLayer(layerMetas, _outputNum, queryResource->getPool());
    updateQuota(splitLayerMetas);
    OP_REQUIRES(ctx, _outputNum == (int32_t)splitLayerMetas.size(),
                errors::Unavailable("output num not equal split layer metas count."));
    for (int idx = 0; idx < _outputNum; idx++) {
        LayerMetasVariant outVariant(splitLayerMetas[idx]);
        Tensor *outputTensor = nullptr;
        OP_REQUIRES_OK(ctx, ctx->allocate_output(idx, TensorShape({}), &outputTensor));
        outputTensor->scalar<Variant>()() = outVariant;
    }
}

vector<LayerMetasPtr> RangeSplitOp::splitLayer(
        const LayerMetasPtr &layerMetas, int count,
        autil::mem_pool::Pool *pool)
{
    vector<LayerMetasPtr> splitLayerMetas;
    LayerMeta layerMeta(pool);
    if (!mergeLayerMeta(layerMetas, layerMeta)) {
        HA3_LOG(WARN, "merge layer meta failed.");
        return splitLayerMetas;
    }
    int32_t size = layerMeta.size();
    int32_t totalLength = 0;
    deque<pair<int32_t, int32_t> > ranges;
    for (int32_t idx = 0; idx < size; idx++) {
        int32_t begin = layerMeta[idx].begin;
        int32_t end = layerMeta[idx].end;
        if (end >= begin) {
            totalLength += end - begin + 1;
            ranges.push_back(make_pair(begin, end));
        }
    }
    int32_t capacity = int32_t(totalLength / count);
    int32_t remainCapacity = totalLength - (capacity * count);
    for (int idx = 0; idx < count; idx++) {
        int32_t currentCapacity = capacity;
        if (idx < remainCapacity) {
            currentCapacity++;
        }
        LayerMetasPtr tmpLayerMetas = splitLayerMeta(ranges, currentCapacity, pool);
        if (tmpLayerMetas && tmpLayerMetas->size() > 0) {
            (*tmpLayerMetas)[0].quota = layerMeta.quota;
            (*tmpLayerMetas)[0].maxQuota = layerMeta.maxQuota;
            (*tmpLayerMetas)[0].quotaMode = layerMeta.quotaMode;
            (*tmpLayerMetas)[0].needAggregate = layerMeta.needAggregate;
            (*tmpLayerMetas)[0].quotaType = layerMeta.quotaType;
        }
        splitLayerMetas.push_back(tmpLayerMetas);
    }
    return splitLayerMetas;
}

bool RangeSplitOp::mergeLayerMeta(const LayerMetasPtr &layerMetas, LayerMeta &layerMeta)
{
    if (layerMetas == nullptr || layerMetas->size() == 0) {
        return false;
    }
    for (size_t i = 0; i < layerMetas->size(); i++) {
        LayerMeta &tmpLayerMeta = (*layerMetas)[i];
        layerMeta.insert(layerMeta.end(), tmpLayerMeta.begin(), tmpLayerMeta.end());
    }
    sort(layerMeta.begin(), layerMeta.end(), DocIdRangeMetaCompare());
    if (layerMeta.size() > 1) {
        docid_t end = layerMeta[0].end;
        for (size_t i = 1; i < layerMeta.size(); i++) {
            if (layerMeta[i].begin <= end) {
                layerMeta[i].begin = end + 1;
            }
            if (layerMeta[i].end > end) {
                end = layerMeta[i].end;
            }
        }
    }
    return true;
}

LayerMetasPtr RangeSplitOp::splitLayerMeta(deque<pair<int32_t, int32_t> > &ranges, int32_t capacity,
        autil::mem_pool::Pool *pool)
{
    LayerMeta layerMeta(pool);
    int remainSize = capacity;
    while (remainSize > 0 && ranges.size() > 0) {
        int firstRangeSize = ranges[0].second - ranges[0].first + 1;
        if (firstRangeSize < remainSize) {
            remainSize -= firstRangeSize;
            layerMeta.push_back(DocIdRangeMeta(ranges[0].first, ranges[0].second));
            ranges.pop_front();
        } else {
            layerMeta.push_back(DocIdRangeMeta(ranges[0].first, ranges[0].first + remainSize - 1));
            ranges[0].first += remainSize;
            if (ranges[0].first > ranges[0].second) {
                ranges.pop_front();
            }
            break;
        }
    }
    if (layerMeta.size() > 0) {
        LayerMetasPtr layerMetas(new LayerMetas(pool));
        layerMetas->push_back(layerMeta);
        return layerMetas;
    } else {
        return LayerMetasPtr();
    }
}

void RangeSplitOp::updateQuota(vector<LayerMetasPtr> &layerMetasVec) {
    for (auto layerMetas : layerMetasVec) {
        if (layerMetas == NULL) {
            continue;
        }
        for (auto &layerMeta : *layerMetas) {
            for (auto &rangeMeta : layerMeta) {
                rangeMeta.quota =  rangeMeta.end - rangeMeta.begin + 1;
            }
        }
    }
}

REGISTER_KERNEL_BUILDER(Name("RangeSplitOp")
                        .Device(DEVICE_CPU),
                        RangeSplitOp);

END_HA3_NAMESPACE(turing);
