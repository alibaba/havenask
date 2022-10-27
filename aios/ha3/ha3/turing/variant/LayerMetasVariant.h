#ifndef ISEARCH_LAYERMETASVARIANT_H
#define ISEARCH_LAYERMETASVARIANT_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/search/LayerMetas.h>
#include <tensorflow/core/framework/variant.h>
#include <tensorflow/core/framework/tensor.h>
#include <tensorflow/core/framework/variant_encode_decode.h>

BEGIN_HA3_NAMESPACE(turing);

class LayerMetasVariant
{
public:
    LayerMetasVariant() {}
    LayerMetasVariant(search::LayerMetasPtr layerMetas) :_layerMetas(layerMetas) {}
    ~LayerMetasVariant() {}

public:
    void Encode(tensorflow::VariantTensorData* data) const {}
    bool Decode(const tensorflow::VariantTensorData& data) {
        return false;
    }
    std::string TypeName() const {
        return "Ha3LayerMetas";
    }
public:
    search::LayerMetasPtr getLayerMetas() const {
        return _layerMetas;
    }

private:
    search::LayerMetasPtr _layerMetas;
private:
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(LayerMetasVariant);

END_HA3_NAMESPACE(turing);

#endif //ISEARCH_LAYERMETASVARIANT_H
