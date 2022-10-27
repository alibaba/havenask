#include <ha3/turing/variant/Ha3RequestVariant.h>
#include <tensorflow/core/framework/variant_op_registry.h>
#include <autil/DataBuffer.h>

using namespace std;
using namespace tensorflow;
using namespace autil;
using namespace autil::mem_pool;

USE_HA3_NAMESPACE(common);
BEGIN_HA3_NAMESPACE(turing);

HA3_LOG_SETUP(turing, Ha3RequestVariant);

Ha3RequestVariant::Ha3RequestVariant()
{
    _externalPool = nullptr;
}

Ha3RequestVariant::Ha3RequestVariant(autil::mem_pool::Pool* pool)
{
    _externalPool = pool;
    _request.reset(new Request(_externalPool));
}

Ha3RequestVariant::Ha3RequestVariant(HA3_NS(common)::RequestPtr request,
                                     autil::mem_pool::Pool *pool)
{
    _externalPool = pool;
    _request = request;
}

Ha3RequestVariant::Ha3RequestVariant(const Ha3RequestVariant &other)
    : _externalPool(other._externalPool)
    , _request(other._request)
    , _metadata(other._metadata)
{
}

Ha3RequestVariant::~Ha3RequestVariant() {
    _request.reset();
    _externalPool = nullptr;
}

void Ha3RequestVariant::Encode(VariantTensorData* data) const {
    data->set_type_name(TypeName());
    _request->serializeToString(data->metadata_);
}

bool Ha3RequestVariant::Decode(const VariantTensorData& data) {
    if (TypeName() != data.type_name()) {
        return false;
    }
    _metadata = data.metadata_;
    return true;
}

bool Ha3RequestVariant::construct(autil::mem_pool::Pool *outPool) {
    _request.reset(new Request(outPool));
    _request->deserializeFromString(_metadata, outPool);
    return true;
}


REGISTER_UNARY_VARIANT_DECODE_FUNCTION(Ha3RequestVariant, "Ha3Request");

END_HA3_NAMESPACE(turing);
