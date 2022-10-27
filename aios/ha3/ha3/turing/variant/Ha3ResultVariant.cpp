#include <ha3/turing/variant/Ha3ResultVariant.h>
#include <tensorflow/core/framework/variant_op_registry.h>
#include <autil/DataBuffer.h>

using namespace std;
using namespace tensorflow;
using namespace autil;
using namespace autil::mem_pool;

USE_HA3_NAMESPACE(common);
BEGIN_HA3_NAMESPACE(turing);

HA3_LOG_SETUP(turing, Ha3ResultVariant);

Ha3ResultVariant::Ha3ResultVariant() 
{
    _externalPool = nullptr;
}

Ha3ResultVariant::Ha3ResultVariant(autil::mem_pool::Pool* pool) 
{
    _externalPool = pool;
    _result.reset(new Result());
}

Ha3ResultVariant::Ha3ResultVariant(HA3_NS(common)::ResultPtr result, autil::mem_pool::Pool* pool)
{
    _externalPool = pool;
    _result = result;
}

Ha3ResultVariant::Ha3ResultVariant(const Ha3ResultVariant &other)
    : _externalPool(other._externalPool)
    , _result(other._result)
    , _metadata(other._metadata)
{
}

Ha3ResultVariant::~Ha3ResultVariant() {
    _result.reset();
    _externalPool = nullptr;
}

void Ha3ResultVariant::Encode(VariantTensorData* data) const {
    data->set_type_name(TypeName());
    _result->serializeToString(data->metadata_, _externalPool);
}

bool Ha3ResultVariant::Decode(const VariantTensorData& data) {
    if (TypeName() != data.type_name()) {
        return false;
    }
    _metadata = data.metadata_;
    return true;
}

bool Ha3ResultVariant::construct(autil::mem_pool::Pool *outPool) {
    _result.reset(new Result());
    _result->deserializeFromString(_metadata, outPool);
    return true;
}

REGISTER_UNARY_VARIANT_DECODE_FUNCTION(Ha3ResultVariant, "Ha3Result");

END_HA3_NAMESPACE(turing);
