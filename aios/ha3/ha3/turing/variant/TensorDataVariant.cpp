#include <ha3/turing/variant/TensorDataVariant.h>
#include <tensorflow/core/framework/variant_op_registry.h>

using namespace std;
using namespace tensorflow;
using namespace autil;

USE_HA3_NAMESPACE(sql);
BEGIN_HA3_NAMESPACE(turing);
HA3_LOG_SETUP(turing, TensorDataVariant);

TensorDataVariant::TensorDataVariant()
    : _pool(NULL)
{}

TensorDataVariant::TensorDataVariant(autil::mem_pool::Pool *pool)
    : _tensorData(new TensorData(pool))
    , _pool(pool)
{
}

TensorDataVariant::~TensorDataVariant() {
    _tensorData.reset();
    _pool = NULL;
}

TensorDataVariant::TensorDataVariant(const TensorDataVariant &other)
    : _tensorData(other._tensorData)
    , _pool(other._pool)
    , _metadata(other._metadata)
{
}

void TensorDataVariant::Encode(VariantTensorData* data) const {
    data->set_type_name(TypeName());
    _tensorData->serializeToString(data->metadata_, _pool);
}

bool TensorDataVariant::Decode(const VariantTensorData& data) {
    if (TypeName() != data.type_name()) {
        return false;
    }
    _metadata = data.metadata_;
    return true;
}

bool TensorDataVariant::construct(autil::mem_pool::Pool *outPool) {
    _tensorData.reset(new TensorData(outPool));
    _tensorData->deserializeFromString(_metadata, outPool);
    return true;
}

REGISTER_UNARY_VARIANT_DECODE_FUNCTION(TensorDataVariant, "TensorData");

END_HA3_NAMESPACE(turing);

