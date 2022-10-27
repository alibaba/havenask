#include <ha3/turing/variant/SqlResultVariant.h>
#include <tensorflow/core/framework/variant_op_registry.h>

using namespace std;
using namespace tensorflow;
using namespace autil;

BEGIN_HA3_NAMESPACE(turing);
HA3_LOG_SETUP(turing, SqlResultVariant);

void SqlResultVariant::Encode(VariantTensorData* data) const {
    data->set_type_name(TypeName());
    _result->serializeToString(data->metadata_, _pool);
}

bool SqlResultVariant::Decode(const VariantTensorData& data) {
    if (TypeName() != data.type_name()) {
        return false;
    }
    _metadata = data.metadata_;
    return true;
}

bool SqlResultVariant::construct(autil::mem_pool::Pool *outPool) {
    if (outPool == nullptr) {
        return false;
    }
    _result.reset(new navi::NaviResult());
    _result->deserializeFromString(_metadata, outPool);
    return true;
}

REGISTER_UNARY_VARIANT_DECODE_FUNCTION(SqlResultVariant, "SqlResult");

END_HA3_NAMESPACE(turing);

