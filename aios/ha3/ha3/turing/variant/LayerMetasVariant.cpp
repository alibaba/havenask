#include <ha3/turing/variant/LayerMetasVariant.h>
#include <tensorflow/core/framework/variant_op_registry.h>

using namespace tensorflow;

BEGIN_HA3_NAMESPACE(turing);

REGISTER_UNARY_VARIANT_DECODE_FUNCTION(LayerMetasVariant, "Ha3LayerMetas");

END_HA3_NAMESPACE(turing);
