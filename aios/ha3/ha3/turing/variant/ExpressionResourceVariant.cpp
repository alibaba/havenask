#include <ha3/turing/variant/ExpressionResourceVariant.h>
#include <tensorflow/core/framework/variant_op_registry.h>

using namespace tensorflow;

BEGIN_HA3_NAMESPACE(turing);


REGISTER_UNARY_VARIANT_DECODE_FUNCTION(ExpressionResourceVariant, "ExpressionResource");

END_HA3_NAMESPACE(turing);
