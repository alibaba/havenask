#include <ha3/turing/variant/InnerResultVariant.h>
#include "tensorflow/core/framework/variant_op_registry.h"
#include <autil/DataBuffer.h>

using namespace std;
using namespace tensorflow;

BEGIN_HA3_NAMESPACE(turing);
HA3_LOG_SETUP(turing, InnerResultVariant);

REGISTER_UNARY_VARIANT_DECODE_FUNCTION(InnerResultVariant, "InnerResult");

END_HA3_NAMESPACE(turing);

