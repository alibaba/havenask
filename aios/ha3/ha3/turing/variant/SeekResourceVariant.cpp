#include <ha3/turing/variant/SeekResourceVariant.h>
#include "tensorflow/core/framework/variant_op_registry.h"
#include <autil/DataBuffer.h>

using namespace std;
using namespace tensorflow;

BEGIN_HA3_NAMESPACE(turing);
HA3_LOG_SETUP(turing, SeekResourceVariant);

REGISTER_UNARY_VARIANT_DECODE_FUNCTION(SeekResourceVariant, "SeekResource");

END_HA3_NAMESPACE(turing);

