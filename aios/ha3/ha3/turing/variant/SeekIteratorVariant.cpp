#include <ha3/turing/variant/SeekIteratorVariant.h>
#include <tensorflow/core/framework/variant_op_registry.h>
#include <autil/DataBuffer.h>

using namespace std;
using namespace tensorflow;
using namespace autil;

BEGIN_HA3_NAMESPACE(turing);
HA3_LOG_SETUP(turing, SeekIterator);
HA3_LOG_SETUP(turing, SeekIteratorVariant);

REGISTER_UNARY_VARIANT_DECODE_FUNCTION(SeekIteratorVariant, "SeekIterator");

END_HA3_NAMESPACE(turing);

