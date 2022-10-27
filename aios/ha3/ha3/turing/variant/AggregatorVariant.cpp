#include <ha3/turing/variant/AggregatorVariant.h>
#include <ha3/search/AggregatorCreator.h>
#include <tensorflow/core/framework/variant_op_registry.h>

using namespace std;
using namespace suez::turing;
using namespace tensorflow;

BEGIN_HA3_NAMESPACE(turing);
HA3_LOG_SETUP(turing, AggregatorVariant);


REGISTER_UNARY_VARIANT_DECODE_FUNCTION(AggregatorVariant, "Aggregator");

END_HA3_NAMESPACE(turing);
