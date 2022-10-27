#include <tensorflow/core/framework/op.h>
#include <tensorflow/core/framework/shape_inference.h>
#include <tensorflow/core/framework/common_shape_fns.h>
#include <tensorflow/core/lib/core/errors.h>
#include <ha3/common.h>
#include <ha3/isearch.h>

using namespace tensorflow;

BEGIN_HA3_NAMESPACE(turing);

REGISTER_OP("RunSqlOp")
.Input("graph_def: string")
.Output("output: string");


END_HA3_NAMESPACE(turing);
