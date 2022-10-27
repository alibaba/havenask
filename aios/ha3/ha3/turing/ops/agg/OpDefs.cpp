#include <tensorflow/core/framework/op.h>
#include <tensorflow/core/framework/shape_inference.h>
#include <tensorflow/core/framework/common_shape_fns.h>
#include <tensorflow/core/lib/core/errors.h>
#include <ha3/common.h>
#include <ha3/isearch.h>

using namespace tensorflow;

BEGIN_HA3_NAMESPACE(turing);

REGISTER_OP("RequestInitOp")
.Input("input_request: variant")
.Output("output_request: variant");

REGISTER_OP("LayerMetasCreateOp")
.Input("input_request: variant")
.Output("layer_metas: variant");

REGISTER_OP("RangeSplitOp")
.Input("input_ranges: variant")
.Output("output_ranges: N * variant")
.Attr("N: int >= 1")
.SetShapeFn([](shape_inference::InferenceContext *c) {
        int dim_size = 0;
        TF_RETURN_IF_ERROR(c->GetAttr("N", &dim_size));
        for (int32_t idx = 0; idx < dim_size; idx++) {
            c->set_output(idx, c->input(0));
        }
        return Status::OK();
    });

REGISTER_OP("PrepareExpressionResourceOp")
.SetIsStateful()
.Input("request: variant")
.Output("express_resource: variant");

REGISTER_OP("SeekIteratorPrepareOp")
.SetIsStateful()
.Input("layer_metas: variant")
.Input("expression_resource: variant")
.Output("seek_iterator: variant");

REGISTER_OP("SeekOp")
.SetIsStateful()
.Attr("batch_size: int = -1")
.Input("input_iterator: variant")
.Output("output_iterator: variant")
.Output("matchdocs: variant")
.Output("eof: bool");

REGISTER_OP("SeekAndAggOp")
.SetIsStateful()
.Attr("batch_size: int = 0")
.Input("aggregator_input: variant")
.Input("iterator: variant")
.Output("aggregator_output: variant");

REGISTER_OP("AggPrepareOp")
.SetIsStateful()
.Attr("relative_path: string = ''")
.Attr("json_path: string = ''")
.Input("express_resource: variant")
.Output("aggregator: variant");

REGISTER_OP("AggregatorOp")
.SetIsStateful()
.Input("aggregator_input: variant")
.Input("matchdocs_input: variant")
.Output("aggregator_output: variant")
.Output("matchdocs_output: variant");

REGISTER_OP("AggMergeOp")
.SetIsStateful()
.Attr("N: int >= 1")
.Input("aggregators: N * variant")
.Output("aggregate_result: variant");

REGISTER_OP("MatchDocReleaseOp")
.SetIsStateful()
.Input("seek_end_input: bool")
.Input("matchdocs: variant")
.Output("seek_end_output: bool");

REGISTER_OP("AggResultConstructOp")
.SetIsStateful()
.Input("ha3aggresults: variant")
.Output("ha3result: variant");

END_HA3_NAMESPACE(turing);
