/*
 * Copyright 2014-present Alibaba Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#include <stdint.h>

#include "tensorflow/core/framework/op.h"
#include "tensorflow/core/framework/resource_handle.pb.h"
#include "tensorflow/core/framework/shape_inference.h"
#include "tensorflow/core/lib/core/errors.h"
#include "tensorflow/core/lib/core/status.h"


using namespace tensorflow;

namespace isearch {
namespace turing {

REGISTER_OP("FinalSortOp")
.Input("integers: int32")
.Output("matchdocs: variant");

REGISTER_OP("IsPhaseOneOp")
.SetIsStateful()
.Input("ha3request: variant")
.Output("is_phase_one: bool");

REGISTER_OP("Ha3NeedHobbitOp")
.SetIsStateful()
.Input("ha3request: variant")
.Output("need_hobbit: bool");

REGISTER_OP("Ha3SummaryOp")
.Input("ha3request: variant")
.Output("ha3result: variant")
.Attr("force_allow_lack_of_summary: bool = false");

REGISTER_OP("Ha3ExtraSummaryOp")
.Input("ha3request: variant")
.Input("inputha3result: variant")
.Output("ha3result: variant")
.Attr("table_name: string = \"\"");

REGISTER_OP("Ha3ResultFormatOp")
.Input("ha3request: variant")
.Input("ha3result: variant")
.Output("result_string: string");

REGISTER_OP("Ha3ResultMergeOp")
.Input("ha3request: variant")
.Input("ha3results: variant")
.Output("ha3result_with_matchdoc: variant")
.Output("ha3result_without_matchdoc: variant")
.Attr("self_define_type: string = \"Switch\"");

REGISTER_OP("Ha3ResultMergeVecOp")
.Input("ha3request: variant")
.Input("ha3results: N * variant")
.Output("ha3result_with_matchdoc: variant")
.Output("ha3result_without_matchdoc: variant")
.Attr("N: int >= 1")
.Attr("self_define_type: string = \"Switch\"");

REGISTER_OP("Ha3SearcherCacheSwitchOp")
.Input("ha3result: variant")
.Output("ha3result_not_use_cache: variant")
.Output("ha3result_miss_cache: variant")
.Output("ha3result_hit_cache: variant")
.Attr("self_define_type: string = \"Switch\"");

REGISTER_OP("Ha3ResultSplitOp")
.Input("ha3request: variant")
.Input("ha3result: variant")
.Output("matchdocs: variant")
.Output("rankcount: uint32")
.Output("totalmatchdocs: uint32")
.Output("actualmatchdocs: uint32")
.Output("ha3result_out: variant")
.Output("src_count: uint16");

REGISTER_OP("Ha3RequestInitOp")
.Input("ha3request: variant")
.Output("ha3request_out: variant");

REGISTER_OP("Ha3SeekOp")
.Input("ha3request: variant")
.Output("matchdocs_out: variant")
.Output("extrarankcount: uint32")
.Output("totalmatchdocs: uint32")
.Output("actualmatchdocs: uint32")
.Output("ha3aggresults: variant")
.Output("ha3errorresults: variant")
.Attr("self_define_type: string = \"Switch\"");

REGISTER_OP("Ha3SeekParaOp")
.Input("ha3request: variant")
.Input("ha3seekresource: variant")
.Output("inner_result: variant")
.Output("has_error: bool");

REGISTER_OP("Ha3SeekParaMergeOp")
.Input("ha3request: variant")
.Input("seek_resources: N * variant")
.Input("seek_results: N * variant")
.Input("has_errors: N * bool")
.Output("matchdocs_out: variant")
.Output("extrarankcount: uint32")
.Output("totalmatchdocs: uint32")
.Output("actualmatchdocs: uint32")
.Output("ha3aggresults: variant")
.Output("ha3errorresults: variant")
.Attr("N: int >= 1")
.Attr("self_define_type: string = \"Switch\"");

REGISTER_OP("Ha3SorterOp")
.Input("ha3request: variant")
.Input("matchdocs: variant")
.Input("rankcount: uint32")
.Input("totalmatchdocs: uint32")
.Input("actualmatchdocs: uint32")
.Input("src_count: uint16")
.Output("matchdocs_out: variant")
.Output("totalmatchdocs_out: uint32")
.Output("actualmatchdocs_out: uint32")
.Attr("attr_location: string");

REGISTER_OP("Ha3ResultConstructOp")
.Input("ha3request: variant")
.Input("matchdocs: variant")
.Input("totalmatchdocs: uint32")
.Input("actualmatchdocs: uint32")
.Input("ha3aggresults: variant")
.Output("ha3result: variant");

REGISTER_OP("Ha3ResultReconstructOp")
.Input("ha3result: variant")
.Input("matchdocs: variant")
.Input("totalmatchdocs: uint32")
.Input("actualmatchdocs: uint32")
.Output("ha3result_out: variant");

REGISTER_OP("Ha3FillAttributesOp")
.Input("ha3request: variant")
.Input("matchdocs: variant")
.Output("matchdocs_out: variant");

REGISTER_OP("Ha3SearcherPrepareOp")
.SetIsStateful()
.Input("ha3request: variant");

REGISTER_OP("Ha3SeekParaSplitOp")
.Input("ha3request: variant")
.Output("output_layermetas: N * variant")
.Attr("N: int >= 1")
.SetShapeFn([](shape_inference::InferenceContext *c) {
        int dim_size = 0;
        TF_RETURN_IF_ERROR(c->GetAttr("N", &dim_size));
        for (int32_t idx = 0; idx < dim_size; idx++) {
            c->set_output(idx, c->input(0));
        }
        return Status::OK();
    });

REGISTER_OP("Ha3SearcherPrepareParaOp")
.SetIsStateful()
.Input("ha3request: variant")
.Input("layermetas: variant")
.Output("out_request: variant")
.Output("seek_resource: variant");

REGISTER_OP("Ha3CachePrepareOp")
.SetIsStateful()
.Input("ha3request: variant");

REGISTER_OP("Ha3SearcherPutCacheOp")
.Input("ha3request: variant")
.Input("ha3result: variant")
.Output("ha3result_out: variant");

REGISTER_OP("Ha3ReleaseRedundantOp")
.Input("ha3result: variant")
.Output("ha3result_out: variant");

REGISTER_OP("Ha3FillGlobalIdInfoOp")
.Input("ha3request: variant")
.Input("ha3result: variant")
.Output("ha3result_out: variant");

REGISTER_OP("Ha3CacheRefreshAttrOp")
.Input("ha3request: variant")
.Input("ha3result: variant")
.Output("ha3results: variant");

REGISTER_OP("Ha3QueryResourcePrepareOp")
.SetIsStateful()
.Input("ha3request: variant")
.Output("kvpair: variant")
.Output("aggregateclause: variant");

REGISTER_OP("AddDegradeInfoOp")
.SetIsStateful()
.Input("ha3request_input: variant")
.Output("ha3request_output: variant");

REGISTER_OP("Ha3HobbitPrepareOp")
.Input("ha3request: variant")
.Input("matchdocs: variant")
.Output("kvpair: variant")
.Output("matchdocs_out: variant");

REGISTER_OP("Ha3EndIdentityOp")
.SetIsStateful()
.Input("input: T")
.Output("output: T")
.Attr("T: type");

REGISTER_OP("Ha3AuxScanOp")
.Input("ha3request: variant")
.Output("matchdocs_out: variant");

REGISTER_OP("Ha3SeekAndJoinOp")
.Input("ha3request: variant")
.Input("aux_matchdocs: variant")
.Output("matchdocs_out: variant")
.Output("extrarankcount: uint32")
.Output("totalmatchdocs: uint32")
.Output("actualmatchdocs: uint32")
.Output("ha3aggresults: variant")
.Output("ha3errorresults: variant")
.Attr("self_define_type: string = \"Switch\"");

REGISTER_OP("NeedUnicornOp")
.Input("ha3request: variant")
.Output("is_need: bool");

REGISTER_OP("Ha3RequestToKvPairOp")
.Input("ha3request: variant")
.Output("ha3request_out: variant");

REGISTER_OP("ErrorResultMergeOp")
.Input("error_info: string")
.Attr("error_code: int = 1");

REGISTER_OP("Ha3CavaScorerOp")
.Input("query_kv_map: variant")
.Input("matchdocs: variant")
.Output("matchdocs_out: variant")
.Output("warning_out: string")
.Attr("enable_score_key: string")
.Attr("cava_scorer_name_key: string")
.Attr("score_ref_name: string")
.Attr("enable_batch: bool = true");

REGISTER_OP("Ha3ReleaseRedundantV2Op")
.Input("ha3request: variant")
.Input("matchdocs: variant")
.Output("matchdocs_out: variant");

REGISTER_OP("RunModelOp")
.Input("ha3request: variant")
.Input("matchdocs: variant")
.Output("matchdocs_out: variant")
.Attr("timeout_ratio: float = 0.7");

} // namespace turing
} // namespace isearch
