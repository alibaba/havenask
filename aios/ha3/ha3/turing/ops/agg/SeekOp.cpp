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
#include <algorithm>
#include <iosfwd>
#include <memory>
#include <type_traits>
#include <utility>
#include <vector>

#include "autil/Log.h" // IWYU pragma: keep
#include "autil/TimeoutTerminator.h"
#include "ha3/common/TimeoutTerminator.h"
#include "ha3/search/LayerMetas.h"
#include "ha3/turing/variant/SeekIteratorVariant.h"
#include "matchdoc/MatchDoc.h"
#include "matchdoc/MatchDocAllocator.h"
#include "matchdoc/ValueType.h"
#include "suez/turing/common/QueryResource.h"
#include "suez/turing/common/RuntimeState.h"
#include "suez/turing/common/SessionResource.h"
#include "tensorflow/core/framework/kernel_def_builder.h"
#include "tensorflow/core/framework/op_kernel.h"
#include "tensorflow/core/framework/tensor.h"
#include "tensorflow/core/framework/tensor_types.h"
#include "tensorflow/core/framework/types.h"
#include "tensorflow/core/framework/variant.h"
#include "tensorflow/core/lib/core/errors.h"
#include "tensorflow/core/lib/core/stringpiece.h"
#include "tensorflow/core/platform/byte_order.h"
#include "turing_ops_util/util/OpUtil.h"
#include "turing_ops_util/variant/MatchDocsVariant.h"
#include "turing_ops_util/variant/Tracer.h"

using namespace std;
using namespace tensorflow;
using namespace matchdoc;
using namespace suez::turing;

using namespace isearch::search;
using namespace isearch::common;

namespace isearch {
namespace turing {

class SeekOp : public tensorflow::OpKernel
{
public:
    explicit SeekOp(tensorflow::OpKernelConstruction* ctx) : tensorflow::OpKernel(ctx) {
        OP_REQUIRES_OK(ctx, ctx->GetAttr("batch_size", &_batchSize));
    }
public:
    void Compute(tensorflow::OpKernelContext* ctx) override {
        auto sessionResource = GET_SESSION_RESOURCE();
        auto queryResource = GET_QUERY_RESOURCE(sessionResource);
        auto seekIteratorVariant = ctx->input(0).scalar<Variant>()().get<SeekIteratorVariant>();
        OP_REQUIRES(ctx, seekIteratorVariant, errors::Unavailable("get seek iterator variant failed"));
        SeekIteratorPtr seekIterator = seekIteratorVariant->getSeekIterator();
        if (!seekIterator) {
            ctx->set_output(0, ctx->input(0));
            Tensor *matchDocsTensor = nullptr;
            MatchDocsVariant matchDocsVariant;
            OP_REQUIRES_OK(ctx, ctx->allocate_output(1, {}, &matchDocsTensor));
            matchDocsTensor->scalar<Variant>()() = move(matchDocsVariant);

            Tensor *eofTensor = nullptr;
            OP_REQUIRES_OK(ctx, ctx->allocate_output(2, {}, &eofTensor));
            eofTensor->scalar<bool>()() = true;
            return;
        }

        MatchDocAllocatorPtr allocator = seekIterator->getMatchDocAllocator();

        MatchDocsVariant matchDocsVariant(allocator, queryResource->getPool());
        vector<MatchDoc> &seekDocs = matchDocsVariant.getMatchDocs();
        bool eof = seekMatchDocs(seekIterator.get(), _batchSize, seekDocs);
        ctx->set_output(0, ctx->input(0));

        Tensor *matchDocsTensor = nullptr;
        OP_REQUIRES_OK(ctx, ctx->allocate_output(1, {}, &matchDocsTensor));
        matchDocsTensor->scalar<Variant>()() = move(matchDocsVariant);

        Tensor *eofTensor = nullptr;
        OP_REQUIRES_OK(ctx, ctx->allocate_output(2, {}, &eofTensor));
        eofTensor->scalar<bool>()() = eof;
  }
private:
    bool seekMatchDocs(SeekIterator *seekIterator, int batchSize, vector<MatchDoc> &seekDocs);

private:
    int _batchSize;
private:
    AUTIL_LOG_DECLARE();

};

bool SeekOp::seekMatchDocs(SeekIterator *seekIterator, int batchSize, vector<MatchDoc> &seekDocs) {
    if (batchSize <= 0) {
        while (true) {
            MatchDoc matchDoc = seekIterator->seek();
            if (matchdoc::INVALID_MATCHDOC == matchDoc) {
                return true;
            }
            seekDocs.push_back(matchDoc);
        }
    } else {
        return seekIterator->batchSeek(batchSize, seekDocs);
    }
}


AUTIL_LOG_SETUP(ha3, SeekOp);

REGISTER_KERNEL_BUILDER(Name("SeekOp")
                        .Device(DEVICE_CPU),
                        SeekOp);

} // namespace turing
} // namespace isearch
