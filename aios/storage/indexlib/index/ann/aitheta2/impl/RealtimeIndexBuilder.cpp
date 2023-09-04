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

#include "indexlib/index/ann/aitheta2/impl/RealtimeIndexBuilder.h"

#include "autil/Scope.h"
using namespace std;
namespace indexlibv2::index::ann {

bool RealtimeIndexBuilder::Build(docid_t docId, const embedding_t& embedding)
{
    auto [_, context] = _contextHolder->CreateIfNotExist(_streamer, _streamer->meta().searcher_name(), "");
    ANN_CHECK(context != nullptr, "get context failed");

    auto ctx = AiThetaContext::Pointer(context);
    autil::ScopeGuard guard([&ctx]() { ctx.release(); });

    ANN_CHECK_OK(_streamer->add_impl(docId, embedding.get(), _indexQueryMeta, ctx), "build failed");
    return true;
}

bool RealtimeIndexBuilder::Delete(docid_t docId)
{
    auto [_, context] = _contextHolder->CreateIfNotExist(_streamer, _streamer->meta().searcher_name(), "");
    ANN_CHECK(context != nullptr, "create context failed");

    auto ctx = AiThetaContext::Pointer(context);
    autil::ScopeGuard guard([&ctx]() { ctx.release(); });

    ANN_CHECK_OK(_streamer->remove(docId, ctx), "delete failed");
    return true;
}

AUTIL_LOG_SETUP(indexlib.index, RealtimeIndexBuilder);
} // namespace indexlibv2::index::ann
