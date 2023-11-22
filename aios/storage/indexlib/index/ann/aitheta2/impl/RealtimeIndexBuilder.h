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

#pragma once
#include "indexlib/index/ann/aitheta2/CommonDefine.h"
#include "indexlib/index/ann/aitheta2/util/AiThetaContextHolder.h"

namespace indexlibv2::index::ann {

class RealtimeIndexBuilder
{
public:
    RealtimeIndexBuilder(const AiThetaStreamerPtr& index, const IndexQueryMeta& queryMeta,
                         const AiThetaContextHolderPtr& holder)
        : _streamer(index)
        , _streamerName(_streamer->meta().streamer_name())
        , _queryMeta(queryMeta)
        , _contextHolder(holder)
    {
    }
    ~RealtimeIndexBuilder() = default;

public:
    bool Build(docid_t docId, const embedding_t& embedding);
    bool Delete(docid_t docId);

public:
    size_t GetIndexSize() const { return _streamer->stats().index_size(); }
    AiThetaStreamerPtr GetIndex() const { return _streamer; }

private:
    AiThetaStreamerPtr _streamer;
    std::string _streamerName;
    IndexQueryMeta _queryMeta;
    AiThetaContextHolderPtr _contextHolder;

    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<RealtimeIndexBuilder> RealtimeIndexBuilderPtr;
typedef std::unordered_map<index_id_t, RealtimeIndexBuilderPtr> RealtimeIndexBuilderMap;

} // namespace indexlibv2::index::ann
