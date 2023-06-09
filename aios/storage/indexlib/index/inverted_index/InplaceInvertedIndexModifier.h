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

#include "indexlib/config/TabletSchema.h"
#include "indexlib/framework/TabletData.h"
#include "indexlib/index/inverted_index/IInvertedDiskIndexer.h"
#include "indexlib/index/inverted_index/IInvertedMemIndexer.h"
#include "indexlib/index/inverted_index/InvertedIndexMetrics.h"
#include "indexlib/index/inverted_index/InvertedIndexModifier.h"
#include "indexlib/index/inverted_index/InvertedIndexerOrganizerUtil.h"
#include "indexlib/index/inverted_index/config/InvertedIndexConfig.h"
#include "indexlib/index/inverted_index/patch/IndexUpdateTermIterator.h"

namespace indexlib::index {

class InplaceInvertedIndexModifier final : public InvertedIndexModifier
{
public:
    InplaceInvertedIndexModifier(const std::shared_ptr<indexlibv2::config::TabletSchema>& schema);
    ~InplaceInvertedIndexModifier() {}

public:
    Status Init(const indexlibv2::framework::TabletData& tabletData);

    // 为什么需要这么多不同接口：
    // 1、关于 ModifiedTokens和IndexUpdateTermIterator
    // 倒排相对于正排不同的地方在于：可更新正排一个Field对应一个Attribute，这样更新只需要一个方法。
    // 而倒排，一个Field会对应到多个Index，那么在构建/OpLog回放链路上的更新，都是Field维度的ModifiedTokens；
    //         但LoadPatch链路上，Patch文件是在每个Index中，只有Index维度的IndexUpdateTermIterator
    // 2、关于 isForReplay
    // 倒排实时Dump只是把DynamicTree文件管理权从MemIndexer交给了DiskIndexer，Dump过程中依然在实时构建，
    // 所以在线OpLog回放链路上无需对实时的Segment进行更新
    // 其实由于 redo 和 build 在不同线程，也不能对倒排的实时部分进行 redo.
    Status Update(indexlibv2::document::IDocumentBatch* docBatch);
    Status UpdateOneFieldTokens(docid_t docId, const document::ModifiedTokens& modifiedTokens,
                                bool isForReplay) override;
    bool UpdateOneIndexTermsForReplay(IndexUpdateTermIterator* termIter);

private:
    void InitFieldMetrics(const std::shared_ptr<InvertedIndexMetrics>& indexMetrics);

private:
    IndexerOrganizerMeta _indexerOrganizerMeta;

    std::map<segmentid_t, std::pair<docid_t, uint64_t>> _segmentId2BaseDocIdAndDocCountPair;

    std::map<indexid_t, SingleInvertedIndexBuildInfoHolder> _buildInfoHolders;

private:
    AUTIL_LOG_DECLARE();
};
} // namespace indexlib::index
