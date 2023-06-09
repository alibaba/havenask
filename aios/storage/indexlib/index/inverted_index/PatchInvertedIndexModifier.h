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

#include "autil/Log.h"
#include "autil/NoCopyable.h"
#include "indexlib/config/TabletSchema.h"
#include "indexlib/framework/TabletData.h"
#include "indexlib/index/inverted_index/InvertedIndexModifier.h"
#include "indexlib/index/inverted_index/patch/InvertedIndexPatchWriter.h"

namespace indexlib::index {

class PatchInvertedIndexModifier final : public InvertedIndexModifier
{
public:
    PatchInvertedIndexModifier(const std::shared_ptr<indexlibv2::config::TabletSchema>& schema,
                               const std::shared_ptr<indexlib::file_system::IDirectory>& workDir);
    ~PatchInvertedIndexModifier() = default;

public:
    Status Init(const indexlibv2::framework::TabletData& tabletData);
    Status UpdateOneFieldTokens(docid_t docId, const document::ModifiedTokens& modifiedTokens, bool) override;
    Status Close();

private:
    std::shared_ptr<indexlib::file_system::IDirectory> _workDir;
    std::map<fieldid_t, std::vector<InvertedIndexPatchWriter*>> _fieldId2PatchWriters;
    std::vector<std::unique_ptr<InvertedIndexPatchWriter>> _patchWriters;
    std::vector<std::tuple<segmentid_t, docid_t /*baseDocId*/, docid_t /*end docid*/>> _segmentInfos;
    size_t _maxDocCount = 0;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlib::index
