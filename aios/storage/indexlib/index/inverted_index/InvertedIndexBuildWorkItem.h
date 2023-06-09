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

#include "indexlib/index/common/BuildWorkItem.h"
#include "indexlib/index/inverted_index/SingleInvertedIndexBuilder.h"

namespace indexlib::index {
// User of work item should ensure lifetime of indexer and data out live the work item.
// Each InvertedIndexBuildWorkItem maps to one inverted index builder, i.e. a single inverted index or one shard of a
// multi-shard inverted index.
class InvertedIndexBuildWorkItem : public BuildWorkItem
{
public:
    // For multi-shard work item, indexConfig should be of type IndexConfig::ISI_NEED_SHARDING.
    InvertedIndexBuildWorkItem(SingleInvertedIndexBuilder* builder,
                               indexlibv2::document::IDocumentBatch* documentBatch);
    ~InvertedIndexBuildWorkItem();

public:
    Status doProcess() override;

private:
    Status BuildOneDoc(indexlibv2::document::IDocument* iDoc);

private:
    SingleInvertedIndexBuilder* _builder;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlib::index
