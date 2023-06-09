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
#include "indexlib/index/deletionmap/SingleDeletionMapBuilder.h"

namespace indexlib::index {
// User of work item should ensure lifetime of indexer and data out live the work item.
// TODO(panghai.hj): Finalize PK lookup to get docid logic
class DeletionMapBuildWorkItem : public BuildWorkItem
{
public:
    DeletionMapBuildWorkItem(SingleDeletionMapBuilder* builder, indexlibv2::document::IDocumentBatch* documentBatch);
    ~DeletionMapBuildWorkItem();

public:
    Status doProcess() override;

private:
    SingleDeletionMapBuilder* _builder;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlib::index
