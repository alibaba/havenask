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
#include "indexlib/framework/index_task/IIndexOperationCreator.h"
#include "indexlib/framework/index_task/IndexOperationDescription.h"

namespace indexlibv2::table {

class KVTableTaskOperationCreator : public framework::IIndexOperationCreator
{
public:
    KVTableTaskOperationCreator(const std::shared_ptr<config::ITabletSchema>& schema);
    ~KVTableTaskOperationCreator();

    std::unique_ptr<framework::IndexOperation>
    CreateOperation(const framework::IndexOperationDescription& opDesc) override;

private:
    std::unique_ptr<framework::IndexOperation>
    CreateOperationForCommon(const framework::IndexOperationDescription& opDesc);
    std::unique_ptr<framework::IndexOperation>
    CreateOperationForMerge(const framework::IndexOperationDescription& opDesc);

private:
    std::shared_ptr<config::ITabletSchema> _schema;

    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::table
