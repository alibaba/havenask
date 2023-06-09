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
#include <string>
#include "indexlib/partition/partition_reader_snapshot.h"
#include "autil/Log.h" // IWYU pragma: keep
#include "ha3/search/LayerMetas.h"
#include "indexlib/misc/common.h"
#include "tensorflow/core/framework/op_kernel.h"
#include "indexlib/partition/index_partition.h"
#include "ha3/common/QueryLayerClause.h"
namespace isearch {
namespace turing {

class LayerMetasCreateOp : public tensorflow::OpKernel
{
public:
    explicit LayerMetasCreateOp(tensorflow::OpKernelConstruction* ctx)
        : tensorflow::OpKernel(ctx)
    {}
public:
    void Compute(tensorflow::OpKernelContext* ctx) override;
public:
    static search::LayerMetasPtr createLayerMetas(
            indexlib::partition::PartitionReaderSnapshot *indexSnapshot,
            const std::string &mainTableName,
            autil::mem_pool::Pool *pool,
            common::QueryLayerClause* queryLayerClause);
    static void updateQuota(search::LayerMetasPtr &layerMetas);

private:
    AUTIL_LOG_DECLARE();
};
} // namespace turing
} // namespace isearch
