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

#include "indexlib/base/Status.h"
#include "indexlib/base/proto/query.pb.h"
#include "indexlib/index/kv/kv_reader.h"
#include "indexlib/partition/index_partition.h"
#include "indexlib/partition/index_partition_reader.h"

namespace indexlib::tools {

class KvTableExecutor
{
    typedef indexlib::index::KVReaderPtr KVReaderPtr;
    typedef indexlib::partition::IndexPartitionPtr IndexPartitionPtr;
    typedef indexlib::partition::IndexPartitionReaderPtr IndexPartitionReaderPtr;

public:
    static Status QueryKVTable(const IndexPartitionReaderPtr& indexPartitionReaderPtr, regionid_t regionId,
                               const indexlibv2::base::PartitionQuery& query,
                               indexlibv2::base::PartitionResponse& partitionResponse);

private:
    static std::vector<std::string> ValidateAttrs(const KVReaderPtr& kvReader, const std::vector<std::string>& attrs);

    template <typename PkType>
    static Status QueryAttrWithPk(const KVReaderPtr& kvReader, const PkType& pk, const std::vector<std::string>& attrs,
                                  indexlibv2::base::Row& row);

    static void FormatKVResult(const KVReaderPtr& kvReader, const std::string& fieldName,
                               const autil::StringView& docValue, indexlibv2::base::Row& row);
    template <typename T>
    static Status FillRows(const KVReaderPtr& kvReader, const std::vector<std::string>& attrs,
                           const std::vector<T>& pkList, indexlibv2::base::PartitionResponse& partitionResponse);
};

} // namespace indexlib::tools
