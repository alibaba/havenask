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
#include "indexlib/common_define.h"
#include "indexlib/index/inverted_index/InvertedIndexReader.h"
#include "indexlib/index/kkv/kkv_reader.h"
#include "indexlib/index/normal/attribute/accessor/attribute_reader.h"
#include "indexlib/index/normal/attribute/accessor/pack_attribute_reader.h"
#include "indexlib/partition/index_partition.h"
#include "indexlib/partition/index_partition_reader.h"
#include "tools/partition_querier/executors/AttrHelper.h"

namespace indexlib::tools {

class KkvTableExecutor
{
    typedef indexlib::common::AttributeReference AttributeReference;
    typedef indexlib::common::PackAttributeFormatterPtr PackAttributeFormatterPtr;
    typedef indexlib::index::KKVIterator KKVIterator;
    typedef indexlib::partition::IndexPartitionPtr IndexPartitionPtr;
    typedef indexlib::partition::IndexPartitionReaderPtr IndexPartitionReaderPtr;

public:
    static Status QueryKkvTable(const IndexPartitionReaderPtr& indexPartitionReaderPtr, regionid_t regionId,
                                const indexlibv2::base::PartitionQuery& query,
                                indexlibv2::base::PartitionResponse& partitionResponse);

private:
    static std::tuple<std::vector<std::string>, std::string>
    ValidateAttrs(const indexlib::config::KKVIndexConfigPtr& kkvIndexConfig,
                  const indexlib::config::PackAttributeConfigPtr& packAttrConfig,
                  const std::vector<std::string>& attrs);

    static void FormatKkvResults(KKVIterator* kkvIterator, const PackAttributeFormatterPtr& formater,
                                 const std::vector<std::string>& attrNames, const std::string& skFieldName,
                                 const FieldType& skFieldType, const int64_t limit,
                                 indexlibv2::base::PartitionResponse& partitionResponse);
};

} // namespace indexlib::tools
