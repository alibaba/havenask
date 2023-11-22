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
#include "indexlib/base/FieldType.h"
#include "indexlib/base/Status.h"
#include "indexlib/base/proto/query.pb.h"
#include "indexlib/framework/TabletReader.h"

namespace indexlibv2::index {
class PackAttributeFormatter;
class KVIndexReader;
} // namespace indexlibv2::index

namespace indexlibv2::config {
class IIndexConfig;
}

namespace indexlibv2::table {

class KVTabletReader : public framework::TabletReader
{
public:
    KVTabletReader(const std::shared_ptr<config::ITabletSchema>& schema) : TabletReader(schema) {}
    ~KVTabletReader() = default;

public:
    Status DoOpen(const std::shared_ptr<framework::TabletData>& tabletData,
                  const framework::ReadResource& readResource) override;

public:
    /* protobuf query.proto:PartitionQuery to json
    jsonQuery format:
      {
         "pk" : [ "chaos", "hello_world" ],
         "pkNumber" : [ 123456, 623445 ],
         "attrs" : ["attr1", "attr2", ...],
         "indexName" : "kv1"
      }
     */
    Status Search(const std::string& jsonQuery, std::string& result) const override;

    Status QueryIndex(const std::shared_ptr<config::IIndexConfig>& indexConfig, const base::PartitionQuery& query,
                      base::PartitionResponse& partitionResponse) const;

private:
    static std::vector<std::string> ValidateAttrs(const std::shared_ptr<index::KVIndexReader>& kvReader,
                                                  const std::vector<std::string>& attrs);

    template <typename PkType>
    static Status QueryAttrWithPk(
        const std::shared_ptr<index::KVIndexReader>& kvReader, const PkType& pk, const std::vector<std::string>& attrs,
        const std::shared_ptr<indexlibv2::index::PackAttributeFormatter>& packAttributeFormatter, base::Row& row);

    static void FormatKVResult(const std::shared_ptr<index::KVIndexReader>& kvReader, const std::string& fieldName,
                               const autil::StringView& docValue,
                               const std::shared_ptr<indexlibv2::index::PackAttributeFormatter>& packAttributeFormatter,
                               base::Row& row);
    template <typename T>
    static Status FillRows(const std::shared_ptr<index::KVIndexReader>& kvReader, const std::vector<std::string>& attrs,
                           const std::vector<T>& pkList,
                           const std::shared_ptr<indexlibv2::index::PackAttributeFormatter>& packAttributeFormatter,
                           base::PartitionResponse& partitionResponse);
    static void GetKvValue(const autil::StringView& value, FieldType fieldType, indexlibv2::base::AttrValue& attrvalue);

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::table
