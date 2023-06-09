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
class KKVIterator;
} // namespace indexlibv2::index

namespace indexlibv2::config {
class IIndexConfig;
}

namespace indexlibv2::table {
class KKVReader;
struct KKVReadOptions;

class KKVTabletReader : public framework::TabletReader
{
public:
    KKVTabletReader(const std::shared_ptr<config::TabletSchema>& schema) : TabletReader(schema) {}
    ~KKVTabletReader() = default;

    Status Open(const std::shared_ptr<framework::TabletData>& tabletData,
                const framework::ReadResource& readResource) override;

public:
    // for ut or debug
    Status QueryIndex(const std::shared_ptr<config::IIndexConfig>& indexConfig, const base::PartitionQuery& query,
                      base::PartitionResponse& partitionResponse) const;

private:
    Status FillRows(const std::shared_ptr<KKVReader>& kkvReader, std::vector<std::string>& attrs,
                    const autil::StringView& pkey, const std::vector<autil::StringView>& skeys,
                    const std::shared_ptr<indexlibv2::index::PackAttributeFormatter>& packAttrFormatter,
                    base::PartitionResponse& partitionResponse) const;
    void FillRow(const autil::StringView& pkey, const std::shared_ptr<KKVReader>& kkvReader, index::KKVIterator* iter,
                 indexlibv2::table::KKVReadOptions& readOptions, const std::vector<std::string>& attrs,
                 const std::shared_ptr<indexlibv2::index::PackAttributeFormatter>& packAttrFormatter,
                 base::PartitionResponse& partitionResponse) const;
    std::vector<std::string> ValidateAttrs(const std::shared_ptr<KKVReader>& kkvReader,
                                           const std::vector<std::string>& inputAttrs) const;

private:
    static void GetKkvSkeyValue(uint64_t value, FieldType fieldType, indexlibv2::base::AttrValue& attrValue);

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::table
