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

#include <set>
#include <string>

#include "autil/Log.h"
#include "autil/legacy/jsonizable.h"
#include "indexlib/config/IIndexConfig.h"
#include "indexlib/index/orc/Common.h"
#include "indexlib/index/orc/OrcGeneralOptions.h"
#include "indexlib/index/orc/OrcReaderOptions.h"
#include "indexlib/index/orc/OrcRowReaderOptions.h"
#include "indexlib/index/orc/OrcWriterOptions.h"
#include "orc/Reader.hh"
#include "orc/Writer.hh"

namespace indexlibv2::config {

class OrcConfig final : public IIndexConfig
{
public:
    OrcConfig();
    explicit OrcConfig(const std::vector<std::shared_ptr<FieldConfig>>& fieldConfigs);
    OrcConfig(const std::string& indexName, const std::vector<std::shared_ptr<FieldConfig>>& fieldConfigs);
    OrcConfig(const std::string& indexName, const std::vector<std::shared_ptr<FieldConfig>>& fieldConfigs,
              const OrcWriterOptions& writerOptions, const OrcReaderOptions& readerOptions,
              const OrcRowReaderOptions& rowReaderOptions, const OrcGeneralOptions& generalOptions);

public:
    const std::string& GetIndexType() const override { return index::ORC_INDEX_TYPE_STR; }
    const std::string& GetIndexName() const override { return _indexName; }
    std::vector<std::string> GetIndexPath() const override;
    std::vector<std::shared_ptr<FieldConfig>> GetFieldConfigs() const override { return _fieldConfigs; }
    size_t GetRowGroupSize() const { return _generalOptions.GetRowGroupSize(); }
    void SetRowGroupSize(size_t rowGroupSize) { return _generalOptions.SetRowGroupSize(rowGroupSize); }

    size_t GetFieldId(const std::string& name) const;
    FieldConfig* GetFieldConfig(const std::string& name) const;
    orc::WriterOptions GetWriterOptions() const { return _writerOptions; }
    orc::ReaderOptions GetReaderOptions() const { return _readerOptions; }
    orc::RowReaderOptions GetRowReaderOptions() const { return _rowReaderOptions; }
    OrcGeneralOptions GetOrcGeneralOptions() const { return _generalOptions; }
    std::string DebugString() const;
    void Check() const override;
    void SetLegacyWriterOptions();
    void Deserialize(const autil::legacy::Any& any, size_t idxInJsonArray,
                     const config::IndexConfigDeserializeResource& resource) override;
    void Serialize(autil::legacy::Jsonizable::JsonWrapper& json) const override;
    Status CheckCompatible(const IIndexConfig* other) const override;

    static const std::string& GetOrcFileName();

private:
    std::string _indexName;
    std::vector<std::shared_ptr<FieldConfig>> _fieldConfigs;
    OrcWriterOptions _writerOptions;
    OrcReaderOptions _readerOptions;
    OrcRowReaderOptions _rowReaderOptions;
    OrcGeneralOptions _generalOptions;
    std::map<std::string, uint64_t> _fieldName2Id;

private:
    static const std::string ORC_FILE_NAME;
    static const std::set<FieldType> SUPPORTED_FIELD_TYPES;

    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::config
