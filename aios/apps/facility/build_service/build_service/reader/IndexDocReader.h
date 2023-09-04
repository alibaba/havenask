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

#include "build_service/common_define.h"
#include "build_service/reader/RawDocumentReader.h"
#include "build_service/util/Log.h"
#include "indexlib/file_system/FileSystemOptions.h"
#include "indexlib/framework/ITabletDocIterator.h"
#include "indexlib/framework/ITabletExporter.h"
#include "indexlib/framework/ITabletFactory.h"
#include "indexlib/framework/MetricsManager.h"
#include "indexlib/framework/ResourceMap.h"
#include "indexlib/framework/Tablet.h"
#include "indexlib/framework/TabletDataSchemaGroup.h"
#include "indexlib/framework/Version.h"

namespace build_service { namespace reader {

/*
"index_version_info_path" version info input file formate, it will get version for each part
{
    "version_id_mapping":
    {
        "0_32767": 536870912,
        "32768_65535": 536870914
    }
}
*/
class IndexDocReader : public RawDocumentReader
{
public:
    IndexDocReader() = default;
    virtual ~IndexDocReader() = default;

    IndexDocReader(const IndexDocReader&) = delete;
    IndexDocReader& operator=(const IndexDocReader&) = delete;
    IndexDocReader(IndexDocReader&&) = delete;
    IndexDocReader& operator=(IndexDocReader&&) = delete;

public:
    bool init(const ReaderInitParam& params) override;
    bool seek(const Checkpoint& checkpoint) override;
    bool isEof() const override;
    RawDocumentReader::ErrorCode getNextRawDoc(document::RawDocument& rawDoc, Checkpoint* checkpoint,
                                               DocInfo& docInfo) override;

protected:
    // partition root path; rangeRatio, versionId
    using IterParam = std::tuple<std::string, std::pair<uint32_t, uint32_t>, versionid_t>;
    indexlib::document::RawDocumentParser* createRawDocumentParser(const ReaderInitParam& params) override;
    ErrorCode readDocStr(std::string& docStr, Checkpoint* checkpoint, DocInfo& docInfo) override
    {
        assert(false);
        return ERROR_EXCEPTION;
    }
    void doFillDocTags(document::RawDocument& rawDoc) override;

private:
    // virtual for test
    virtual std::pair<bool, std::vector<IterParam>> prepareIterParams(const ReaderInitParam& params);

    // virtual for test
    virtual std::unique_ptr<indexlibv2::framework::ITabletDocIterator>
    createTabletDocIterator(indexlib::framework::ITabletExporter* tabletExporter, size_t segmentCount);

    std::shared_ptr<indexlibv2::config::TabletOptions>
    createTabletOptions(indexlib::framework::ITabletExporter* tabletExporter);

    indexlib::Status switchTablet(int32_t iterId);
    void setCheckpoint(const std::string& iterCkpt, Checkpoint* checkpoint);

    static std::optional<std::pair<uint32_t, uint32_t>> calculateRange(std::pair<uint32_t, uint32_t> readerRange,
                                                                       std::pair<uint32_t, uint32_t> indexRange);

private:
    static const std::string USER_REQUIRED_FIELDS;

    std::shared_ptr<indexlibv2::framework::Tablet> _currentTablet;
    std::unique_ptr<indexlibv2::framework::ITabletDocIterator> _iter;
    std::shared_ptr<indexlibv2::framework::MetricsManager> _metricsManager;
    std::vector<IterParam> _iterParams;
    ReaderInitParam _parameters;
    int32_t _currentIterId = -1;
    int64_t _checkpointDocCounter = 0;

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(IndexDocReader);

}} // namespace build_service::reader
