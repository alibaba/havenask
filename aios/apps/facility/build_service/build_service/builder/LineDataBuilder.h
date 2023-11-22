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

#include <stdint.h>
#include <string>

#include "build_service/builder/Builder.h"
#include "build_service/common/Locator.h"
#include "build_service/common_define.h"
#include "build_service/config/BuilderConfig.h"
#include "build_service/util/Log.h"
#include "fslib/fs/FileLock.h"
#include "indexlib/base/Constant.h"
#include "indexlib/config/index_partition_options.h"
#include "indexlib/file_system/file/BufferedFileWriter.h"
#include "indexlib/file_system/fslib/FenceContext.h"
#include "indexlib/indexlib.h"
#include "indexlib/partition/index_builder.h"
#include "indexlib/util/counter/CounterMap.h"

namespace build_service { namespace builder {

class LineDataBuilder : public Builder
{
public:
    LineDataBuilder(const std::string& indexPath, const indexlib::config::IndexPartitionSchemaPtr& schema,
                    fslib::fs::FileLock* fileLock, const std::string& epochId);
    ~LineDataBuilder();

private:
    LineDataBuilder(const LineDataBuilder&);
    LineDataBuilder& operator=(const LineDataBuilder&);

public:
    bool init(const config::BuilderConfig& builderConfig,
              indexlib::util::MetricProviderPtr metricProvider = indexlib::util::MetricProviderPtr()) override;
    bool build(const indexlib::document::DocumentPtr& doc) override;
    void stop(int64_t stopTimestamp = INVALID_TIMESTAMP) override;
    bool merge(const indexlib::config::IndexPartitionOptions& options) override { return false; }
    int64_t getIncVersionTimestamp() const override { return -1; }
    bool getLastLocator(common::Locator& locator) const override
    {
        locator = common::Locator();
        return true;
    }
    bool getLatestVersionLocator(common::Locator& locator) const override { return false; }
    const indexlib::util::CounterMapPtr& GetCounterMap() override { return _counterMap; }
    virtual RET_STAT getIncVersionTimestampNonBlocking(int64_t& ts) const override
    {
        ts = -1;
        return RS_OK;
    }

    virtual RET_STAT getLastLocatorNonBlocking(common::Locator& locator) const override
    {
        locator = common::Locator();
        return RS_OK;
    }

public:
    // for test
    void setFileWriter(const indexlib::file_system::BufferedFileWriterPtr& writer) { _writer = writer; }

public:
    static bool storeSchema(const std::string& indexPath, const indexlib::config::IndexPartitionSchemaPtr& schema,
                            indexlib::file_system::FenceContext* fenceContext);

private:
    bool prepareOne(const indexlib::document::DocumentPtr& doc, std::string& oneLineDoc);

private:
    std::string _indexPath;
    indexlib::config::IndexPartitionSchemaPtr _schema;
    indexlib::file_system::BufferedFileWriterPtr _writer;
    std::string _fieldSeperator;
    indexlib::util::CounterMapPtr _counterMap;
    std::string _epochId;

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(LineDataBuilder);

}} // namespace build_service::builder
