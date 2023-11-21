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

#include <assert.h>
#include <memory>
#include <string>

#include "autil/legacy/any.h"
#include "build_service/common_define.h"
#include "build_service/config/TaskInputConfig.h"
#include "build_service/io/IODefine.h"
#include "build_service/io/Input.h"
#include "build_service/io/InputCreator.h"
#include "build_service/util/Log.h"
#include "indexlib/partition/remote_access/partition_resource_provider.h"
#include "indexlib/partition/remote_access/partition_seeker.h"

namespace build_service { namespace io {

class IndexlibIndexInput : public Input
{
public:
    static const std::string INDEX_ROOT;
    static const std::string PARTITION_RANGE_FROM;
    static const std::string PARTITION_RANGE_TO;
    static const std::string PLUGIN_PATH;
    static const std::string GENERATION_ID;
    static const std::string CLUSTER_NAME;
    static const std::string INDEX_PARTITION_OPTIONS;

public:
    IndexlibIndexInput(const config::TaskInputConfig& inputConfig);
    ~IndexlibIndexInput();

private:
    IndexlibIndexInput(const IndexlibIndexInput&);
    IndexlibIndexInput& operator=(const IndexlibIndexInput&);

public:
    bool init(const KeyValueMap& params) override;
    std::string getType() const override { return INDEXLIB_INDEX; }
    ErrorCode read(autil::legacy::Any& any) override
    {
        assert(false);
        return EC_ERROR;
    }

    indexlib::partition::IndexPartitionReaderPtr getReader() const { return _provider->GetReader(); }

private:
    bool getIndexPath(const KeyValueMap& params, std::string& indexPath);

private:
    indexlib::partition::PartitionResourceProviderPtr _provider;

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(IndexlibIndexInput);

class IndexlibIndexInputCreator : public InputCreator
{
public:
    bool init(const config::TaskInputConfig& inputConfig) override
    {
        _taskInputConfig = inputConfig;
        return true;
    }

    InputPtr create(const KeyValueMap& params) override
    {
        InputPtr input(new IndexlibIndexInput(_taskInputConfig));
        if (input->init(params)) {
            return input;
        }
        return InputPtr();
    }

private:
    config::TaskInputConfig _taskInputConfig;
};

BS_TYPEDEF_PTR(IndexlibIndexInputCreator);
}} // namespace build_service::io
