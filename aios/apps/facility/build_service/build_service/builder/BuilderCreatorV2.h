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

#include <memory>
#include <stdint.h>
#include <string>

#include "build_service/builder/BuilderV2.h"
#include "build_service/common_define.h"
#include "build_service/config/ResourceReader.h"
#include "build_service/proto/BasicDefs.pb.h"
#include "build_service/util/Log.h"
#include "indexlib/framework/ITablet.h"

namespace build_service::builder {

class BuilderCreatorV2
{
public:
    explicit BuilderCreatorV2(std::shared_ptr<indexlibv2::framework::ITablet> tablet = nullptr);
    virtual ~BuilderCreatorV2();

    BuilderCreatorV2(const BuilderCreatorV2&) = delete;
    BuilderCreatorV2& operator=(const BuilderCreatorV2&) = delete;

public:
    virtual std::unique_ptr<BuilderV2> create(const config::ResourceReaderPtr& resourceReader,
                                              const KeyValueMap& kvMaps, const proto::PartitionId& partitionId,
                                              std::shared_ptr<indexlib::util::MetricProvider> metricProvider);

private:
    std::shared_ptr<indexlibv2::framework::ITablet> _tablet;
    proto::PartitionId _partitionId;
    int32_t _workerPathVersion;
    std::string _indexRootPath;

private:
    BS_LOG_DECLARE();
};

} // namespace build_service::builder
