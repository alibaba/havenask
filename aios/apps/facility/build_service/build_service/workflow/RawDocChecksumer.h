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
#include "build_service/config/ResourceReader.h"
#include "build_service/document/RawDocument.h"
#include "build_service/proto/BasicDefs.pb.h"
#include "build_service/util/Log.h"

namespace build_service { namespace workflow {

class RawDocChecksumer
{
public:
    RawDocChecksumer(const proto::PartitionId& pid, const config::ResourceReaderPtr& resourceReader);
    ~RawDocChecksumer() = default;

public:
    void recover(uint64_t checksum);
    bool ensureInited();
    void evaluate(document::RawDocumentPtr& rawDoc);
    bool finish();
    uint64_t getChecksum() const { return _checksum; }
    bool enabled() const { return _enabled; }

private:
    bool _inited = false;
    proto::PartitionId _pid;
    config::ResourceReaderPtr _resourceReader;

    bool _enabled = false;
    uint64_t _checksum = 0;
    std::string _checksumOutputFile;

private:
    BS_LOG_DECLARE();
};

}} // namespace build_service::workflow
