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

#include <map>
#include <memory>
#include <vector>

#include "indexlib/index/attribute/patch/AttributeUpdater.h"
#include "indexlib/index/common/patch/PatchWriter.h"

namespace indexlibv2::config {
class IIndexConfig;
}

namespace indexlibv2::index {

class AttributePatchWriter : public PatchWriter
{
public:
    AttributePatchWriter(std::shared_ptr<indexlib::file_system::IDirectory> workDir, segmentid_t srcSegmentId,
                         const std::shared_ptr<config::IIndexConfig>& indexConfig);
    ~AttributePatchWriter() = default;

public:
    Status Write(segmentid_t targetSegmentId, docid_t localDocId, std::string_view value, bool isNull);

public:
    Status Close() override;
    Status GetPatchFileInfos(PatchFileInfos* patchFileInfos) override;

private:
    std::shared_ptr<config::IIndexConfig> _indexConfig;
    std::map<segmentid_t, std::unique_ptr<AttributeUpdater>> _segmentId2Updaters;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::index
