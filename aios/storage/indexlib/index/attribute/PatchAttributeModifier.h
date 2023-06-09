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
#include <vector>

#include "autil/Log.h"
#include "autil/StringView.h"
#include "indexlib/framework/TabletData.h"
#include "indexlib/index/attribute/AttributeModifier.h"
#include "indexlib/index/attribute/AttributePatchWriter.h"

namespace indexlibv2::index {

class PatchAttributeModifier : public AttributeModifier
{
public:
    PatchAttributeModifier(const std::shared_ptr<config::TabletSchema>& schema,
                           const std::shared_ptr<indexlib::file_system::IDirectory>& workDir);
    ~PatchAttributeModifier() = default;

public:
    Status Init(const framework::TabletData& tabletData) override;
    bool UpdateField(docid_t docId, fieldid_t fieldId, const autil::StringView& value, bool isNull) override;
    Status Close();

private:
    std::shared_ptr<indexlib::file_system::IDirectory> _workDir;
    std::map<fieldid_t, std::shared_ptr<AttributePatchWriter>> _patchWriters;
    std::vector<std::tuple<segmentid_t, docid_t /*baseDocId*/, docid_t /*end docid*/>> _segmentInfos;
    docid_t _maxDocCount = INVALID_DOCID;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::index
