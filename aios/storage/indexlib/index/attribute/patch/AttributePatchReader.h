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
#include "autil/NoCopyable.h"
#include "indexlib/base/Status.h"
#include "indexlib/base/Types.h"
#include "indexlib/index/attribute/patch/IAttributePatch.h"
#include "indexlib/index/common/patch/PatchFileInfos.h"

namespace indexlib::file_system {
class IDirectory;
}
namespace indexlibv2::config {
class AttributeConfig;
}
namespace indexlibv2::index {

class AttributePatchReader : public IAttributePatch
{
public:
    AttributePatchReader(const std::shared_ptr<config::AttributeConfig>& attrConfig) : _attrConfig(attrConfig) {}
    virtual ~AttributePatchReader() = default;

    virtual Status Init(const PatchInfos* patchInfos, segmentid_t segmentId);

    virtual Status AddPatchFile(const std::shared_ptr<indexlib::file_system::IDirectory>& dir,
                                const std::string& fileName, segmentid_t srcSegmentId) = 0;

    virtual docid_t GetCurrentDocId() const = 0;
    virtual size_t GetPatchFileLength() const = 0;
    virtual size_t GetPatchItemCount() const = 0;

    virtual std::pair<Status, size_t> Next(docid_t& docId, uint8_t* buffer, size_t bufferLen, bool& isNull) = 0;

    virtual bool HasNext() const = 0;

    bool UpdateField(docid_t docId, const autil::StringView& value, bool isNull) override
    {
        assert(false);
        return false;
    }

protected:
    std::shared_ptr<config::AttributeConfig> _attrConfig;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::index
