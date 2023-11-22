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

#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"

DECLARE_REFERENCE_CLASS(index_base, PartitionData);
DECLARE_REFERENCE_CLASS(file_system, Directory);
DECLARE_REFERENCE_CLASS(config, AttributeConfig);

namespace indexlib { namespace index {

class AttributePatchReader
{
public:
    AttributePatchReader(const config::AttributeConfigPtr& attrConfig) : mAttrConfig(attrConfig) {}

    virtual ~AttributePatchReader() {}

public:
    virtual void Init(const index_base::PartitionDataPtr& partitionData, segmentid_t segmentId);

public:
    virtual void AddPatchFile(const file_system::DirectoryPtr& directory, const std::string& fileName,
                              segmentid_t srcSegmentId) = 0;

    virtual docid_t GetCurrentDocId() const = 0;
    virtual size_t GetPatchFileLength() const = 0;
    virtual size_t GetPatchItemCount() const = 0;

    virtual size_t Next(docid_t& docId, uint8_t* buffer, size_t bufferLen, bool& isNull) = 0;

    virtual size_t Seek(docid_t docId, uint8_t* value, size_t maxLen) = 0;

    virtual bool HasNext() const = 0;

    virtual uint32_t GetMaxPatchItemLen() const = 0;

protected:
    config::AttributeConfigPtr mAttrConfig;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(AttributePatchReader);

typedef AttributePatchReader AttributeSegmentPatchIterator;
typedef AttributePatchReaderPtr AttributeSegmentPatchIteratorPtr;
}} // namespace indexlib::index
