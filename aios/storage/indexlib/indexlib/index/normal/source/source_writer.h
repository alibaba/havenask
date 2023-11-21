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

#include "autil/StringUtil.h"
#include "indexlib/common_define.h"
#include "indexlib/config/source_schema.h"
#include "indexlib/document/index_document/normal_document/serialized_source_document.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/index/normal/source/in_mem_source_segment_reader.h"
#include "indexlib/indexlib.h"
#include "indexlib/util/memory_control/BuildResourceMetrics.h"

namespace indexlib { namespace index {

class SourceWriter
{
public:
    SourceWriter() {}
    virtual ~SourceWriter() {}

public:
    virtual void Init(const config::SourceSchemaPtr& sourceSchema,
                      util::BuildResourceMetrics* buildResourceMetrics) = 0;
    virtual void AddDocument(const document::SerializedSourceDocumentPtr& document) = 0;
    virtual void Dump(const file_system::DirectoryPtr& dir, autil::mem_pool::PoolBase* dumpPool) = 0;
    virtual const InMemSourceSegmentReaderPtr CreateInMemSegmentReader() = 0;

    void SetTemperatureLayer(const std::string& temperature) { mTemperatureLayer = temperature; }

protected:
    std::string mTemperatureLayer;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(SourceWriter);
}} // namespace indexlib::index
