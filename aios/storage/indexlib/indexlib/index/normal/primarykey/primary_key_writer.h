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
#include "indexlib/document/document.h"
#include "indexlib/document/index_document/normal_document/normal_document.h"
#include "indexlib/index/normal/framework/index_writer.h"
#include "indexlib/indexlib.h"

namespace indexlib { namespace index {

class PrimaryKeyWriter : public IndexWriter
{
public:
    PrimaryKeyWriter() {}
    ~PrimaryKeyWriter() {}

public:
    void AddField(const document::Field* field) override {}
    virtual bool CheckPrimaryKeyStr(const std::string& str) const = 0;

private:
    IE_LOG_DECLARE();
};

typedef std::shared_ptr<PrimaryKeyWriter> PrimaryKeyWriterPtr;
}} // namespace indexlib::index
