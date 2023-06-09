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

#include "indexlib/index/inverted_index/PostingWriter.h"
#include "indexlib/index/inverted_index/format/PostingFormatOption.h"

namespace indexlib::index {

class PostingDumper
{
public:
    PostingDumper() : _termPayload(0) {}
    ~PostingDumper() {}

public:
    void SetTermPayload(termpayload_t termPayload) { _termPayload = termPayload; }
    virtual uint64_t GetDumpLength() const = 0;
    virtual bool IsCompressedPostingHeader() const = 0;

    virtual std::shared_ptr<PostingWriter> GetPostingWriter() { return _postingWriter; }
    virtual void Dump(const std::shared_ptr<file_system::FileWriter>& postingFile) = 0;
    virtual df_t GetDocFreq() const { return _postingWriter->GetDF(); }
    virtual ttf_t GetTotalTF() const { return _postingWriter->GetTotalTF(); }
    termpayload_t GetTermPayload() const { return _termPayload; }

protected:
    std::shared_ptr<PostingWriter> _postingWriter;
    termpayload_t _termPayload;

    AUTIL_LOG_DECLARE();
};

} // namespace indexlib::index
