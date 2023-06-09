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

#include <stddef.h>

#include "autil/WorkItem.h"
#include "indexlib/file_system/file/ReadOption.h"

namespace indexlib { namespace file_system {

class BufferedFileOutputStream;
class FileBuffer;
class FileNode;

class WriteWorkItem : public autil::WorkItem
{
public:
    WriteWorkItem(BufferedFileOutputStream* stream, FileBuffer* fileBuffer);
    ~WriteWorkItem();

public:
    void process() noexcept(false) override;
    void destroy() override;
    void drop() override;

private:
    BufferedFileOutputStream* _stream;
    FileBuffer* _fileBuffer;
};

class ReadWorkItem : public autil::WorkItem
{
public:
    ReadWorkItem(FileNode* fileNode, FileBuffer* fileBuffer, size_t offset, ReadOption option);
    ~ReadWorkItem();

public:
    void process() noexcept(false) override;
    void destroy() override;
    void drop() override;

private:
    FileNode* _fileNode;
    FileBuffer* _fileBuffer;
    size_t _offset;
    ReadOption _option;
};
}} // namespace indexlib::file_system
