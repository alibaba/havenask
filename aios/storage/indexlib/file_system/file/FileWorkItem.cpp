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
#include "indexlib/file_system/file/FileWorkItem.h"

#include <cstddef>

#include "fslib/common/common_type.h"
#include "fslib/fs/File.h"
#include "indexlib/file_system/file/BufferedFileOutputStream.h"
#include "indexlib/file_system/file/FileBuffer.h"
#include "indexlib/file_system/file/FileNode.h"

using namespace std;

namespace indexlib { namespace file_system {

WriteWorkItem::WriteWorkItem(BufferedFileOutputStream* stream, FileBuffer* fileBuffer)
    : _stream(stream)
    , _fileBuffer(fileBuffer)
{
}

WriteWorkItem::~WriteWorkItem() {}

void WriteWorkItem::process() noexcept(false)
{
    _stream->Write(_fileBuffer->GetBaseAddr(), _fileBuffer->GetCursor()).GetOrThrow();
}

void WriteWorkItem::destroy()
{
    _fileBuffer->SetCursor(0);
    _fileBuffer->Notify();
    delete this;
}

void WriteWorkItem::drop() { destroy(); }

// read
ReadWorkItem::ReadWorkItem(FileNode* fileNode, FileBuffer* fileBuffer, size_t offset, ReadOption option)
    : _fileNode(fileNode)
    , _fileBuffer(fileBuffer)
    , _offset(offset)
    , _option(option)
{
}

ReadWorkItem::~ReadWorkItem() {}

void ReadWorkItem::process() noexcept(false)
{
    size_t size =
        _fileNode->Read(_fileBuffer->GetBaseAddr(), _fileBuffer->GetBufferSize(), _offset, _option).GetOrThrow();
    _fileBuffer->SetCursor(size);
}

void ReadWorkItem::destroy()
{
    _fileBuffer->Notify();
    delete this;
}

void ReadWorkItem::drop() { destroy(); }
}} // namespace indexlib::file_system
