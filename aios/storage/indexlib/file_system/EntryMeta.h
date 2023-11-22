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

#include <assert.h>
#include <memory>
#include <stdint.h>
#include <string>

#include "autil/CommonMacros.h"
#include "autil/Log.h"
#include "autil/legacy/jsonizable.h"
#include "indexlib/util/PathUtil.h"

namespace indexlib { namespace file_system {

class EntryMeta : public autil::legacy::Jsonizable
{
public:
    EntryMeta();
    EntryMeta(const std::string& logicalPath, const std::string& physicalPath, const std::string* physicalRoot);
    EntryMeta(const EntryMeta& other);
    ~EntryMeta();
    EntryMeta& operator=(const EntryMeta& other);
    bool CompatibleWith(const EntryMeta& other, bool ignorePerssion = false) const;
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;

public:
    static EntryMeta Dir(const std::string& logicalPath, const std::string& physicalPath,
                         const std::string* physicalRoot);
    static EntryMeta NoEnt(const std::string& logicalPath, const std::string& physicalPath,
                           const std::string* physicalRoot);

private:
    enum {
        LENGTH_UNKNOWN = -1,
        LENGTH_DIR = -2,
    };

public:
    bool IsFile() const { return _length >= 0; }
    bool IsDir() const { return _length == LENGTH_DIR; }
    bool IsNoEnt() const { return _isNoEnt; }
    bool IsInPackage() const { return _offset >= 0; } // TODO
    bool IsOwner() const { return _isOwner; }
    bool IsMutable() const { return _isMutable; }
    bool IsMemFile() const { return _isMemFile; }
    bool IsLazy() const { return _isLazy; }

    int64_t GetLength() const
    {
        assert(IsFile());
        return _length;
    }
    int64_t GetOffset() const { return _offset; }
    const std::string& GetLogicalPath() const { return _logicalPath; }
    const std::string& GetPhysicalPath() const { return _physicalPath ? *_physicalPath : _logicalPath; }
    const std::string& GetPhysicalRoot() const
    {
        assert(_physicalRoot);
        return *_physicalRoot;
    }
    const std::string& GetRawPhysicalRoot() const
    {
        assert(_rawPhysicalRoot);
        return *_rawPhysicalRoot;
    }
    const std::string* GetPhysicalRootPointer() const { return _physicalRoot; }
    std::string GetFullPhysicalPath() const;
    std::string GetRawFullPhysicalPath() const;

public:
    void SetNoEnt() { _isNoEnt = true; }
    void SetDir() { _length = LENGTH_DIR; }
    void SetLength(int64_t length)
    {
        assert(length >= LENGTH_DIR);
        _length = length;
    }
    void SetMutable(bool isMutable) { _isMutable = isMutable; }
    void SetOwner(bool isOwner) { _isOwner = isOwner; }
    void SetOffset(int64_t offset) { _offset = offset; }
    void SetLogicalPath(const std::string& logicalPath) { _logicalPath = logicalPath; }
    void SetPhysicalPath(const std::string& physicalPath);
    void SetPhysicalRoot(const std::string* physicalRoot)
    {
        assert(physicalRoot);
        _physicalRoot = physicalRoot;
        if (!_rawPhysicalRoot) {
            _rawPhysicalRoot = physicalRoot;
        }
    }
    void SetIsMemFile(bool isMemFile) { _isMemFile = isMemFile; }
    void SetLazy(bool isLazy) { _isLazy = isLazy; }
    void Accept(const EntryMeta& other);

public:
    std::string DebugString() const;

private:
    std::string _logicalPath;                   // normalized relative path
    std::string* _physicalPath = nullptr;       // normalized relative path:
                                                //  for exist-on-disk file, it's the actual physical path,
                                                //  for in-mem file, it's not-in-package path
    const std::string* _physicalRoot = nullptr; // entry table need guarantee its life cycle
    const std::string* _rawPhysicalRoot =
        nullptr; // entry table need guarantee its life cycle, before redirectPhysicalRoot, for toJson
    int64_t _length = 0;
    union {
        struct {
            int64_t _offset     : 52; // JSON, >=0 when in package
            uint64_t _reserved  : 7;  // MEM, reserved bits
            uint64_t _isNoEnt   : 1;  // MEM, 1 for no entry
            uint64_t _isMutable : 1;  // MEM, for dirs 1 means can should put new sub entry in outputRoot instead of
                                      // patchRoot, for files 1 means is created by myself
            uint64_t _isOwner   : 1;  // MEM, 1 for owner, use for delete
            uint64_t _isMemFile : 1;  // MEM, 1 for not dump
            uint64_t _isLazy    : 1; // MEM, only for dir, 1 means some subpaths may exist on disk but not in EntryTable
        };
        // _isMemFile=1, _isOwner=1, _isMutable=0, _isNoEnt=1, _reserved=0xff, _offset=7 ==> 0xdff0000000000007L
        uint64_t _unionAttribute = 0;
    };

private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<EntryMeta> EntryMetaPtr;

//////////////////////////////////////////////////////////////////////
inline EntryMeta EntryMeta::Dir(const std::string& logicalPath, const std::string& physicalPath,
                                const std::string* physicalRoot)
{
    EntryMeta entryMeta(logicalPath, physicalPath, physicalRoot);
    entryMeta.SetDir();
    return entryMeta;
}

inline EntryMeta EntryMeta::NoEnt(const std::string& logicalPath, const std::string& physicalPath,
                                  const std::string* physicalRoot)
{
    EntryMeta entryMeta(logicalPath, physicalPath, physicalRoot);
    entryMeta.SetNoEnt();
    return entryMeta;
}

inline std::string EntryMeta::GetFullPhysicalPath() const
{
    // TODO: Enable the following assert, or set physical path correctly for dir typed metas.
    // assert(!IsDir());
    if (unlikely(!_physicalRoot)) {
        return "null/" + GetPhysicalPath();
    }
    return util::PathUtil::JoinPath(*_physicalRoot, GetPhysicalPath());
}

inline std::string EntryMeta::GetRawFullPhysicalPath() const
{
    // TODO: Enable the following assert, or set physical path correctly for dir typed metas.
    // assert(!IsDir());
    if (unlikely(!_rawPhysicalRoot)) {
        return "null/" + GetPhysicalPath();
    }
    return util::PathUtil::JoinPath(*_rawPhysicalRoot, GetPhysicalPath());
}

inline void EntryMeta::SetPhysicalPath(const std::string& physicalPath)
{
    if (_logicalPath == physicalPath) {
        if (_physicalPath) {
            delete _physicalPath;
        }
        _physicalPath = nullptr;
    } else if (!_physicalPath) {
        _physicalPath = new std::string(physicalPath);
    } else {
        *_physicalPath = physicalPath;
    }
}
}} // namespace indexlib::file_system
