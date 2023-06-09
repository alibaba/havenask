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
#include "indexlib/file_system/EntryMeta.h"

#include <map>
#include <ostream>

#include "autil/legacy/any.h"

using namespace std;

namespace indexlib { namespace file_system {
AUTIL_LOG_SETUP(indexlib.file_system, EntryMeta);

EntryMeta::EntryMeta()
    : _offset(-1)
    , _reserved(0)
    , _isNoEnt(false)
    , _isMutable(false)
    , _isOwner(false)
    , _isMemFile(false)
    , _isLazy(false)
{
}

EntryMeta::EntryMeta(const std::string& logicalPath, const std::string& physicalPath, const std::string* physicalRoot)
    : _logicalPath(logicalPath)
    , _physicalRoot(physicalRoot)
    , _rawPhysicalRoot(physicalRoot)
    , _offset(-1)
    , _reserved(0)
    , _isNoEnt(false)
    , _isMutable(false)
    , _isOwner(false)
    , _isMemFile(false)
    , _isLazy(false)
{
    SetPhysicalPath(physicalPath);
    // assert(!logicalPath.empty() && !physicalPath.empty() && physicalRoot);
}

EntryMeta::EntryMeta(const EntryMeta& other)
    : _logicalPath(other._logicalPath)
    , _physicalRoot(other._physicalRoot)
    , _rawPhysicalRoot(other._rawPhysicalRoot)
    , _length(other._length)
    , _unionAttribute(other._unionAttribute)
{
    SetPhysicalPath(other.GetPhysicalPath());
}

EntryMeta& EntryMeta::operator=(const EntryMeta& other)
{
    assert(sizeof(EntryMeta) ==
           sizeof(autil::legacy::Jsonizable) + sizeof(string) + sizeof(std::string*) * 3 + sizeof(int64_t) * 2);
    if (this == &other) {
        return *this;
    }
    _logicalPath = other._logicalPath;
    SetPhysicalPath(other.GetPhysicalPath());
    _physicalRoot = other._physicalRoot;
    _rawPhysicalRoot = other._rawPhysicalRoot;
    _length = other._length;
    _unionAttribute = other._unionAttribute;
    return *this;
}

void EntryMeta::Accept(const EntryMeta& other)
{
    auto raw = _rawPhysicalRoot;
    EntryMeta::operator=(other);
    if (raw) {
        _rawPhysicalRoot = raw;
    }
    return;
}

EntryMeta::~EntryMeta()
{
    if (_physicalPath) {
        delete _physicalPath;
    }
}

bool EntryMeta::CompatibleWith(const EntryMeta& other, bool ignorePerssion) const
{
    return _length == other._length && (IsDir() || (!IsDir() && _offset == other._offset)) &&
           _isNoEnt == other._isNoEnt && (ignorePerssion || _isMutable == other._isMutable) &&
           (ignorePerssion || _isOwner == other._isOwner) && _logicalPath == other._logicalPath &&
           (IsDir() || GetFullPhysicalPath() == other.GetFullPhysicalPath());
}

void EntryMeta::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json)
{
    json.Jsonize("length", _length, _length);
    if (json.GetMode() == FROM_JSON) {
        int64_t offset = _offset;
        json.Jsonize("offset", offset, offset);
        _offset = offset;
        map<string, autil::legacy::Any> jsonMap = json.GetMap();
        if (jsonMap.find("path") != jsonMap.end()) {
            if (!_physicalPath) {
                _physicalPath = new string();
            }
            json.Jsonize("path", *_physicalPath);
        }
    } else {
        if (_physicalPath) {
            json.Jsonize("path", *_physicalPath);
        }
        if (_offset >= 0) {
            int64_t offset = _offset;
            json.Jsonize("offset", offset);
        }
    }
}

std::string EntryMeta::DebugString() const
{
    std::stringstream ss;
    ss << "logical path[" << GetLogicalPath() << "], "
       << "physical path[" << GetPhysicalPath() << "], ";
    ss << "full physical path[" << GetFullPhysicalPath() << "], ";
    if (GetPhysicalRoot() != GetRawPhysicalRoot()) {
        ss << "raw physical root[" << GetRawPhysicalRoot() << "], ";
    }
    ss << "IsDir [" << IsDir() << "], IsOwner [" << IsOwner() << "], IsMutable [" << IsMutable() << "], ";
    ss << "IsMemFile [" << IsMemFile() << "], "
       << "IsLazy [" << IsLazy() << "]";
    if (!IsDir()) {
        ss << ", IsInPackage [" << IsInPackage() << "]";
        ss << ", Length [" << GetLength() << "], Offset [" << GetOffset() << "]";
    }
    return ss.str();
}
}} // namespace indexlib::file_system
