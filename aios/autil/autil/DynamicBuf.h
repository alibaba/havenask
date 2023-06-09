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
#include <stdint.h>
#include <string.h>
#include <string>
#include <memory>


namespace autil {

class DynamicBuf
{
public:
    DynamicBuf(uint32_t initSize = 0, bool owns = true)
        : _size(initSize), _own(owns)
    {
        if (initSize > 0) {
            _buffer = _ptr = new char[initSize];
        } else {
            _buffer = _ptr = NULL;
        }
    }
    
    DynamicBuf(char* buf, uint32_t len, bool owns = false)
        : _size(len), _own(owns), _buffer(buf), _ptr(buf)
    {
    }
    
    ~DynamicBuf() {release();}
    const char* getPtr() const {return _ptr;}
    char* getPtr() {return _ptr;}
    void movePtr(size_t len) {_ptr += len;}
    char* getBuffer() {return _buffer;}
    const char* getBuffer() const {return _buffer;}
    uint32_t getTotalSize() const {return _size;}
    uint32_t getDataLen() const {return (uint32_t)(_ptr - _buffer);}

    uint32_t remaining() const { 
        return (uint32_t)(_size - (_ptr - _buffer));
    }

    void reserve(uint32_t length) {
        if (remaining() < length) {
            grow(getDataLen() + length);
        }
    }

    const char* add(const char* sourceStr, uint32_t length,  const char*& oldBuffer)
    {
        if (sourceStr == NULL || length == 0) {
            return NULL;
        }
        oldBuffer = ensure(length);
        memcpy(_ptr, sourceStr, length);
        char* ret = _ptr;
        _ptr += length;
        return ret; 
    }

    const char* add(const char* sourceStr, uint32_t length)
    {
        const char* notUsed;
        return add(sourceStr, length, notUsed);
    }

    char* alloc(uint32_t length) {
        if (length == 0)
            return NULL;
        ensure(length);
        char* rPtr = _ptr;
        _ptr += length;
        return rPtr;
    }

    const char* backspace(uint32_t length) {
        if (_ptr - length < _buffer) {
            _ptr = _buffer;
            return _buffer;
        }   
        _ptr -= length;
        return _ptr;
    }

    void reset() {_ptr = _buffer;}

    void release() {
        if (_own) {
            delete []_buffer;
        }
        _buffer = _ptr = NULL;
        _size = 0;
    }

    void serialize(std::string& str) const
    {
        uint32_t len = (uint32_t)(_ptr - _buffer);
        str.append((char*)&len, sizeof(len));
        str.append(_buffer, len);
    }

    int32_t deserialize(const char* str, uint32_t strLen)
    {
        if (strLen < sizeof(uint32_t)) {
            return -1;
        }

        uint32_t len;
        memcpy((char*)&len, str, sizeof(uint32_t));

        if (strLen - sizeof(uint32_t) < len) {
            return -1;
        }

        if (_size < len)  {
            if (_own && _buffer) {
                delete []_buffer;
            }
            _buffer = _ptr = new char[len];
            _size = len;
            _own = true;
        }

        memcpy(_buffer, str + sizeof(uint32_t), len);
        _ptr = _buffer + len;
        
        return _ptr - _buffer + sizeof(uint32_t);
    }

    /**
     * Ensure we have length memory available. Alloc length*3/2 for future use
     * @param length memory length needed
     */
    const char* ensure(uint32_t length) {
        if (remaining() < length) {
            // TODO: the size for enlarge to be decided
            return grow((getDataLen() + length)*3/2);
        }
        else {
            return _buffer;
        }
    }

private:
    const char* grow(uint32_t newSize) {
        assert(newSize > _size);
        const char* ret = _buffer;
        char* newBuf = new char[newSize];

        if (_buffer && (_ptr - _buffer) > 0)
            memcpy(newBuf, _buffer, _ptr - _buffer);

        _ptr = newBuf + (_ptr - _buffer);
        if (_own && _buffer)
           delete []_buffer;
        _buffer = newBuf;
        _size = newSize;
        _own  = true;

        return ret;
    }

private:
    uint32_t _size;
    bool _own;
    char* _buffer;
    char* _ptr; /*< current pointer pos */
};

typedef std::shared_ptr<DynamicBuf> DynamicBufPtr;

}

