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

#include <stdint.h>
#include <string.h>
#include <algorithm>
#include <iostream>
#include <iterator>
#include <string>

namespace autil {

class ShortString
{
public:
    typedef char* iterator;
    typedef const char* const_iterator;
public:
    ShortString() {init();}
    ShortString(const char *s) {init(s, s + strlen(s));}
    ShortString(const char *s, uint32_t len) {init(s, s + len);}
    ShortString(const std::string &s) {init(s.begin(), s.end());}
    ShortString(const ShortString &s) {init(s.begin(), s.end());}
    template<typename InputIterator>
    ShortString(InputIterator begin, InputIterator end) {init(begin, end);}
    ~ShortString() {deleteData();}
public:
    ShortString& operator = (const char* s) {return assign(s);}
    ShortString& operator = (const std::string &s) {return assign(s);}
    ShortString& operator = (const ShortString &s) {return assign(s);}
    ShortString& operator += (const ShortString &addStr);
public:
    ShortString& assign(const char* s) {return assign(s, s + strlen(s));}
    ShortString& assign(const std::string &s) {return assign(s.begin(), s.end());}
    ShortString& assign(const ShortString &s) {
        if (this == &s) {return *this;}
        return assign(s.begin(), s.end());
    }
    ShortString& assign(const char *s, uint32_t len) {
        return assign(s, s + len);
    }
    template<typename InputIterator>
    ShortString& assign(InputIterator begin, InputIterator end) {
        deleteData();
        init(begin, end);
        return *this;
    }
public:
    bool empty() const {return _size == 0;}
    size_t size() const {return _size;}
    size_t length() const {return size();}
    iterator data() {return _data;}
    const_iterator data() const {return _data;}
    const char* c_str() const {return _data;}
    iterator begin() {return data();}
    iterator end() {return data() + size();}
    const_iterator begin() const {return data();}
    const_iterator end() const {return data() + size();}
    char& operator[] (size_t pos) {return data()[pos];}
    const char& operator[] (size_t pos) const {return data()[pos];}
    void clear() {
        deleteData();
        init();
    }

 private:
    void init() {
        _size = 0;
        _data = _buf;
        _buf[0] = '\0';
    }

    template<typename InputIterator>
    void init(InputIterator begin, InputIterator end) {
        _size = std::distance(begin, end);
        initData();
        std::copy(begin, end, _data);
        _data[_size] = '\0';
    }

    void initData() {
        if (_size < BUF_SIZE) {
            _data = _buf;
        } else {
            _data = new char[_size + 1];
        }
    }

    void deleteData() {
        if (_data != _buf)
            delete[] _data;
    }

    void reserve(size_t size) {
        deleteData();
        _size = size;
        initData();
    }

public:
    // 99.99% token length < 43
    static const uint32_t BUF_SIZE = 48;
private:
    size_t _size;
    char *_data;
    char _buf[BUF_SIZE];
private:
    friend class ShortStringTest;
};

inline bool operator < (const ShortString &lhs, const ShortString &rhs) {
    return std::lexicographical_compare(lhs.begin(), lhs.end(), rhs.begin(), rhs.end());
}

inline bool operator <= (const ShortString &lhs, const ShortString &rhs) {
    return !(rhs < lhs);
}

inline bool operator > (const ShortString &lhs, const ShortString &rhs) {
    return rhs < lhs;
}

inline bool operator >= (const ShortString &lhs, const ShortString &rhs) {
    return !(lhs < rhs);
}

inline bool operator == (const ShortString &lhs, const ShortString &rhs) {
    if (lhs.length() != rhs.length()) {
        return false;
    }
    return std::equal(lhs.begin(), lhs.end(), rhs.begin());
}

inline bool operator != (const ShortString &lhs, const ShortString &rhs) {
    return !(lhs == rhs);
}

inline std::istream& operator >> (std::istream &in, ShortString &s) {
    std::string tmp;
    in >> tmp;
    s = tmp;
    return in;
}

inline std::ostream& operator << (std::ostream &out, const ShortString &s) {
    std::string tmp(s.begin(), s.end());
    out << tmp;
    return out;
}

}
