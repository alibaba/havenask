//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// Simple hash function used for internal data structures

#ifndef __INDEXLIB_CACHE_HASH_H
#define __INDEXLIB_CACHE_HASH_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"

IE_NAMESPACE_BEGIN(util);

class CacheHash
{
public:
    CacheHash();
    ~CacheHash();

public:
    static uint32_t Hash(const char* data, size_t n, uint32_t seed);
    
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(CacheHash);

IE_NAMESPACE_END(util);

#endif //__INDEXLIB_CACHE_HASH_H
