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
#include "indexlib/index/normal/primarykey/primary_key_formatter.h"
#include "indexlib/indexlib.h"

namespace indexlib { namespace index {

template <typename Key>
class PrimaryKeyPairSegmentIterator
{
public:
    typedef typename PrimaryKeyFormatter<Key>::PKPair PKPair;

public:
    PrimaryKeyPairSegmentIterator() {}

    virtual ~PrimaryKeyPairSegmentIterator() {}

public:
    virtual void Init(const file_system::FileReaderPtr& fileReader) = 0;
    virtual bool HasNext() const = 0;
    virtual void Next(PKPair& pkPair) = 0;
    virtual void GetCurrentPKPair(PKPair& pair) const = 0;
    virtual uint64_t GetPkCount() const = 0;

private:
    IE_LOG_DECLARE();
};
}} // namespace indexlib::index
