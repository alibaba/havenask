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

#include "autil/Log.h"
#include "autil/NoCopyable.h"
#include "indexlib/base/Status.h"
#include "indexlib/file_system/file/FileReader.h"
#include "indexlib/index/primary_key/PrimaryKeyPair.h"

namespace indexlibv2::index {

template <typename Key>
class PrimaryKeyLeafIterator : public autil::NoCopyable
{
public:
    PrimaryKeyLeafIterator() {}
    virtual ~PrimaryKeyLeafIterator() {}

public:
    using PKPairTyped = PKPair<Key, docid_t>;

public:
    virtual Status Init(const indexlib::file_system::FileReaderPtr& fileReader) = 0;
    virtual bool HasNext() const = 0;
    virtual Status Next(PKPairTyped& pkPair) = 0;
    virtual void GetCurrentPKPair(PKPairTyped& pair) const = 0;
    virtual uint64_t GetPkCount() const = 0;

private:
    AUTIL_LOG_DECLARE();
};

AUTIL_LOG_SETUP_TEMPLATE(indexlib.index, PrimaryKeyLeafIterator, T);

} // namespace indexlibv2::index
