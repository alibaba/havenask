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
#include "indexlib/base/Status.h"
#include "indexlib/util/Exception.h"

namespace indexlib::util {

inline void ThrowIfStatusError(indexlib::Status status)
{
    if (status.IsOK()) {
        return;
    }
    const std::string& errStr = status.ToString();
    const char* errMsg = errStr.c_str();
    if (status.IsCorruption()) {
        INDEXLIB_THROW(indexlib::util::IndexCollapsedException, "%s", errMsg);
    } else if (status.IsIOError()) {
        INDEXLIB_THROW(indexlib::util::FileIOException, "%s", errMsg);
    } else if (status.IsNoMem()) {
        INDEXLIB_THROW(indexlib::util::OutOfMemoryException, "%s", errMsg);
    } else if (status.IsUnknown()) {
        INDEXLIB_THROW(indexlib::util::RuntimeException, "%s", errMsg);
    } else if (status.IsConfigError()) {
        INDEXLIB_THROW(indexlib::util::BadParameterException, "%s", errMsg);
    } else if (status.IsInvalidArgs()) {
        INDEXLIB_THROW(indexlib::util::BadParameterException, "%s", errMsg);
    } else if (status.IsInternalError()) {
        INDEXLIB_THROW(indexlib::util::RuntimeException, "%s", errMsg);
    } else if (status.IsOutOfRange()) {
        INDEXLIB_THROW(indexlib::util::OutOfRangeException, "%s", errMsg);
    } else if (status.IsNotFound()) {
        INDEXLIB_THROW(indexlib::util::ReachMaxResourceException, "%s", errMsg);
    } else if (status.IsNeedDump()) {
        INDEXLIB_THROW(indexlib::util::RuntimeException, "%s", errMsg);
    } else if (status.IsUnimplement()) {
        INDEXLIB_THROW(indexlib::util::UnImplementException, "%s", errMsg);
    } else if (status.IsUninitialize()) {
        INDEXLIB_THROW(indexlib::util::RuntimeException, "%s", errMsg);
    } else if (status.IsNotFound()) {
        INDEXLIB_THROW(indexlib::util::NonExistException, "%s", errMsg);
    } else if (status.IsExpired()) {
        INDEXLIB_THROW(indexlib::util::RuntimeException, "%s", errMsg);
    } else if (status.IsExist()) {
        INDEXLIB_THROW(indexlib::util::ExistException, "%s", errMsg);
    } else if (status.IsSealed()) {
        INDEXLIB_THROW(indexlib::util::RuntimeException, "%s", errMsg);
    } else {
        INDEXLIB_THROW(indexlib::util::RuntimeException, "%s", errMsg);
    }
}

#define THROW_IF_STATUS_ERROR(status)                                                                                  \
    do {                                                                                                               \
        indexlib::Status __status__ = (status);                                                                        \
        indexlib::util::ThrowIfStatusError(__status__);                                                                \
    } while (0)
} // namespace indexlib::util
