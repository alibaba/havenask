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
#include <string>

#include "autil/CommonMacros.h"
#include "fslib/common/common_type.h"
#include "indexlib/base/Status.h"
#include "indexlib/util/Exception.h"

namespace indexlib { namespace file_system {

// same value with fslib to avoid too many error code value in log
enum [[nodiscard]] ErrorCode {
    FSEC_OK = fslib::ErrorCode::EC_OK,                             // 0
    FSEC_BADARGS = fslib::ErrorCode::EC_BADARGS,                   // 3
    FSEC_EXIST = fslib::ErrorCode::EC_EXIST,                       // 6
    FSEC_ISDIR = fslib::ErrorCode::EC_ISDIR,                       // 8
    FSEC_NOENT = fslib::ErrorCode::EC_NOENT,                       // 10
    FSEC_NOTDIR = fslib::ErrorCode::EC_NOTDIR,                     // 11
    FSEC_NOTSUP = fslib::ErrorCode::EC_NOTSUP,                     // 13
    FSEC_OPERATIONTIMEOUT = fslib::ErrorCode::EC_OPERATIONTIMEOUT, // 17
    FSEC_ERROR = fslib::ErrorCode::EC_IO_ERROR,                    // 35
    FSEC_UNKNOWN = fslib::ErrorCode::EC_UNKNOWN,                   // 23
};

#define RETURN_IF_FS_ERROR(ec, format, args...)                                                                        \
    do {                                                                                                               \
        const ErrorCode __ec = (ec);                                                                                   \
        if (unlikely(__ec != FSEC_OK)) {                                                                               \
            AUTIL_LOG(ERROR, "ec[%d] " format, __ec, ##args);                                                          \
            return __ec;                                                                                               \
        }                                                                                                              \
    } while (0)

#define RETURN2_IF_FS_ERROR(ec, result, format, args...)                                                               \
    do {                                                                                                               \
        const ErrorCode __ec = (ec);                                                                                   \
        if (unlikely(__ec != FSEC_OK)) {                                                                               \
            AUTIL_LOG(ERROR, "ec[%d] " format, __ec, ##args);                                                          \
            return {__ec, (result)};                                                                                   \
        }                                                                                                              \
    } while (0)

#define FL_CORETURN_IF_FS_ERROR(ec, format, args...)                                                                   \
    if (ErrorCode __ec = (ec); unlikely(__ec != FSEC_OK)) {                                                            \
        AUTIL_LOG(ERROR, "ec[%d] " format, __ec, ##args);                                                              \
        FL_CORETURN __ec;                                                                                              \
    }

#define FL_CORETURN2_IF_FS_ERROR(ec, result, format, args...)                                                          \
    if (ErrorCode __ec = (ec); unlikely(__ec != FSEC_OK)) {                                                            \
        auto __result = (result);                                                                                      \
        AUTIL_LOG(ERROR, "ec[%d] " format, __ec, ##args);                                                              \
        FL_CORETURN FSResult<decltype(__result)>(__ec, std::move(__result));                                           \
    } // namespace file_system

#define THROW_IF_FS_ERROR(ec, format, args...)                                                                         \
    switch (indexlib::file_system::ErrorCode __ec(ec); __ec) {                                                         \
    case indexlib::file_system::FSEC_OK:                                                                               \
        break;                                                                                                         \
    case indexlib::file_system::FSEC_EXIST:                                                                            \
        INDEXLIB_THROW(indexlib::util::ExistException, "ec[%d] " format, __ec, ##args);                                \
    case indexlib::file_system::FSEC_NOENT:                                                                            \
        INDEXLIB_THROW(indexlib::util::NonExistException, "ec[%d] " format, __ec, ##args);                             \
    case indexlib::file_system::FSEC_ISDIR:                                                                            \
    case indexlib::file_system::FSEC_NOTDIR:                                                                           \
    case indexlib::file_system::FSEC_BADARGS:                                                                          \
        INDEXLIB_THROW(indexlib::util::BadParameterException, "ec[%d] " format, __ec, ##args);                         \
    case indexlib::file_system::FSEC_NOTSUP:                                                                           \
        INDEXLIB_THROW(indexlib::util::UnSupportedException, "ec[%d] " format, __ec, ##args);                          \
    case indexlib::file_system::FSEC_OPERATIONTIMEOUT:                                                                 \
    case indexlib::file_system::FSEC_ERROR:                                                                            \
    case indexlib::file_system::FSEC_UNKNOWN:                                                                          \
        INDEXLIB_THROW(indexlib::util::FileIOException, "ec[%d] " format, __ec, ##args);                               \
    default:                                                                                                           \
        assert(false);                                                                                                 \
        INDEXLIB_THROW(indexlib::util::UnSupportedException, "ec[%d] " format, __ec, ##args);                          \
    }

inline Status toStatus(ErrorCode ec)
{
    switch (ec) {
    case FSEC_OK:
        return Status::OK();
    case FSEC_ERROR:
        return Status::IOError("io error");
    case FSEC_EXIST:
        return Status::Exist("already exists");
    case FSEC_ISDIR:
        return Status::InternalError("is a dir");
    case FSEC_NOENT:
        return Status::NotFound("is not exist");
    case FSEC_NOTDIR:
        return Status::InternalError("is not a dir");
    case FSEC_NOTSUP:
        return Status::Unimplement("is not support");
    case FSEC_BADARGS:
        return Status::InvalidArgs("bad arguments");
    case FSEC_OPERATIONTIMEOUT:
        return Status::InternalError("timeout");
    case FSEC_UNKNOWN:
        return Status::Unknown("unknown error");
    default:
        return Status::Unknown("unknown error: %d", ec);
    }
}

inline ErrorCode ParseFromFslibEC(fslib::ErrorCode ec)
{
    switch (ec) {
    case fslib::ErrorCode::EC_OK:
        return ErrorCode::FSEC_OK;
    case fslib::ErrorCode::EC_EXIST:
        return ErrorCode::FSEC_EXIST;
    case fslib::ErrorCode::EC_NOENT:
        return ErrorCode::FSEC_NOENT;
    case fslib::ErrorCode::EC_ISDIR:
        return ErrorCode::FSEC_ISDIR;
    case fslib::ErrorCode::EC_NOTDIR:
        return ErrorCode::FSEC_NOTDIR;
    case fslib::ErrorCode::EC_NOTSUP:
        return ErrorCode::FSEC_NOTSUP;
    case fslib::ErrorCode::EC_UNKNOWN:
        return ErrorCode::FSEC_UNKNOWN;
    case fslib::ErrorCode::EC_OPERATIONTIMEOUT:
        return ErrorCode::FSEC_OPERATIONTIMEOUT;
    case fslib::ErrorCode::EC_BADARGS:
        return ErrorCode::FSEC_BADARGS;
    case fslib::ErrorCode::EC_FALSE: // return ErrorCode::FSEC_FALSE;
    case fslib::ErrorCode::EC_TRUE:  // return ErrorCode::FSEC_TRUE;
    case fslib::ErrorCode::EC_AGAIN:
    case fslib::ErrorCode::EC_BADF:
    case fslib::ErrorCode::EC_BUSY:
    case fslib::ErrorCode::EC_CONNECTIONLOSS:
    case fslib::ErrorCode::EC_KUAFU:
    case fslib::ErrorCode::EC_NOTEMPTY:
    case fslib::ErrorCode::EC_PARSEFAIL:
    case fslib::ErrorCode::EC_PERMISSION:
    case fslib::ErrorCode::EC_XDEV:
    case fslib::ErrorCode::EC_INIT_ZOOKEEPER_FAIL:
    case fslib::ErrorCode::EC_INVALIDSTATE:
    case fslib::ErrorCode::EC_SESSIONEXPIRED:
    case fslib::ErrorCode::EC_PANGU_FILE_LOCK:
    case fslib::ErrorCode::EC_LOCAL_DISK_NO_SPACE:
    case fslib::ErrorCode::EC_LOCAL_DISK_IO_ERROR:
    case fslib::ErrorCode::EC_PANGU_MASTER_MODE:
    case fslib::ErrorCode::EC_PANGU_CLIENT_SESSION:
    case fslib::ErrorCode::EC_PANGU_FILE_LOCK_BY_SAME_PROCESS:
    case fslib::ErrorCode::EC_PANGU_FLOW_CONTROL:
    case fslib::ErrorCode::EC_PANGU_USER_DENIED:
    case fslib::ErrorCode::EC_PANGU_NOT_ENOUGH_CHUNKSERVER:
    case fslib::ErrorCode::EC_PANGU_STREAM_CORRUPTED:
    case fslib::ErrorCode::EC_PANGU_INTERNAL_SERVER_ERROR:
    case fslib::ErrorCode::EC_NO_SPACE:
    case fslib::ErrorCode::EC_IO_ERROR:
    case fslib::ErrorCode::EC_READ_ONLY_FILE_SYSTEM:
    case fslib::ErrorCode::EC_ACCESS_DENY:
        return ErrorCode::FSEC_ERROR;
    default:
        return ErrorCode::FSEC_UNKNOWN;
    }
}

#define RETURN_IF_FS_EXCEPTION(expr, format, args...)                                                                  \
    try {                                                                                                              \
        (expr);                                                                                                        \
    } catch (const indexlib::util::NonExistException& e) {                                                             \
        AUTIL_LOG(ERROR, "exception[%s], " format, e.what(), ##args);                                                  \
        return FSEC_NOENT;                                                                                             \
    } catch (const indexlib::util::ExistException& e) {                                                                \
        AUTIL_LOG(ERROR, "exception[%s], " format, e.what(), ##args);                                                  \
        return FSEC_EXIST;                                                                                             \
    } catch (const indexlib::util::UnSupportedException& e) {                                                          \
        AUTIL_LOG(ERROR, "exception[%s], " format, e.what(), ##args);                                                  \
        return FSEC_NOTSUP;                                                                                            \
    } catch (const std::exception& e) {                                                                                \
        AUTIL_LOG(ERROR, "exception[%s], " format, e.what(), ##args);                                                  \
        return FSEC_ERROR;                                                                                             \
    } catch (...) {                                                                                                    \
        AUTIL_LOG(ERROR, "" format, ##args);                                                                           \
        return FSEC_ERROR;                                                                                             \
    }

#define RETURN2_IF_FS_EXCEPTION(expr, result, format, args...)                                                         \
    try {                                                                                                              \
        (expr);                                                                                                        \
    } catch (const indexlib::util::NonExistException& e) {                                                             \
        AUTIL_LOG(ERROR, "exception[%s], " format, e.what(), ##args);                                                  \
        return {FSEC_NOENT, result};                                                                                   \
    } catch (const indexlib::util::ExistException& e) {                                                                \
        AUTIL_LOG(ERROR, "exception[%s], " format, e.what(), ##args);                                                  \
        return {FSEC_EXIST, result};                                                                                   \
    } catch (const indexlib::util::UnSupportedException& e) {                                                          \
        AUTIL_LOG(ERROR, "exception[%s], " format, e.what(), ##args);                                                  \
        return {FSEC_NOTSUP, result};                                                                                  \
    } catch (const std::exception& e) {                                                                                \
        AUTIL_LOG(ERROR, "exception[%s], " format, e.what(), ##args);                                                  \
        return {FSEC_ERROR, result};                                                                                   \
    } catch (...) {                                                                                                    \
        AUTIL_LOG(ERROR, "" format, ##args);                                                                           \
        return {FSEC_ERROR, result};                                                                                   \
    }

}} // namespace indexlib::file_system
