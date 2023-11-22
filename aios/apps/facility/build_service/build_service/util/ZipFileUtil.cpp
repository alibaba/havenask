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
#include "build_service/util/ZipFileUtil.h"

#include <ostream>
#include <stddef.h>
#include <stdint.h>

#include "alog/Logger.h"
#include "autil/legacy/exception.h"
#include "indexlib/file_system/ErrorCode.h"
#include "indexlib/file_system/FSResult.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/util/Exception.h"
#include "mz.h"
#include "mz_strm_mem.h"
#include "mz_zip.h"

using namespace std;

using namespace indexlib::file_system;

namespace build_service { namespace util {
BS_LOG_SETUP(util, ZipFileUtil);

ZipFileUtil::ZipFileUtil() {}

ZipFileUtil::~ZipFileUtil() {}

#define DIR_DELIMTER '/'

bool ZipFileUtil::readZipFile(const string& fileName, unordered_map<string, string>& fileMap)
{
    string fileContent;
    indexlib::file_system::ErrorCode ec = FslibWrapper::AtomicLoad(fileName, fileContent).Code();
    if (ec == FSEC_NOENT) {
        return false;
    } else if (ec != FSEC_OK) {
        AUTIL_LEGACY_THROW(indexlib::util::FileIOException, "load file [" + fileName + "] FAILED.");
    }
    mz_zip_file* file_info = NULL;
    void* stream = NULL;
    void* handle = NULL;
    char buffer[4096];
    uint64_t num_entries = 0;
    int64_t entry_pos = 0;
    int32_t err = MZ_OK;
    uint8_t encrypted = 0;

    mz_stream_mem_create(&stream);
    mz_stream_mem_set_buffer(stream, (void*)fileContent.c_str(), fileContent.size());

    mz_zip_create(&handle);
    err = mz_zip_open(handle, stream, MZ_OPEN_MODE_READ);
    if (err == MZ_OK) {
        mz_zip_get_number_entry(handle, &num_entries);

        err = mz_zip_goto_first_entry(handle);
        while (err == MZ_OK) {
            err = mz_zip_entry_get_info(handle, &file_info);
            if (err != MZ_OK) {
                BS_LOG(ERROR, "move to next inner file for %s failed.", fileName.c_str());
                break;
            }
            BS_LOG(DEBUG, "inner filename %s is opened.", file_info->filename);
            encrypted = (file_info->flag & MZ_ZIP_FLAG_ENCRYPTED);

            if (file_info->filename[file_info->filename_size - 1] == DIR_DELIMTER) {
                err = mz_zip_goto_next_entry(handle);
                if (err != MZ_OK) {
                    BS_LOG(ERROR, "move to next inner file for %s failed.", fileName.c_str());
                    break;
                }
                continue;
            }

            if (encrypted) {
                BS_LOG(ERROR, "zip file %s is encrypted.", fileName.c_str());
                break;
            }
            err = mz_zip_entry_read_open(handle, 0, NULL);
            if (err != MZ_OK) {
                BS_LOG(ERROR, "open inner file[%s] in zip file[%s] failed.", file_info->filename, fileName.c_str());
                mz_zip_entry_close(handle);
                break;
            }

            err = mz_zip_entry_is_open(handle);
            if (err != MZ_OK) {
                BS_LOG(ERROR, "open inner file[%s] in zip file[%s] failed.", file_info->filename, fileName.c_str());
                mz_zip_entry_close(handle);
                break;
            }

            entry_pos = mz_zip_get_entry(handle);
            if (entry_pos < 0) {
                BS_LOG(ERROR, "get entry pos for inner file[%s] in zip file[%s] failed.", file_info->filename,
                       fileName.c_str());
                mz_zip_entry_close(handle);
                break;
            }
            int32_t readSize = -1;
            bool readSuccess = true;
            stringstream ss;
            while (readSize != 0) {
                readSize = mz_zip_entry_read(handle, buffer, sizeof(buffer));
                if (readSize < 0) {
                    BS_LOG(ERROR, "read inner file[%s] in zip file[%s] failed.", file_info->filename, fileName.c_str());
                    readSuccess = false;
                    break;
                }
                ss << string(buffer, readSize);
            }
            if (readSuccess) {
                string innerFileName(file_info->filename, file_info->filename_size);
                fileMap[innerFileName] = ss.str();
            }
            err = mz_zip_entry_close(handle);
            if (err != MZ_OK) {
                BS_LOG(ERROR, "close inner file[%s] in zip file[%s] failed.", file_info->filename, fileName.c_str());
                break;
            }
            err = mz_zip_goto_next_entry(handle);
        }
        mz_zip_entry_close(handle);
        mz_zip_close(handle);
    }

    mz_zip_delete(&handle);
    mz_stream_mem_delete(&stream);

    return err == MZ_OK || err == MZ_END_OF_LIST;
}

}} // namespace build_service::util
