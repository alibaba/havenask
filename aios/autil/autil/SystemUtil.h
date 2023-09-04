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
#ifndef AUTIL_SYSTEMUTIL_H
#define AUTIL_SYSTEMUTIL_H

#include "autil/Log.h"

namespace autil {

class SystemUtil {
public:
    SystemUtil();
    ~SystemUtil();

public:
    static bool tar(const std::string &archiveFile, const std::string &sourceDir);

    static bool unTar(const std::string &archiveFile, const std::string &targetDir);

    static bool aInst2(const std::string &rpmFile, const std::string &targetDir, const std::string &varDir);
    // 180 s
    static int32_t call(const std::string &command, std::string *out, int32_t timeout = 180, bool useTimeout = true);

    static void readStream(FILE *stream, std::string *pattern, int32_t timeout);

    static std::string getEnv(const std::string &key);

    static bool getPidCgroup(pid_t pid, std::string &cgroupPath);

    static bool asyncCall(const std::string &command, pid_t &pid);

private:
    AUTIL_LOG_DECLARE();
};

} // namespace autil

#endif // AUTIL_SYSTEMUTIL_H
