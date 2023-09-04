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
#ifndef CARBON_TARGETRECORDER_H
#define CARBON_TARGETRECORDER_H

#include "common/common.h"
#include "carbon/Log.h"
#include "autil/Lock.h"

BEGIN_CARBON_NAMESPACE(common);

class Recorder
{
public:
    Recorder();
    ~Recorder();

public:
    static void newTarget(const std::string &UID, const std::string &target);
    static void newFinalTarget(const std::string &UID, const std::string &finalTarget);
    static void newCurrent(const std::string &UID, const std::string &current);
    static void newAdminTarget(const std::string &UID, const std::string &target);
    static void logWhenDiff(const std::string &key, const std::string &content,
                            alog::Logger *logger);
    static bool diff(const std::string &key, const std::string &content);

private:
    static void newRecord(const std::string &content, const std::string &dir, const std::string &suffix);
    static bool hasSuffix(const std::string &str, const std::string &suffix);
    static size_t _maxCount;
    static std::string _basePath;
    static std::map<std::string, std::string> _cache;
    static autil::ThreadMutex _mutex;
private:
    CARBON_LOG_DECLARE();
};

END_CARBON_NAMESPACE(common);

#endif //CARBON_TARGETRECORDER_H
