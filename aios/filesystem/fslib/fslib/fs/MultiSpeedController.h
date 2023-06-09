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
#ifndef FSLIB_MULTISPEEDCONTROLLER_H
#define FSLIB_MULTISPEEDCONTROLLER_H

#include <autil/Log.h>
#include "fslib/fslib.h"
#include "fslib/util/Singleton.h"
#include "fslib/fs/SpeedController.h"

FSLIB_BEGIN_NAMESPACE(fs);

class MultiSpeedController : public util::Singleton<MultiSpeedController>
{
public:
    MultiSpeedController();
    ~MultiSpeedController();
public:
    static SpeedController* getReadSpeedController(const std::string& filePath);
    static bool updateQuota(const std::string& patternString, int64_t quotaSizePerSec);
    static double getQuotaUsageRatio(const std::string& patternString);
    
    // quotaInfo pair format:
    // * first: pattern, second: quotaPerSize
    static void getQuotaDetailInfo(std::vector<std::pair<std::string, int64_t>>& quotaInfos);

public:
    std::vector<SpeedControllerPtr>& TEST_getContollers() { return _readSpeedControllers; }

private:
    void init(const std::string& params);
    SpeedController* findReadSpeedController(const std::string& filePath) const;
    SpeedController* findReadSpeedControllerByPatternString(const std::string& pattern) const;

private:
    std::vector<SpeedControllerPtr> _readSpeedControllers;

};

FSLIB_TYPEDEF_AUTO_PTR(MultiSpeedController);

FSLIB_END_NAMESPACE(fs);

#endif //FSLIB_MULTIREADSPEEDCONTROLLER_H
