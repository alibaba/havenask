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
#ifndef CARBON_COMMON_TIMERTRACKER_H
#define CARBON_COMMON_TIMERTRACKER_H

#include "autil/TimeUtility.h"
#include "common/common.h"

BEGIN_CARBON_NAMESPACE(common);

JSONIZABLE_CLASS(TimeTracker) {
    JSONIZABLE_CLASS(Label) {
    public:
        std::string name;
        int64_t elapsed;

        Label() : elapsed(0) {}
        Label(const std::string& s, int64_t e) : name(s), elapsed(e) {}
        JSONIZE() {
            JSON_FIELD(name);
            JSON_FIELD(elapsed);
        }
    };
public:
    JSONIZE() {
        JSON_FIELD(_lbls);
    }

    void append(const std::string& name, int64_t elapsed) {
        _lbls.push_back(Label(name, elapsed));
    }

private:
    std::vector<Label> _lbls;
};

#define TIME_TRACKER() common::TimeTracker __tracker
#define TIME_TRACK(name, exps) { \
    int64_t start = TimeUtility::currentTimeInMicroSeconds(); \
    exps; \
    int64_t end = TimeUtility::currentTimeInMicroSeconds(); \
    __tracker.append(name, (end - start) / 1000); \
}
#define TIME_TRACK0(exps) TIME_TRACK(#exps, exps)
#define TIME_TRACK_STRING() (ToJsonString(__tracker, true).c_str())

END_CARBON_NAMESPACE(common);

#endif

