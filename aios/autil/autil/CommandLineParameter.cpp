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
#include "autil/CommandLineParameter.h"

#include <iosfwd>
#include <stdint.h>
#include <string.h>

#include "autil/Log.h"
#include "autil/StringTokenizer.h"

namespace autil {
AUTIL_DECLARE_AND_SETUP_LOGGER(autil, CommandLineParameter);

using namespace std;

CommandLineParameter::CommandLineParameter(const string &cmd) {
    // tokenize the cmd
    StringTokenizer st(cmd, " ", StringTokenizer::TOKEN_TRIM | StringTokenizer::TOKEN_IGNORE_EMPTY);

    // copy tokenized cmd into argv
    _argc = st.getNumTokens();
    ;
    _argv = new char *[_argc];
    for (int32_t i = 0; i < _argc; ++i) {
        int32_t size = st[i].length() + 1;
        _argv[i] = new char[size];
        strncpy(_argv[i], st[i].c_str(), size);
        AUTIL_LOG(TRACE3, "argv[%d]: %s", i, _argv[i]);
    }
}

CommandLineParameter::~CommandLineParameter() {
    for (int32_t i = 0; i < _argc; ++i) {
        delete[] _argv[i];
        _argv[i] = NULL;
    }
    delete[] _argv;
    _argv = NULL;
}

} // namespace autil
