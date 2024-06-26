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

namespace cava {

enum ErrorLevel {
    EL_INFO = 0
};

enum ErrorType {
    ET_Lexer,
};

// unlikely error code : X0000
enum ErrorCode {
    EC_Lexer_0 = 10000,
};

enum ExceptionCode {
    EXC_NONE = 0,
    EXC_INVALID_ARGUMENT=1
};

} // end namespace cava
