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
#ifndef ISEARCH_MULTI_CALL_ERRORCONTROLLER_H
#define ISEARCH_MULTI_CALL_ERRORCONTROLLER_H

#include "aios/network/gig/multi_call/controller/ControllerFeedBack.h"

namespace multi_call {

class ErrorController
{
public:
    ErrorController();
    ~ErrorController();

private:
    ErrorController(const ErrorController &);
    ErrorController &operator=(const ErrorController &);

public:
    float update(ControllerFeedBack &feedBack);

private:
    AUTIL_LOG_DECLARE();
};

MULTI_CALL_TYPEDEF_PTR(ErrorController);

} // namespace multi_call

#endif // ISEARCH_MULTI_CALL_ERRORCONTROLLER_H
