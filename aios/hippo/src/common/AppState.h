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
#ifndef HIPPO_APPSTATE_H
#define HIPPO_APPSTATE_H

#include "util/Log.h"
#include "common/common.h"
#include "common/Serializeable.h"

BEGIN_HIPPO_NAMESPACE(common);

class AppState
{
public:
    AppState();
    virtual ~AppState();
private:
    AppState(const AppState &);
    AppState& operator=(const AppState &);
public:
    bool read(const std::string &fileName, Serializeable *obj) const;
    bool write(const std::string &fileName, const Serializeable *obj);

    virtual bool read(const std::string &fileName,
                      std::string &content) const = 0;
    
    virtual bool write(const std::string &fileName,
                       const std::string &content) = 0;
    virtual bool check(const std::string &path, bool &bExist) = 0;

    virtual bool remove(const std::string &path) = 0;
private:
    HIPPO_LOG_DECLARE();
};

HIPPO_INTERNAL_TYPEDEF_PTR(AppState);

END_HIPPO_NAMESPACE(common);

#endif //HIPPO_APPSTATE_H
