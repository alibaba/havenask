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
#include <memory>

#include "autil/Log.h"

namespace autil {
class InterfaceManager;
} // namespace autil

namespace sql {

class UdfToQueryCreatorFactory {
public:
    UdfToQueryCreatorFactory() {}
    virtual ~UdfToQueryCreatorFactory() {}

private:
    UdfToQueryCreatorFactory(const UdfToQueryCreatorFactory &);
    UdfToQueryCreatorFactory &operator=(const UdfToQueryCreatorFactory &);

public:
    virtual bool registerUdfToQuery(autil::InterfaceManager *manager) = 0;

public:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<UdfToQueryCreatorFactory> UdfToQueryCreatorFactoryPtr;
} // namespace sql
