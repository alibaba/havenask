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
#include "autil/metric/MetricUtil.h"
#include <ctype.h>
#include <string.h>

namespace autil {
namespace metric {

MetricUtil::MetricUtil() { 
}

MetricUtil::~MetricUtil() { 
}

const char* MetricUtil::skipToken(const char *str) {
    unsigned char *p = (unsigned char*)str;
    while (isspace(*p)) { 
        p++;
    }
    while (*p && !isspace(*p)) {
        p++;
    }
    return (char*)p;
}

const char* MetricUtil::skipWhite(const char *str) {
    unsigned char *p = (unsigned char*)str;
    int i = strlen(str);
    while (i > 0 && isspace(*p)) { 
        p++;
        i--;
    }  
    if (i == 0) {
        return NULL;
    }
    return (char *)p;
}

}
}

