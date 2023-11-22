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
#ifndef __BEEPER_BEEPER_H
#define __BEEPER_BEEPER_H
#include "beeper/common/common_type.h"

static const int BEEPER_MAX_MSG_LENGTH = 1024;

#define BEEPER_INIT_FROM_LOCAL_CONF_FILE(localConfFile, globalTags...)  \
    {                                                                   \
    }

#define BEEPER_INIT_FROM_CONF_STRING(configStr, globalTags...)          \
    {                                                                   \
    }

#define BEEPER_ADD_GLOBAL_TAG(key, value)                               \
    {                                                                   \
    }
    
#define DECLARE_BEEPER_COLLECTOR(id, collectorTags...)                  \
    {                                                                   \
    }

#define REMOVE_BEEPER_COLLECTOR(id)                                     \
    {                                                                   \
    }

#define BEEPER_LEVEL_REPORT(id, level, msg, eventTags...)               \
    {                                                                   \
    }

#define BEEPER_REPORT(id, msg, eventTags...)                            \
    {                                                                   \
    }

#define BEEPER_FORMAT_REPORT(id, eventTags, format, args...)            \
    {                                                                   \
    }


// prevent ccplus treat unused variables as error
#define BEEPER_FORMAT_REPORT_WITHOUT_TAGS(id, format, args...)          \
    {                                                                   \
        char buffer[BEEPER_MAX_MSG_LENGTH];                             \
        snprintf(buffer, BEEPER_MAX_MSG_LENGTH, format, args);          \
    }


#define BEEPER_INTERVAL_REPORT(interval, id, msg, eventTags...)         \
    {                                                                   \
    }

#define BEEPER_FORMAT_INTERVAL_REPORT(interval, id, eventTags, format, args...) \
    {                                                                   \
    }

#define BEEPER_FORMAT_INTERVAL_WITHOUT_TAGS_REPORT(interval, id, format, args...)                                        \
    {                                                                   \
    }

#define BEEPER_INTERVAL_LEVEL_REPORT(interval, id, level, msg, eventTags...) \
    {                                                                   \
    }


#define BEEPER_CLOSE()                                                  \
    {                                                                   \
    }


#endif //__BEEPER_BEEPER_H
