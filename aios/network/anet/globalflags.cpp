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
#include "aios/network/anet/globalflags.h"

#include <stdint.h>
#include <sstream>
#include <string>

#include "aios/network/anet/crc.h"

namespace anet {
namespace flags {

    #define DEFINE_GLOBAL_FLAG(type, name, defaultvalue) \
        static type name = defaultvalue; \
        type get##name(void) { \
            return name; \
        }\
        void set##name(type newval) { \
            name = newval; \
        }

    /*************************************************************************/
    DEFINE_GLOBAL_FLAG(int, ConnectTimeout, 5000000);
    DEFINE_GLOBAL_FLAG(bool, ChecksumState, true);
    DEFINE_GLOBAL_FLAG(int64_t, MaxConnectionCount, -1);
    DEFINE_GLOBAL_FLAG(bool, EnableSocketNonBlockConnect, true);

    /* Dump all global config for debugging purpose. */
    int dump(std::ostringstream &buf) {
        buf << "=================================Dump of Config=================================\n";
        buf << "Checksum enabled: " << getChecksumState()
            << "\t" << "algorithm: " << std::string(GetCrcFuncStr()) << std::endl;

        buf << "Connecting Timout(us): " << getConnectTimeout() << std::endl;
        return 0;
    }
}
}
