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
#ifndef NAVI_REMOTEINPUTPORT_H
#define NAVI_REMOTEINPUTPORT_H

#include "navi/engine/Port.h"

namespace navi {

class GraphDomain;

class RemoteInputPort : public Port
{
public:
    RemoteInputPort(const EdgeDef &def,
                    SubGraphBorder *border);
    ~RemoteInputPort();
private:
    RemoteInputPort(const RemoteInputPort &);
    RemoteInputPort &operator=(const RemoteInputPort &);
public:
    void setCallback(NaviPartId partId, bool eof) override;
    void getCallback(NaviPartId partId, bool eof) override;
};

}

#endif //NAVI_REMOTEINPUTPORT_H
