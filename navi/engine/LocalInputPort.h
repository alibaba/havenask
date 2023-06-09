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
#ifndef NAVI_LOCALINPUTPORT_H
#define NAVI_LOCALINPUTPORT_H

#include "navi/engine/LocalPortBase.h"

namespace navi {

class LocalInputPort : public LocalPortBase
{
public:
    LocalInputPort(const EdgeDef &def, SubGraphBorder *border);
    ~LocalInputPort();
private:
    LocalInputPort(const LocalInputPort &);
    LocalInputPort &operator=(const LocalInputPort &);
public:
    void setCallback(NaviPartId partId, bool eof) override;
    void getCallback(NaviPartId partId, bool eof) override;
private:
    void doCallBack(NaviPartId partId, bool eof);
};

}

#endif //NAVI_LOCALINPUTPORT_H
