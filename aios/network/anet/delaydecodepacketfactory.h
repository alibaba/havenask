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
/**
 * File name: delaydecodepacketfactory.h
 * Author: lizhang
 * Create time: 2010-11-07 18:34:30
 * $Id$
 *
 * Description: ***add description here***
 *
 */

#ifndef ANET_DELAYDECODEPACKETFACTORY_H_
#define ANET_DELAYDECODEPACKETFACTORY_H_

#include "aios/network/anet/ipacketfactory.h"

namespace anet {
class Packet;

class DelayDecodePacketFactory : public IPacketFactory {
public:
    DelayDecodePacketFactory();

    ~DelayDecodePacketFactory();

    virtual Packet *createPacket(int pcode);
};

} /*end namespace anet*/
#endif /*ANET_DELAYDECODEPACKETFACTORY_H_*/
