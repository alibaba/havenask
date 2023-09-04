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
#ifndef ANET_ADVANCEPACKETFACTORY_H_
#define ANET_ADVANCEPACKETFACTORY_H_

#include "aios/network/anet/anet.h"
#include "aios/network/anet/ipacketfactory.h"

BEGIN_ANET_NS();
template <class T>
class AdvancePacketFactory : public IPacketFactory {
public:
    AdvancePacketFactory();

    ~AdvancePacketFactory();

    virtual Packet *createPacket(int pcode);
};

/*****************************************************************************/
/*****************************************************************************/
template <class T>
AdvancePacketFactory<T>::AdvancePacketFactory() {}

template <class T>
AdvancePacketFactory<T>::~AdvancePacketFactory() {}

template <class T>
Packet *AdvancePacketFactory<T>::createPacket(int pcode) {
    /* We assume that no method id can be greater than 32768 for now.
     * Below is a bad assertion since server will treat pcode as
     * service id + method id, which is likely to trigger this assertion.*/
    // DBGASSERT(pcode >= 0);

    /* When pcode == 0, it normally means that this comes form the path for
     * client to allocate the packet and try to initiate a RPC. In this case
     * We should create AdvancePacket. */
    if (AdvancePacket<T>::IsAdvancePacket(pcode))
        return new AdvancePacket<T>;
    else
        return new T;
}

END_ANET_NS();

#endif
