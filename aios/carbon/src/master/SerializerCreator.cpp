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
#include "master/SerializerCreator.h"
#include "carbon/Log.h"
#include "common/PathUtil.h"
#include "master/CompressedSerializer.h"

using namespace std;
using namespace hippo;

USE_CARBON_NAMESPACE(common);
BEGIN_CARBON_NAMESPACE(master);

CARBON_LOG_SETUP(master, SerializerCreator);

SerializerCreator::SerializerCreator(
        worker_framework::LeaderChecker *leaderChecker,
        const string &serializerDir,
        const string &backupDir)
{
    _leaderChecker = leaderChecker;
    _serializerDir = serializerDir;
    _backupDir = backupDir;
    _leaderSerializerCreator = new LeaderSerializerCreator();
}

SerializerCreator::~SerializerCreator() {
    delete _leaderSerializerCreator;
}

SerializerPtr SerializerCreator::create(const string &fileName,
                                        const string &id,
                                        bool compressed)
{
    string name = PathUtil::baseName(fileName);
    string dir = PathUtil::getParentDir(fileName);

    string serializeDir = PathUtil::joinPath(_serializerDir, dir);
    string backupRoot;
    if (_backupDir != "") {
        backupRoot = PathUtil::joinPath(_backupDir, dir);
        if (!PathUtil::makeSureDirExist(backupRoot)) {
            CARBON_LOG(ERROR, "create leader serializer failed, "
                       "cause make sure backup root exist failed. "
                       "serializeDir:[%s], backupDir:[%s], "
                       "fileName:[%s], backupRoot:[%s].",
                       _serializerDir.c_str(), _backupDir.c_str(),
                       fileName.c_str(), backupRoot.c_str());
            return SerializerPtr();
        }
    }
    
    LeaderSerializer *leaderSerializer = _leaderSerializerCreator->create(
            _leaderChecker, serializeDir, name, backupRoot);
    if (leaderSerializer == NULL) {
        CARBON_LOG(ERROR, "create leader serializer failed, "
                   "serializerDir:[%s], "
                   "fileName:[%s], backupRoot:[%s].", _serializerDir.c_str(),
                   fileName.c_str(), backupRoot.c_str());
        return SerializerPtr();
    }
    
    if (compressed) {
        return SerializerPtr(new CompressedSerializer(leaderSerializer, id));
    } else {
        return SerializerPtr(new Serializer(leaderSerializer, id));
    }

    return SerializerPtr();
}

END_CARBON_NAMESPACE(master);

