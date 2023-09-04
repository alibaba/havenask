#!/bin/env python
import sys

usage = '''
usage:
%(prog)s cmd [options]

commands:
    deploy[dp]               deploy configurations
    download[dl]             download configurations

    start[st]                start swift system
    stop[sp]                 stop swift system
    getstatus[gs]            get the status of swift system
    getappstatus[gas]        get swift app status for hippo
    delete[del]              delete configurations and directories
    getbrokerstatus[gbs]     get broker status

    updateadminconfig[uac]   update admin config
    updatebrokerconfig[ubc]  update broker config
    rollbackbrokerconfig[rbc]rollback broker config

    addtopic[at]             add new topic to swift system
    addtopic[atb]            batch add new topic to swift system
    modifytopic[mt]          modify topic infos
    transferpartition[tp]    transfer partition
    changeslot[cs]           change slot
    gettopicnames[gtn]       get all topic names of swift system
    gettopicinfo[gti]        get a specific topic infos
    gettopicmeta[gtm]        get topic metas
    deletetopic[dt]          delelte a topic from swift system
    deletetopicbytime[dtt]   delelte topics from swift system which hasn't message long time
    deletetopicdata[dtd]     delelte topic data from swift system which hasn't message long time
    sendmsg[sm]              send msg to broker
    getmsg[gm]               get msg from broker
    getmaxmsgid[gmi]         get max msg id from broker
    getminmsgidbytime[gmit]  get min msg id by time from broker
    exporttopics[et]         export topic metas into a file
    importtopics[it]         add topics from export meta file
    updatetopics[ut]         update topics from export meta file
    copy[cp]                 copy file
    configversion[cv]        get config version
    getconfig[gc]            get config
    registerschema[regscm]   register schema
    gettopicrwtime[gtrw]     get topic rw time
    getlastdeletednousetopic[gtldnt]  get latest deleted not use topics
    getdeletednousetopic[gtdnt]       get deleted no use topics
    getdeletednousetopicfiles[gtdntf] get deleted no use topic files
    updatewriterversion[uwv] update writer version
    turntomaster[ttm]        turn to master
    topicaclmanage[tam]      topic acl management
    test[test]                      run unittest

see more details, do:
    %(prog)s cmd -h|--help
'''


class CmdContext:
    def __init__(self, argv, toolsConfFileDir=""):
        if len(argv) < 2:
            self._usage()
            sys.exit(1)
        self.initCmdAuthorityCheckTable()
        cmdStr = argv[1]
        if (cmdStr in ['deploy', 'dp']):
            import swift_config_cmd
            self.cmd = swift_config_cmd.SwiftDeployCmd()
            self.cmdStr = 'dp'
        elif (cmdStr in ['download', 'dl']):
            import swift_config_cmd
            self.cmd = swift_config_cmd.SwiftDownloadCmd()
            self.cmdStr = 'dl'
        elif (cmdStr in ['copy', 'cp']):
            import swift_config_cmd
            self.cmd = swift_config_cmd.SwiftCopyCmd()
            self.cmdStr = 'cp'
        elif (cmdStr in ['configversion', 'cv']):
            import swift_config_cmd
            self.cmd = swift_config_cmd.SwiftVersionCmd()
            self.cmdStr = 'cv'
        elif (cmdStr in ['getconfig', 'gc']):
            import swift_config_cmd
            self.cmd = swift_config_cmd.GetConfigCmd()
            self.cmdStr = 'gc'
        elif (cmdStr in ['updatebrokerconfig', 'ubc']):
            import upc_cmd
            self.cmd = upc_cmd.UpdateBrokerConfigCmd()
            self.cmdStr = 'ubc'
        elif (cmdStr in ['rollbackbrokerconfig', 'rbc']):
            import upc_cmd
            self.cmd = upc_cmd.RollbackBrokerConfigCmd()
            self.cmdStr = 'rbc'
        elif (cmdStr in ['updateadminconfig', 'uac']):
            import app_cmd
            self.cmd = app_cmd.SwiftUpdateAdminConfCmd()
            self.cmdStr = 'uac'
        elif (cmdStr in ['start', 'st']):
            import app_cmd
            self.cmd = app_cmd.SwiftStartCmd()
            self.cmdStr = 'st'
        elif (cmdStr in ['stop', 'sp']):
            import app_cmd
            self.cmd = app_cmd.SwiftStopCmd()
            self.cmdStr = 'sp'
        elif (cmdStr in ['gas', 'getappstatus']):
            import app_cmd
            self.cmd = app_cmd.SwiftAppStatusCmd()
            self.cmdStr = 'gas'
        elif (cmdStr in ['delete', 'del']):
            import app_cmd
            self.cmd = app_cmd.SwiftDeleteCmd()
            self.cmdStr = 'del'
        elif (cmdStr in ['getbrokerstatus', 'gbs']):
            import app_cmd
            self.cmd = app_cmd.GetBrokerStatusCmd()
            self.cmdStr = 'gbs'
        elif (cmdStr in ['killbroker', 'kb']):
            import app_cmd
            self.cmd = app_cmd.SwiftKillBrokerCmd()
            self.cmdStr = 'kb'
        elif (cmdStr in ['getstatus', 'gs']):
            import swift_status_cmd
            self.cmd = swift_status_cmd.SwiftStatusCmd()
            self.cmdStr = 'gs'
        elif (cmdStr in ['addtopic', 'at']):
            import topic_cmd
            self.cmd = topic_cmd.TopicAddCmd()
            self.cmdStr = 'at'
        elif (cmdStr in ['addtopicbatch', 'atb']):
            import topic_cmd
            self.cmd = topic_cmd.TopicBatchAddCmd()
            self.cmdStr = 'atb'
        elif (cmdStr in ['deletetopic', 'dt']):
            import topic_cmd
            self.cmd = topic_cmd.TopicDeleteCmd()
            self.cmdStr = 'dt'
        elif (cmdStr in ['deletetopicbatch', 'dtb']):
            import topic_cmd
            self.cmd = topic_cmd.TopicDeleteBatchCmd()
            self.cmdStr = 'dtb'
        elif (cmdStr in ['deletetopicsbytime', 'dtt']):
            import topic_cmd
            self.cmd = topic_cmd.TopicDeleteByTimeCmd()
            self.cmdStr = 'dtt'
        elif (cmdStr in ['deletetopicdata', 'dtd']):
            import topic_cmd
            self.cmd = topic_cmd.TopicDataDeleteCmd()
            self.cmdStr = 'dtd'
        elif (cmdStr in ['gettopicnames', 'gtn']):
            import topic_cmd
            self.cmd = topic_cmd.TopicNamesCmd()
            self.cmdStr = 'gtn'
        elif (cmdStr in ['gettopicinfo', 'gti']):
            import topic_cmd
            self.cmd = topic_cmd.TopicInfosCmd()
            self.cmdStr = 'gti'
        elif (cmdStr in ['gettopicsimpleinfo', 'gtsi']):
            import topic_cmd
            self.cmd = topic_cmd.TopicSimpleInfosCmd()
            self.cmdStr = 'gtsi'
        elif (cmdStr in ['gettopicmeta', 'gtm']):
            import topic_cmd
            self.cmd = topic_cmd.TopicMetaCmd()
            self.cmdStr = 'gtm'
        elif (cmdStr in ['modifytopic', 'mt']):
            import topic_cmd
            self.cmd = topic_cmd.TopicModifyCmd()
            self.cmdStr = 'mt'
        elif (cmdStr in ['transferpartition', 'tp']):
            import topic_cmd
            self.cmd = topic_cmd.TransferPartitionCmd()
            self.cmdStr = 'tp'
        elif (cmdStr in ['changeslot', 'cs']):
            import topic_cmd
            self.cmd = topic_cmd.ChangeSlotCmd()
            self.cmdStr = 'cs'
        elif (cmdStr in ['sendmsg', 'sm']):
            import producer_cmd
            self.cmd = producer_cmd.ProducerCmd()
            self.cmdStr = 'sm'
        elif (cmdStr in ['getmsg', 'gm']):
            import consumer_cmd
            self.cmd = consumer_cmd.ConsumerGetMsgCmd()
            self.cmdStr = 'gm'
        elif (cmdStr in ['getmaxmsgid', 'gmi']):
            import consumer_cmd
            self.cmd = consumer_cmd.ConsumerGetMaxMsgIdCmd()
            self.cmdStr = 'gmi'
        elif (cmdStr in ['getminmsgidbytime', 'gmit']):
            import consumer_cmd
            self.cmd = consumer_cmd.ConsumerGetMinMsgIdByTime()
            self.cmdStr = 'gmit'
        elif (cmdStr in ['exporttopics', 'et']):
            import topic_cmd
            self.cmd = topic_cmd.ExportTopicsCmd()
            self.cmdStr = 'et'
        elif (cmdStr in ['importtopics', 'it']):
            import topic_cmd
            self.cmd = topic_cmd.ImportTopicsCmd()
            self.cmdStr = 'it'
        elif (cmdStr in ['updatetopics', 'ut']):
            import topic_cmd
            self.cmd = topic_cmd.UpdateTopicsCmd()
            self.cmdStr = 'ut'
        elif (cmdStr in ['registerschema', 'regscm']):
            import topic_cmd
            self.cmd = topic_cmd.RegisterSchemaCmd()
            self.cmdStr = 'regscm'
        elif (cmdStr in ['getschema', 'gscm']):
            import topic_cmd
            self.cmd = topic_cmd.GetSchemaCmd()
            self.cmdStr = 'gscm'
        elif (cmdStr in ['gettopicrwtime', 'gtrw']):
            import topic_cmd
            self.cmd = topic_cmd.GetTopicRWTimeCmd()
            self.cmdStr = 'gtrw'
        elif (cmdStr in ['getlastdeletednousetopic', 'gtldnt']):
            import topic_cmd
            self.cmd = topic_cmd.GetLastDeletedNoUseTopicCmd()
            self.cmdStr = 'gtldnt'
        elif (cmdStr in ['getdeletednousetopic', 'gtdnt']):
            import topic_cmd
            self.cmd = topic_cmd.GetDeletedNoUseTopicCmd()
            self.cmdStr = 'gtdnt'
        elif (cmdStr in ['getdeletednousetopicfiles', 'gtdntf']):
            import topic_cmd
            self.cmd = topic_cmd.GetDeletedNoUseTopicFilesCmd()
            self.cmdStr = 'gtdntf'
        elif (cmdStr in ['topicaclmanage', 'tam']):
            import topic_cmd
            self.cmd = topic_cmd.TopicAclManageCmd()
            self.cmdStr = 'tam'
        elif (cmdStr in ['updatewriterversion', 'uwv']):
            import app_cmd
            self.cmd = app_cmd.UpdateWriterVersionCmd()
            self.cmdStr = 'uwv'
        elif (cmdStr in ['turntomaster', 'ttm']):
            import app_cmd
            self.cmd = app_cmd.TurnToMasterCmd()
            self.cmdStr = 'ttm'
        elif (cmdStr in ['test', 'test']):
            import unittest_cmd
            self.cmd = unittest_cmd.UnittestCmd(toolsConfFileDir)
            self.cmdStr = 'test'
            return
        else:
            self._usage()
            sys.exit(1)

        ret, errMsg = self.cmd.parseParams(argv[2:])
        if not ret:
            print('parseParams Error: %s' % errMsg)
            self.cmd.usage()
            sys.exit(1)

    def run(self):
        if self.checkTable[self.cmdStr] and not self.cmd.checkAuthority():
            print 'check authority failed'
            return False
        data, error, code = self.cmd.run()
        if code != 0:
            if error:
                print error
            return False
        else:
            return True

    def _usage(self):
        global usage
        print usage % {'prog': sys.argv[0]}

    def initCmdAuthorityCheckTable(self):
        self.checkTable = {}
        self.checkTable['dp'] = False
        self.checkTable['dl'] = False
        self.checkTable['cv'] = False
        self.checkTable['gc'] = False
        self.checkTable['del'] = False
        self.checkTable['gbs'] = False
        self.checkTable['gms'] = False
        self.checkTable['st'] = False
        self.checkTable['sp'] = False
        self.checkTable['gas'] = False
        self.checkTable['gs'] = False
        self.checkTable['gml'] = False
        self.checkTable['at'] = False
        self.checkTable['atb'] = False
        self.checkTable['mt'] = False
        self.checkTable['tp'] = False
        self.checkTable['cs'] = False
        self.checkTable['gtn'] = False
        self.checkTable['gti'] = False
        self.checkTable['gtsi'] = False
        self.checkTable['gtm'] = False
        self.checkTable['dt'] = False
        self.checkTable['dtb'] = False
        self.checkTable['dtt'] = False
        self.checkTable['dtd'] = False
        self.checkTable['sm'] = False
        self.checkTable['gm'] = False
        self.checkTable['gmi'] = False
        self.checkTable['gmit'] = False
        self.checkTable['uac'] = False
        self.checkTable['ubc'] = False
        self.checkTable['rbc'] = False
        self.checkTable['umc'] = False
        self.checkTable['rmc'] = False
        self.checkTable['test'] = False
        self.checkTable['et'] = False
        self.checkTable['it'] = False
        self.checkTable['ut'] = False
        self.checkTable['cp'] = False
        self.checkTable['kb'] = False
        self.checkTable['regscm'] = False
        self.checkTable['gscm'] = False
        self.checkTable['gtrw'] = False
        self.checkTable['gtldnt'] = False
        self.checkTable['gtdnt'] = False
        self.checkTable['gtdntf'] = False
        self.checkTable['uwv'] = False
        self.checkTable['ttm'] = False
        self.checkTable['tam'] = False
