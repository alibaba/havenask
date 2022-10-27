# -*- coding: utf-8 -*-

# generationId is int32_t in build_service and ha3
MAX_GENERATION_ID = 2**31 - 1 # max int32
MIN_GENERATION_ID = 0 # can not less than 0

class GenerationManager(object):
    def __init__(self):
        pass

    @staticmethod
    def checkGenerationId(generationId):
        if MIN_GENERATION_ID > generationId or generationId > MAX_GENERATION_ID:
            raise Exception('generationId[%d] should be in [%d, %d]' % (
                    generationId, MIN_GENERATION_ID, MAX_GENERATION_ID))
