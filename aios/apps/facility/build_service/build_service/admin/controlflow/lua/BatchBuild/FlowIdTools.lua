local FlowIdTools = {}

function FlowIdTools.getBatchProcessor(id)
   return "BatchProcessor-"..id
end

function FlowIdTools.getIncProcessor()
   return "BSIncProcess"
end

function FlowIdTools.getBatchBuilder(id, clusterName)
   return clusterName.."-BatchBuilder-"..id
end

function FlowIdTools.getIncBuild(clusterName)
   return clusterName..":IncBuild"
end

function FlowIdTools.getBatchMerger(id, clusterName)
   return clusterName.."-BatchMerger-"..id
end

function FlowIdTools.getIncMerger(clusterName)
   return clusterName..":BSIncMerger"
end

function FlowIdTools.getFullProcessor()
   return "BSFullProcess"
end

function FlowIdTools.getFullBuilder(clusterName)
   return clusterName..":FullBuild"
end

function FlowIdTools.getFullMerger(clusterName)
   return clusterName..":BSFullMerger"
end

function FlowIdTools.getBatchControlTag()
   return "BSBatchController"
end
