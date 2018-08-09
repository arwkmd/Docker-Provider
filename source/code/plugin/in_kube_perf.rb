#!/usr/local/bin/ruby
# frozen_string_literal: true

module Fluent
    
      class Kube_Perf_Input < Input
        Plugin.register_input('kubeperf', self)
    
        def initialize
          super
          require 'yaml'
          require 'json'
    
          require_relative 'KubernetesApiClient'
          require_relative 'oms_common'
          require_relative 'omslog'
        end
    
        config_param :run_interval, :time, :default => '1m'
        config_param :tag, :string, :default => "oms.api.KubePerf"
    
        def configure (conf)
          super
        end
    
        def start
          if @run_interval
            @finished = false
            @condition = ConditionVariable.new
            @mutex = Mutex.new
            @thread = Thread.new(&method(:run_periodic))
          end
        end
    
        def shutdown
          if @run_interval
            @mutex.synchronize {
              @finished = true
              @condition.signal
            }
            @thread.join
          end
        end
    
        def enumerate()
          time = Time.now.to_f
          begin
              eventStream = MultiEventStream.new
              
                $log.info("in_kube_perf::enumerate : Getting pods from Kube API @ #{Time.now.utc.iso8601}")
                #get resource requests & resource limits per container as perf data 
                podInventory = JSON.parse(KubernetesApiClient.getKubeResourceInfo('pods').body)
                $log.info("in_kube_perf::enumerate : Done getting pods from Kube API @ #{Time.now.utc.iso8601}")
                if(!podInventory.empty?) 
                  containerMetricDataItems = []
                  hostName = (OMS::Common.get_hostname)
                  containerMetricDataItems.concat(KubernetesApiClient.getContainerResourceRequestsAndLimits(podInventory, "requests", "cpu","cpuRequestNanoCores"))
                  containerMetricDataItems.concat(KubernetesApiClient.getContainerResourceRequestsAndLimits(podInventory, "requests", "memory","memoryRequestBytes"))
                  containerMetricDataItems.concat(KubernetesApiClient.getContainerResourceRequestsAndLimits(podInventory, "limits", "cpu","cpuLimitNanoCores"))
                  containerMetricDataItems.concat(KubernetesApiClient.getContainerResourceRequestsAndLimits(podInventory, "limits", "memory","memoryLimitBytes"))
      
                  containerMetricDataItems.each do |record|
                    record['DataType'] = "LINUX_PERF_BLOB"
                    record['IPName'] = "LogManagement"
                    eventStream.add(time, record) if record
                    #router.emit(@tag, time, record) if record  
                  end
                end

                #get allocatable limits per node as perf data
                #<TODO> Node capacity is different from node allocatable. Allocatable is what is avaialble for allocating pods.
                # In theory Capacity = Allocatable + kube-reserved + system-reserved + eviction-threshold
                # For more details refer to https://kubernetes.io/docs/tasks/administer-cluster/reserve-compute-resources/#node-allocatable
                $log.info("in_kube_perf::enumerate : Getting nodes from Kube API @ #{Time.now.utc.iso8601}")
                nodeInventory = JSON.parse(KubernetesApiClient.getKubeResourceInfo('nodes').body)
                $log.info("in_kube_perf::enumerate : Done getting nodes from Kube API @ #{Time.now.utc.iso8601}")
                if(!nodeInventory.empty?)
                  nodeMetricDataItems = []
                  #allocatable metrics @ node level
                  nodeMetricDataItems.concat(KubernetesApiClient.parseNodeLimits(nodeInventory, "allocatable", "cpu", "cpuAllocatableNanoCores"))
                  nodeMetricDataItems.concat(KubernetesApiClient.parseNodeLimits(nodeInventory, "allocatable", "memory", "memoryAllocatableBytes"))
                  #capacity metrics @ node level
                  nodeMetricDataItems.concat(KubernetesApiClient.parseNodeLimits(nodeInventory, "capacity", "cpu", "cpuCapacityNanoCores"))
                  nodeMetricDataItems.concat(KubernetesApiClient.parseNodeLimits(nodeInventory, "capacity", "memory", "memoryCapacityBytes"))

                  nodeMetricDataItems.each do |record|
                    record['DataType'] = "LINUX_PERF_BLOB"
                    record['IPName'] = "LogManagement"
                    eventStream.add(time, record) if record
                    #router.emit(@tag, time, record) if record 
                  end 
                end
              router.emit_stream(@tag, eventStream) if eventStream          
              rescue  => errorStr
              $log.warn "Failed to retrieve metric data: #{errorStr}"
              $log.debug_backtrace(errorStr.backtrace)
          end
        end
    
        def run_periodic
          @@kperfmemoryfootprintbegin = `ps -eo vsz,rss,comm | grep omsagent`
          $log.info("kubeperf-begin-memoryfootprint @ #{Time.now.utc.iso8601}, #{@@kperfmemoryfootprintbegin}")
          @mutex.lock
          done = @finished
          until done
            @condition.wait(@mutex, @run_interval)
            done = @finished
            @mutex.unlock
            if !done
              begin
                $log.info("in_kube_perf::run_periodic @ #{Time.now.utc.iso8601}")
                enumerate
              rescue => errorStr
                $log.warn "in_kube_perf::run_periodic: enumerate Failed to retrieve kube perf metrics: #{errorStr}"
              end
            end
            @mutex.lock
          end
          @mutex.unlock
          @@kperfmemoryfootprintend = `ps -eo vsz,rss,comm | grep omsagent`
          $log.info("kubeperf-end-memoryfootprint @ #{Time.now.utc.iso8601}, #{@@kperfmemoryfootprintend}")
        end
      end # Kube_Perf_Input
end # module
