package com.alstom.datalab.pipelines

/**
  * Created by guillaumepinot on 13/02/2016.
  * Aim : create one file by server and by type of information (flow, cpu usage, storage usage)
  *   - server list is from AIP-Server. Key : IP
  *   - aggregated by day and by month

  *   - flows relation
  *     - source :
  *       - device_type : laptop_desktop, server
  *       - if server : ip
  *       - site
  *       - con_type : tcp, udp
  *       - port
  *       - con_traffic
  *       - con_connection
  *       - con_distinct_user
  *     - destination :
  *       - device_type : laptop_desktop, server
  *       - if server : ip
  *       - site
  *       - con_type : tcp, udp
  *       - port
  *       - con_traffic
  *       - con_connection
  *       - con_distinct_user
  *   - CPU usage or memory usage
  *     - type : cpu, memory
  *     - percentiles %used : q0, q10, q25, q50, q75, q90, q100
  *   - Storage usage
  *     - charged_type : SAN, NAS, ...
  *     - percentiles %available : q0, q10, q25, q50, q75, q90, q100
  *     - percentiles available mb : q0, q10, q25, q50, q75, q90, q100
  *     - max_total_avail
  *     - charged_used_mb
  *     - charged_total_mb
  **/


class serverallaxis {


}
