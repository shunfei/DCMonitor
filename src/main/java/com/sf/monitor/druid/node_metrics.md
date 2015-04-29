Druid metrics
====

## Common metrics

* jvm/mem 

```
{
    "from": "2015-04-29 16:00:00",
    "to": "2015-04-29 16:10:00",
    "metrics": [
        "jvm_mem_committed",
        "jvm_mem_init",
        "jvm_mem_max",
        "jvm_mem_used"
    ],
    "tagValues": [
        {
            "tag": "host",
            "value": "192.168.10.41:8007"
        },
        {
            "tag": "service",
            "value": "druid:prod:overlord"
        }
    ],
    "groups": [
        "type"
    ]
}
```

* jvm/pool

```
{
    "from": "2015-04-29 16:00:00",
    "to": "2015-04-29 16:10:00",
    "metrics": [
        "jvm_pool_committed",
        "jvm_pool_init",
        "jvm_pool_max",
        "jvm_pool_used"
    ],
    "tagValues": [
        {
            "tag": "host",
            "value": "192.168.10.41:8007"
        },
        {
            "tag": "service",
            "value": "druid:prod:overlord"
        }
    ],
    "groups": [
        "type"
    ]
}
```

* jvm/gc

```
{
    "from": "2015-04-29 17:00:00",
    "to": "2015-04-29 17:15:00",
    "metrics": [
        "jvm_gc_time",
        "jvm_gc_count"
    ],
    "tagValues": [
        {
            "tag": "host",
            "value": "192.168.10.41:8007"
        },
        {
            "tag": "service",
            "value": "druid:prod:overlord"
        }
    ],
    "groups": [
        "type"
    ]
}
```


* jvm/bufferpool

```
{
    "from": "2015-04-29 17:00:00",
    "to": "2015-04-29 17:15:00",
    "metrics": [
        "jvm_bufferpool_capacity",
        "jvm_bufferpool_used"
    ],
    "tagValues": [
        {
            "tag": "host",
            "value": "192.168.10.41:8007"
        },
        {
            "tag": "service",
            "value": "druid:prod:overlord"
        }
    ],
    "groups": [
        "type"
    ]
}
```

* sys/mem

```
{
    "from": "2015-04-29 17:50:00",
    "to": "2015-04-29 18:00:00",
    "metrics": [
        "sys_mem_max",
        "sys_mem_used"
    ],
    "tagValues": [
        {
            "tag": "host",
            "value": "192.168.10.60:8009"
        },
        {
            "tag": "service",
            "value": "druid:prod:broker"
        }
    ],
    "groups": [
        "type"
    ],
    "debug": true
}
```

* sys/fs

```
{
    "from": "2015-04-29 17:50:00",
    "to": "2015-04-29 18:00:00",
    "metrics": [
        "sys_fs_max",
        "sys_fs_used"
    ],
    "tagValues": [
        {
            "tag": "host",
            "value": "192.168.10.60:8009"
        },
        {
            "tag": "service",
            "value": "druid:prod:broker"
        }
    ],
    "groups": [
        "device", "mount"
    ],
    "debug": true
}
```

* sys/disk

```
{
    "from": "2015-04-29 17:50:00",
    "to": "2015-04-29 18:00:00",
    "metrics": [
        "sys_disk_write_count",
        "sys_disk_write_size",
        "sys_disk_read_count",
        "sys_disk_read_size"
    ],
    "tagValues": [
        {
            "tag": "host",
            "value": "192.168.10.60:8009"
        },
        {
            "tag": "service",
            "value": "druid:prod:broker"
        }
    ],
    "groups": [
        "device", "mount"
    ],
    "debug": true
}
```

* sys/net

```
{
    "from": "2015-04-29 17:50:00",
    "to": "2015-04-29 18:00:00",
    "metrics": [
        "sys_net_write_size",
        "sys_net_read_size"
    ],
    "tagValues": [
        {
            "tag": "host",
            "value": "192.168.10.60:8009"
        },
        {
            "tag": "service",
            "value": "druid:prod:broker"
        }
    ],
    "groups": [
        "device", "ip", "ether"
    ],
    "debug": true
}
```

* sys/cpu

```
{
    "from": "2015-04-29 17:50:00",
    "to": "2015-04-29 18:00:00",
    "metrics": [
        "sys_cpu"
    ],
    "tagValues": [
        {
            "tag": "host",
            "value": "192.168.10.60:8009"
        },
        {
            "tag": "service",
            "value": "druid:prod:broker"
        }
    ],
    "groups": [
        "cpuid", "type"
    ],
    "debug": true
}
```

* sys/swap

```
{
    "from": "2015-04-29 17:50:00",
    "to": "2015-04-29 18:00:00",
    "metrics": [
        "sys_swap_max",
        "sys_swap_free",
        "sys_swap_pageIn",
        "sys_swap_pageOut"
    ],
    "tagValues": [
        {
            "tag": "host",
            "value": "192.168.10.60:8009"
        },
        {
            "tag": "service",
            "value": "druid:prod:broker"
        }
    ],
    "groups": [

    ],
    "debug": true
}
```



## Realtime

* events

```
{
    "from": "2015-04-29 17:00:00",
    "to": "2015-04-29 17:15:00",
    "metrics": [
        "events_thrownAway",
        "events_unparseable",
        "events_processed"
    ],
    "tagValues": [
        {
            "tag": "host",
            "value": "192.168.10.51:8110"
        },
        {
            "tag": "service",
            "value": "druid:prod:middlemanager"
        }
    ],
    "groups": [
        "dataSource"
    ]
}
```

* rows/output

```
{
    "from": "2015-04-29 17:00:00",
    "to": "2015-04-29 17:15:00",
    "metrics": [
        "rows_output"
    ],
    "tagValues": [
        {
            "tag": "host",
            "value": "192.168.10.51:8110"
        },
        {
            "tag": "service",
            "value": "druid:prod:middlemanager"
        }
    ],
    "groups": [
        "dataSource"
    ]
}
```

* persists

```
{
    "from": "2015-04-29 17:00:00",
    "to": "2015-04-29 17:15:00",
    "metrics": [
        "persists_num",
        "persists_time",
        "persists_backPressure"
    ],
    "tagValues": [
        {
            "tag": "host",
            "value": "192.168.10.51:8110"
        },
        {
            "tag": "service",
            "value": "druid:prod:middlemanager"
        }
    ],
    "groups": [
        "dataSource"
    ]
}
```

## Historical

* cache/delta

```
{
    "from": "2015-04-29 17:00:00",
    "to": "2015-04-29 17:15:00",
    "metrics": [
      "cache_delta_numEntries",
      "cache_delta_numEntries",
      "cache_delta_sizeBytes",
      "cache_delta_hits",
      "cache_delta_misses",
      "cache_delta_evictions",
      "cache_delta_averageBytes",
      "cache_delta_timeouts",
      "cache_delta_errors",
      "cache_delta_hitRate"
    ],
    "tagValues": [
        {
            "tag": "host",
            "value": "192.168.10.51:8001"
        },
        {
            "tag": "service",
            "value": "druid:prod:historical"
        }
    ],
    "groups": [

    ]
}
```

* server/segment

```
{
    "from": "2015-04-29 17:00:00",
    "to": "2015-04-29 17:15:00",
    "metrics": [
        "server_segment_max"
    ],
    "tagValues": [
        {
            "tag": "host",
            "value": "192.168.10.51:8001"
        },
        {
            "tag": "service",
            "value": "druid:prod:historical"
        }
    ],
    "groups": [

    ],
    "debug": true
}
```

* server/segment/detail

```
{
    "from": "2015-04-29 17:00:00",
    "to": "2015-04-29 17:15:00",
    "metrics": [
        "server_segment_used",
        "server_segment_usedPercent",
        "server_segment_count"
    ],
    "tagValues": [
        {
            "tag": "host",
            "value": "192.168.10.51:8001"
        },
        {
            "tag": "service",
            "value": "druid:prod:historical"
        }
    ],
    "groups": [
        "dataSource",
        "tier"
    ]
}
```



* server/segment/total

```
{
    "from": "2015-04-29 17:00:00",
    "to": "2015-04-29 17:15:00",
    "metrics": [
        "server_segment_totalUsed",
        "server_segment_totalCount",
        "server_segment_totalUsedPercent"
    ],
    "tagValues": [
        {
            "tag": "host",
            "value": "192.168.10.51:8001"
        },
        {
            "tag": "service",
            "value": "druid:prod:historical"
        }
    ],
    "groups": [
        "tier"
    ]
}
```
