# Top-k window view 
Esper extension for top-k queries


## Dependencies
* Java
* Esper

## Get started
```
Configuration config = new Configuration();
EPServiceProvider epService = EPServiceProviderManager.getDefaultProvider(config);
epService.getEPAdministrator().getConfiguration().addPlugInView("custom", "topk", "view.TopkWindowViewFactory");
EPStatement stmt = epService.getEPAdministrator().createEPL(EPL Query);
```

## Overview
A Top-k query on an event stream yields k events with largest values among the entire input events. This proposed code extends and integrate the open source event stream processing system Esper with a new querying facility so that its engine would process Top-k query efficiently. Finally, the proposed query shows enormous improvement in performance, compared to the combination of built-in query facilities provided by Esper. In addition, we analyze the source programs of Esper to find a proper way of Top-k query extension of Esper, otherwise, in a naive extension method, it could not support the correct meaning of Top-k queries as intended.

## Laboratory Site
[PLAS LAB](http://plas.cnu.ac.kr/, https://sites.google.com/cs-cnu.org/plas/)

## University Site
[CNU](http://computer.cnu.ac.kr/, http://computer.cnu.ac.kr/index.php?mid=int_greet_en, http://plus.cnu.ac.kr/html/en/, http://www.cnu.ac.kr)
