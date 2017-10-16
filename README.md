# Top-k window view 
code for esper's extension view with top-k concept


## dependencies
* java
* Esper

## get started
```
Configuration config = new Configuration();
EPServiceProvider epService = EPServiceProviderManager.getDefaultProvider(config);
epService.getEPAdministrator().getConfiguration().addPlugInView("custom", "topk", "view.TopkWindowViewFactory");
EPStatement stmt = epService.getEPAdministrator().createEPL(EPL Query);
```

## Laboratory Site
[PLAS LAB](http://plas.cnu.ac.kr/)

## University Site
[CNU](http://computer.cnu.ac.kr/)